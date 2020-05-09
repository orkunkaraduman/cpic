package main

import (
	"context"
	"crypto/md5"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/goinsane/xlog"
	"github.com/jehiah/go-strftime"

	"gitlab.com/orkunkaraduman/cpic/catalog"
	"gitlab.com/orkunkaraduman/cpic/exiftool"
	"gitlab.com/orkunkaraduman/cpic/util"
)

type importCommand struct {
	command

	WorkerCount    int
	Format         string
	Remove         bool
	ExtList        string
	FollowSymLinks bool
	SrcDirs        []string

	format  string
	srcDirs []string
	extList map[string]struct{}

	stats struct {
		total            uint64
		unknownExtension uint64
		infoError        uint64
		dateTimeError    uint64
		dateTimeNotFound uint64
		alreadyExists    uint64
		renamed          uint64
		imported         uint64
		removed          uint64
	}
}

func (c *importCommand) Prepare() {
	c.format = filepath.FromSlash(c.Format)
	if c.format[0] == os.PathSeparator {
		xlog.Fatalf("format %q must be relative path", c.Format)
	}
	if c.format[len(c.format)-1] == os.PathSeparator {
		xlog.Fatalf("format %q must be file name prefix", c.Format)
	}
	c.format = filepath.Clean(c.format)
	lFormat := strings.ToLower(c.format)
	for _, dir := range []string{"cpic", "tmp"} {
		if lFormat == dir || strings.HasPrefix(lFormat, dir+string(os.PathSeparator)) {
			xlog.Fatalf("format %q must be different than %q directory", c.Format, dir)
		}
	}

	c.srcDirs = make([]string, 0, 128)
	for _, srcDir := range c.SrcDirs {
		absSrcDir, err := filepath.Abs(srcDir)
		if err != nil {
			xlog.Fatalf("source directory abs error: %v", err)
		}
		stat, err := os.Lstat(srcDir)
		if err != nil {
			xlog.Fatalf("source directory stat error: %v", err)
		}
		if !stat.IsDir() {
			xlog.Fatalf("source directory %q is not a directory", srcDir)
		}
		c.srcDirs = append(c.srcDirs, absSrcDir)
	}

	c.extList = make(map[string]struct{}, 128)
	for _, ext := range strings.Split(strings.ToUpper(c.ExtList), ",") {
		ext = strings.TrimSpace(ext)
		if ext == "" {
			continue
		}
		c.extList[ext] = struct{}{}
	}
}

func (c *importCommand) Run(ctx context.Context) {
	ctx, ctxCancel := context.WithCancel(ctx)
	defer ctxCancel()
	wg := new(sync.WaitGroup)
	workerCount := c.WorkerCount
	if workerCount <= 0 {
		workerCount = runtime.NumCPU()
	}
	srcFileCh := make(chan string, workerCount*2)

	wg.Add(1)
	go func() {
		defer wg.Done()
		for _, srcDir := range c.srcDirs {
			if err := util.FileScan(ctx, srcDir, srcFileCh, c.FollowSymLinks); err != nil {
				if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
					xlog.Error(err)
				}
				break
			}
		}
		close(srcFileCh)
	}()

	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := c.copyFiles(ctx, srcFileCh); err != nil {
				if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
					xlog.Error(err)
					ctxCancel()
				}
			}
		}()
	}

	wg.Wait()
	xlog.WithFieldKeyVals(
		"total", c.stats.total,
		"unknownExtension", c.stats.unknownExtension,
		"infoError", c.stats.infoError,
		"dateTimeError", c.stats.dateTimeError,
		"dateTimeNotFound", c.stats.dateTimeNotFound,
		"alreadyExists", c.stats.alreadyExists,
		"renamed", c.stats.renamed,
		"imported", c.stats.imported,
		"removed", c.stats.removed,
	).Infof("%d of %d pictures successfully imported", c.stats.imported, c.stats.total)
}

func (c *importCommand) copyFiles(ctx context.Context, srcFileCh <-chan string) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case srcFile, ok := <-srcFileCh:
			if !ok {
				return nil
			}

			atomic.AddUint64(&c.stats.total, 1)

			if len(c.extList) > 0 {
				if _, ok := c.extList[strings.TrimPrefix(strings.ToUpper(filepath.Ext(srcFile)), ".")]; !ok {
					xlog.V(5).Warningf("picture %q has unknown extension", srcFile)
					atomic.AddUint64(&c.stats.unknownExtension, 1)
					break
				}
			}

			if err := c.copyFile(ctx, srcFile); err != nil {
				return err
			}

			if c.Remove {
				if err := os.Remove(srcFile); err != nil {
					return fmt.Errorf("source file remove error: %w", err)
				}
				atomic.AddUint64(&c.stats.removed, 1)
			}
		}
	}
}

func (c *importCommand) copyFile(ctx context.Context, srcFile string) error {
	srcFileHandle, err := os.OpenFile(srcFile, os.O_RDONLY, 0)
	if err != nil {
		return fmt.Errorf("source file open error: %w", err)
	}
	defer srcFileHandle.Close()

	srcFileStat, err := srcFileHandle.Stat()
	if err != nil {
		return fmt.Errorf("source file stat error: %w", err)
	}
	if srcFileStat.Mode()&os.ModeType != 0 {
		return fmt.Errorf("source file %q is not a reqular file", srcFile)
	}

	pic := catalog.Picture{}

	if t, err := exiftool.ReadTagsFromFileContext(ctx, srcFile); err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return err
		}
		xlog.V(3).Warningf("source file %q info error: %v", srcFile, err)
		atomic.AddUint64(&c.stats.infoError, 1)
	} else {
		tm, err := t.DateTime()
		if err != nil {
			if errors.Is(err, exiftool.ErrDateTimeNotFound) {
				xlog.V(4).Warningf("source file %q datetime not found", srcFile)
				atomic.AddUint64(&c.stats.dateTimeNotFound, 1)
			} else {
				xlog.V(3).Warningf("source file %q datetime error: %v", srcFile, err)
				atomic.AddUint64(&c.stats.dateTimeError, 1)
			}
		} else {
			pic.TakenAt = &tm
		}
	}

	if _, err := srcFileHandle.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("source file seek error: %w", err)
	}

	dstExt := filepath.Ext(srcFile)
	dstBase := strings.ToUpper(strings.TrimSuffix(filepath.Base(srcFile), dstExt))
	dstExt = strings.ToUpper(dstExt)
	dstDir := "noinfo"
	if pic.TakenAt != nil {
		s := strftime.Format(c.format, *pic.TakenAt) + "-" + dstBase
		dstDir = filepath.Dir(s)
		dstBase = filepath.Base(s)
	}

	absDstDir := c.WorkDir + string(os.PathSeparator) + dstDir
	if err := os.MkdirAll(absDstDir, 0755); err != nil {
		return fmt.Errorf("destination directory create error: %w", err)
	}

	tmpFileHandle, err := ioutil.TempFile(c.TmpDir, srcFileStat.Name())
	if err != nil {
		return fmt.Errorf("temp file open error: %w", err)
	}
	defer tmpFileHandle.Close()

	copyOK := false

	tmpFile := tmpFileHandle.Name()
	defer func() {
		if copyOK {
			return
		}
		if err := os.Remove(tmpFile); err != nil {
			xlog.Warningf("temp file remove error: %v", err)
		}
	}()

	buf := make([]byte, 64*1024)
	sumMD5 := md5.New()
	sumSHA256 := sha256.New()
	pic.Size, err = util.Copy(ctx, tmpFileHandle, srcFileHandle, buf, []hash.Hash{sumMD5, sumSHA256})
	if err != nil {
		return err
	}
	pic.SumMD5 = strings.ToUpper(hex.EncodeToString(sumMD5.Sum(nil)[:]))
	pic.SumSHA256 = strings.ToUpper(hex.EncodeToString(sumSHA256.Sum(nil)[:]))

	dstFileName := dstBase + dstExt
	dstFile := dstDir + string(os.PathSeparator) + dstFileName
	dstAbsFile := c.WorkDir + string(os.PathSeparator) + dstFile

	hashStr := pic.SumMD5 + pic.SumSHA256
	for i := 0; i < 1+len(hashStr)/4; i++ {
		if i > 0 {
			k := (i - 1) * 4
			dstFileName = dstBase + "-" + hashStr[k:k+4] + dstExt
			dstFile = dstDir + string(os.PathSeparator) + dstFileName
			dstAbsFile = c.WorkDir + string(os.PathSeparator) + dstFile
		}
		pic.Path = filepath.ToSlash(dstFile)
		if err := c.Catalog.NewPicture(pic); err != nil {
			if errors.Is(err, catalog.ErrPathAlreadyExists) {
				continue
			}
			if errors.Is(err, catalog.ErrPictureAlreadyExists) {
				xlog.V(2).Warningf("picture %q already exists", srcFile)
				atomic.AddUint64(&c.stats.alreadyExists, 1)
				return nil
			}
			return err
		}
		err := func() error {
			if _, err := os.Stat(dstAbsFile); !os.IsNotExist(err) {
				if err == nil {
					return fmt.Errorf("picture %q destination file %q already exists", srcFile, dstFile)
				}
				return err
			}
			if err := os.Rename(tmpFile, dstAbsFile); err != nil {
				if os.IsExist(err) {
					return fmt.Errorf("picture %q destination file %q already exists", srcFile, dstFile)
				}
				return err
			}
			return nil
		}()
		if err != nil {
			if _, err := c.Catalog.DeletePicture(pic.Path); err != nil {
				xlog.Warningf("picture %q delete catalog record error, possibly data inconsistency: %v", err)
			}
			return err
		}
		copyOK = true
		if i > 0 {
			xlog.V(5).Warningf("picture %q renamed to %q", srcFile, dstFile)
			atomic.AddUint64(&c.stats.renamed, 1)
		}
		break
	}

	if !copyOK {
		return fmt.Errorf("picture %q destination file path collision error", srcFile)
	}

	xlog.V(1).Infof("picture %q imported to %q", srcFile, dstFile)
	atomic.AddUint64(&c.stats.imported, 1)
	return nil
}
