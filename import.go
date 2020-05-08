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

	"github.com/goinsane/xlog"
	"github.com/jehiah/go-strftime"
	"github.com/rwcarlsen/goexif/exif"

	"gitlab.com/orkunkaraduman/cpic/catalog"
	"gitlab.com/orkunkaraduman/cpic/util"
)

type importCommand struct {
	command

	Format         string
	Remove         bool
	ExtList        string
	FollowSymLinks bool
	SrcDirs        []string

	format  string
	srcDirs []string
	extList map[string]struct{}
	locker  *util.Locker
}

func (c *importCommand) Prepare() {
	c.format = strings.Trim(filepath.Clean(c.Format), string(os.PathSeparator))
	if s := strings.ToLower(c.format); s == "cpic" || strings.HasPrefix(s, "cpic"+string(os.PathSeparator)) {
		xlog.Fatalf("format %q must be different than %q dir", c.Format, "cpic")
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

	c.locker = util.NewLocker()
}

func (c *importCommand) Run(ctx context.Context) {
	ctx, ctxCancel := context.WithCancel(ctx)
	defer ctxCancel()
	wg := new(sync.WaitGroup)
	workerCount := runtime.NumCPU()
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
			if err := c.copyFile(ctx, srcFile); err != nil {
				return err
			}
			if c.Remove {
				if err := os.Remove(srcFile); err != nil {
					return fmt.Errorf("source file remove error: %w", err)
				}
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

	dstExt := filepath.Ext(srcFile)
	dstBase := strings.ToUpper(strings.TrimSuffix(filepath.Base(srcFile), dstExt))
	dstExt = strings.ToUpper(dstExt)
	dstDir := "noexif"
	if _, ok := c.extList[strings.TrimPrefix(dstExt, ".")]; !ok {
		xlog.V(5).Warningf("picture %q has not needing extension", srcFile)
		return nil
	}

	pic := catalog.Picture{}

	if ef, err := exif.Decode(srcFileHandle); err != nil {
		if !errors.Is(err, io.EOF) {
			xlog.V(3).Warningf("source file %q exif decode error: %v", srcFile, err)
		} else {
			xlog.V(4).Warningf("source file %q exif not found", srcFile)
		}
	} else {
		tm, err := ef.DateTime()
		if err != nil {
			xlog.V(3).Warningf("source file %q exif get datetime error: %v", srcFile, err)
		} else {
			pic.TakenAt = &tm
		}
	}

	if _, err := srcFileHandle.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("source file seek error: %w", err)
	}

	if pic.TakenAt != nil {
		s := strftime.Format(c.format, *pic.TakenAt)
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

	for i := 0; i < 1+len(pic.SumSHA256)/4; i++ {
		if i > 0 {
			k := (i - 1) * 4
			dstFileName = dstBase + "-" + pic.SumSHA256[k:k+4] + dstExt
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
		break
	}

	if !copyOK {
		return fmt.Errorf("picture %q destination file path collision error", srcFile)
	}

	xlog.V(1).Infof("picture %q imported", srcFile)
	return nil
}
