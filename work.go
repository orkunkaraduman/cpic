package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/goinsane/xlog"
	"github.com/jehiah/go-strftime"
	"github.com/rwcarlsen/goexif/exif"
)

func work(ctx context.Context, wg *sync.WaitGroup, srcFilePathCh <-chan string, dstDirPath string, format string, rm bool, tmpDirPath string) {
	defer wg.Done()

	for done := false; !done; {
		select {
		case <-ctx.Done():
			done = true
		case srcFilePath, ok := <-srcFilePathCh:
			if !ok {
				done = true
				break
			}
			if workInFile(ctx, srcFilePath, dstDirPath, format, tmpDirPath) && rm {
				if err := os.Remove(srcFilePath); err != nil {
					xlog.Warningf("source file %q remove error: %v", srcFilePath, err)
				}
			}
		}
	}
}

func workInFile(ctx context.Context, srcFilePath string, dstDirPath string, format string, tmpDirPath string) bool {
	srcFile, err := os.OpenFile(srcFilePath, os.O_RDONLY, 0)
	if err != nil {
		xlog.Errorf("source file %q open error: %v", srcFilePath, err)
		return false
	}
	defer srcFile.Close()

	srcFileStat, err := srcFile.Stat()
	if err != nil {
		xlog.Errorf("source file %q stat error: %v", srcFilePath, err)
		return false
	}
	if srcFileStat.Mode()&os.ModeType != 0 {
		xlog.Errorf("source file %q is not reqular file", srcFilePath)
		return false
	}

	var tm time.Time
	ef, err := exif.Decode(srcFile)
	if err != nil {
		xlog.Warningf("source file %q exif decode error: %v", srcFilePath, err)
	} else {
		tm, err = ef.DateTime()
		if err != nil {
			xlog.Warningf("source file %q exif get datetime error: %v", srcFilePath, err)
		}
	}
	ext := filepath.Ext(srcFilePath)
	name := strings.ToUpper(strings.TrimSuffix(filepath.Base(srcFilePath), ext))
	ext = strings.ToUpper(ext)
	dirPath := dstDirPath +"/noexif"
	if !tm.IsZero() {
		dirPath = dstDirPath +"/"+strftime.Format(format, tm)
	}
	if err := os.MkdirAll(dirPath, 0755); err != nil {
		xlog.Errorf("destination directories %q create error: %v", dirPath, err)
		return false
	}

	tmpFile, err := ioutil.TempFile(tmpDirPath, srcFileStat.Name())
	if err != nil {
		xlog.Errorf("temp file open error: %v", err)
		return false
	}
	defer tmpFile.Close()
	defer func() {
		if err := os.Remove(tmpFile.Name()); err != nil {
			xlog.Warningf("temp file %q remove error: %v", tmpFile.Name(), err)
		}
	}()

	if _, err := srcFile.Seek(0, io.SeekStart); err != nil {
		xlog.Errorf("source file %q seek error: %v", srcFilePath, err)
		return false
	}

	buf := make([]byte, 32*1024)
	fh := fileHash{}
	sum := sha256.New()
	for {
		nr, er := srcFile.Read(buf)
		if nr > 0 {
			nw, ew := tmpFile.Write(buf[:nr])
			if nw > 0 {
				fh.size += int64(nw)
				sum.Write(buf[:nw])
			}
			if ew != nil {
				xlog.Errorf("temp file %q write error: %v", tmpFile.Name(), ew)
				return false
			}
			if nr != nw {
				xlog.Errorf("temp file %q write error: %v", tmpFile.Name(), io.ErrShortWrite)
				return false
			}
		}
		if er != nil {
			if er != io.EOF {
				xlog.Errorf("source file %q read error: %v", srcFilePath, er)
				return false
			}
			break
		}
	}
	copy(fh.sum[:], sum.Sum(nil))

	if !fileHashSet(fh) {
		return false
	}

	sumStr := strings.ToUpper(hex.EncodeToString(fh.sum[:]))
	linked := false
	for i := 0; i < 1+len(sumStr)/4; i++ {
		path := dirPath+"/"+name+ext
		if i > 0 {
			k := (i-1)*4
			path = dirPath+"/"+name+"-"+sumStr[k:k+4]+ext
		}
		fileLock(path)
		if err := os.Link(tmpFile.Name(), path); err == nil || !os.IsExist(err) {
			fileUnlock(path)
			if err != nil {
				xlog.Errorf("destination file %q link error: %v", path, err)
				return false
			}
			linked = true
			break
		}
		fileUnlock(path)
	}

	if !linked {
		fileHashDel(fh)
		xlog.Errorf("destination file collision error for source file %q", srcFilePath)
		return false
	}

	return true
}
