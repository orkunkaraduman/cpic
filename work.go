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

func copyFiles(ctx context.Context, wg *sync.WaitGroup, srcFilePathCh <-chan string, dstDirPath string, format string, rm bool, tmpDirPath string) {
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
			if copyFile(ctx, srcFilePath, dstDirPath, format, tmpDirPath) {
				if rm {
					if err := os.Remove(srcFilePath); err != nil {
						xlog.Warningf("source file %q remove error: %v", srcFilePath, err)
					}
				}
			}
		}
	}
}

func copyFile(ctx context.Context, srcFilePath string, dstDirPath string, format string, tmpDirPath string) bool {
	errLogger := xlog.WithFieldKeyVals("srcFilePath", srcFilePath)
	srcFile, err := os.OpenFile(srcFilePath, os.O_RDONLY, 0)
	if err != nil {
		errLogger.Errorf("source file open error: %v", err)
		return false
	}
	defer srcFile.Close()

	srcFileStat, err := srcFile.Stat()
	if err != nil {
		errLogger.Errorf("source file stat error: %v", err)
		return false
	}
	if srcFileStat.Mode()&os.ModeType != 0 {
		errLogger.Errorf("source file is not reqular file")
		return false
	}

	var tm time.Time
	ef, err := exif.Decode(srcFile)
	if err != nil {
		errLogger.Warningf("source file exif decode error: %v", err)
	} else {
		tm, err = ef.DateTime()
		if err != nil {
			errLogger.Warningf("source file exif get datetime error: %v", err)
		}
	}
	ext := filepath.Ext(srcFilePath)
	name := strings.ToUpper(strings.TrimSuffix(filepath.Base(srcFilePath), ext))
	ext = strings.ToUpper(ext)
	dstFileDirPath := dstDirPath +"/noexif"
	if !tm.IsZero() {
		dstFileDirPath = dstDirPath +"/"+strftime.Format(format, tm)
	}
	errLogger = errLogger.WithFieldKeyVals("dstFileDirPath", dstFileDirPath)
	if err := os.MkdirAll(dstFileDirPath, 0755); err != nil {
		errLogger.Errorf("destination directories create error: %v", err)
		return false
	}

	tmpFile, err := ioutil.TempFile(tmpDirPath, srcFileStat.Name())
	if err != nil {
		errLogger.Errorf("temp file open error: %v", err)
		return false
	}
	defer tmpFile.Close()
	tmpFilePath := tmpFile.Name()
	errLogger = errLogger.WithFieldKeyVals("tmpFilePath", tmpFilePath)
	defer func() {
		if err := os.Remove(tmpFilePath); err != nil {
			errLogger.Warningf("temp file remove error: %v", err)
		}
	}()

	if _, err := srcFile.Seek(0, io.SeekStart); err != nil {
		errLogger.Errorf("source file seek error: %v", err)
		return false
	}

	buf := make([]byte, 32*1024)
	fh := fileHash{}
	sum := sha256.New()
	for {
		select {
		case <-ctx.Done():
			return false
		default:
		}
		nr, er := srcFile.Read(buf)
		if nr > 0 {
			nw, ew := tmpFile.Write(buf[:nr])
			if nw > 0 {
				fh.size += int64(nw)
				sum.Write(buf[:nw])
			}
			if ew != nil {
				errLogger.Errorf("temp file write error: %v", ew)
				return false
			}
			if nr != nw {
				errLogger.Errorf("temp file write error: %v", io.ErrShortWrite)
				return false
			}
		}
		if er != nil {
			if er != io.EOF {
				errLogger.Errorf("source file read error: %v", er)
				return false
			}
			break
		}
	}
	copy(fh.sum[:], sum.Sum(nil))

	if !fileHashSet(fh) {
		xlog.V(1).Infof("source file %q content is already copied, ignoring", srcFilePath)
		return true
	}

	sumStr := strings.ToUpper(hex.EncodeToString(fh.sum[:]))
	linked := false
	for i := 0; i < 1+len(sumStr)/4; i++ {
		path := dstFileDirPath +"/"+name+ext
		if i > 0 {
			k := (i-1)*4
			path = dstFileDirPath +"/"+name+"-"+sumStr[k:k+4]+ext
		}
		fileLock(path)
		if err := os.Link(tmpFile.Name(), path); err == nil || !os.IsExist(err) {
			fileUnlock(path)
			if err != nil {
				errLogger.Errorf("destination file %q link error: %v", path, err)
				break
			}
			linked = true
			errLogger = errLogger.WithFieldKeyVals("dstFilePath", path)
			break
		}
		fileUnlock(path)
	}

	if !linked {
		fileHashDel(fh)
		errLogger.Errorf("destination file collision error")
		return false
	}

	xlog.V(1).Infof("source file %q copied", srcFilePath)
	return true
}
