package main

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/goinsane/xlog"
)

type fileScanRecursion struct {
	depth           uint
	firstSrcDirPath string
	firstSrcDirStat os.FileInfo
}

func fileScan(ctx context.Context, wg *sync.WaitGroup, srcDirPath string, srcFilePathCh chan<- string, followSymLinks bool, extentions map[string]struct{}) {
	fileScan2(ctx, wg, srcDirPath, srcFilePathCh, followSymLinks, extentions, fileScanRecursion{})
	close(srcFilePathCh)
	wg.Done()
}

func fileScan2(ctx context.Context, wg *sync.WaitGroup, srcDirPath string, srcFilePathCh chan<- string, followSymLinks bool, extentions map[string]struct{}, r fileScanRecursion) {
	srcDir, err := os.OpenFile(srcDirPath, os.O_RDONLY, os.ModeDir)
	if err != nil {
		xlog.Errorf("source directory %q open error: %v", srcDirPath, err)
		return
	}
	defer srcDir.Close()

	if r.depth == 0 {
		r.firstSrcDirPath = srcDirPath
		r.firstSrcDirStat, err = srcDir.Stat()
		if err != nil {
			xlog.Errorf("source directory %q stat error: %v", srcDirPath, err)
			return
		}
	}

	stats, err := srcDir.Readdir(0)
	if err != nil {
		xlog.Errorf("source directory %q readdir error: %v", srcDirPath, err)
		return
	}

	for _, stat := range stats {
		done := false
		select {
		case <-ctx.Done():
			done = true
		default:
		}
		if done {
			break
		}

		path := srcDirPath + "/" + stat.Name()
		mode := stat.Mode()

		for tryAgain := true; tryAgain; {
			tryAgain = false
			switch {
			case mode&os.ModeDir != 0:
				if os.SameFile(stat, r.firstSrcDirStat) {
					break
				}
				r2 := r
				r2.depth++
				fileScan2(ctx, wg, path, srcFilePathCh, followSymLinks, extentions, r2)
			case mode&os.ModeSymlink != 0:
				if !followSymLinks {
					break
				}
				path2, err := filepath.EvalSymlinks(path)
				if err != nil {
					xlog.Errorf("sym-link %q eval error: %v", path, err)
					break
				}
				path = path2
				stat, err = os.Lstat(path)
				if err != nil {
					xlog.Errorf("sym-link target %q stat error: %v", path, err)
					break
				}
				mode = stat.Mode()
				pathLen := len(path)
				firstSrcDirPathLen := len(r.firstSrcDirPath)
				if firstSrcDirPathLen > pathLen {
					tryAgain = true
					break
				}
				if firstSrcDirPathLen+1 <= pathLen && !os.IsPathSeparator(path[firstSrcDirPathLen]) {
					break
				}
				path2 = path[:firstSrcDirPathLen]
				if path2 == r.firstSrcDirPath {
					break
				}
				if stat.Mode()&os.ModeSymlink == 0 && strings.EqualFold(path2, r.firstSrcDirPath) {
					stat2, err := os.Lstat(path2)
					if err != nil {
						xlog.Errorf("source of sym-link target %q stat error: %v", path2, err)
						break
					}
					if os.SameFile(stat, stat2) {
						break
					}
				}
				tryAgain = true
			case mode&os.ModeType == 0:
				if _, ok := extentions[strings.ToUpper(strings.TrimPrefix(filepath.Ext(path), "."))]; !ok {
					break
				}
				select {
				case <-ctx.Done():
					done = true
				case srcFilePathCh <- path:
				}
			}
		}
	}
}
