package util

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

type fileScanRecursion struct {
	depth         uint
	firstRoot     string
	firstRootStat os.FileInfo
}

func FileScan(ctx context.Context, root string, pathCh chan<- string, followSymLinks bool) error {
	root, err := filepath.Abs(root)
	if err != nil {
		return err
	}
	r := fileScanRecursion{}
	r.firstRoot = root
	r.firstRootStat, err = os.Lstat(root)
	if err != nil {
		return err
	}
	if !r.firstRootStat.IsDir() {
		return fmt.Errorf("root %s must be directory", root)
	}
	return fileScan(ctx, root, pathCh, followSymLinks, r)
}

func fileScan(ctx context.Context, root string, pathCh chan<- string, followSymLinks bool, r fileScanRecursion) error {
	rootHandle, err := os.OpenFile(root, os.O_RDONLY, 0)
	if err != nil {
		return err
	}
	defer rootHandle.Close()

	stats, err := rootHandle.Readdir(0)
	if err != nil {
		return err
	}

	for _, stat := range stats {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		path := root + string(os.PathSeparator) + stat.Name()
		mode := stat.Mode()

		for {
			if os.SameFile(stat, r.firstRootStat) {
				break
			}

			if mode&os.ModeDir != 0 {
				r2 := r
				r2.depth++
				if err := fileScan(ctx, path, pathCh, followSymLinks, r2); err != nil {
					return err
				}
				break
			}

			if mode&os.ModeType == 0 {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case pathCh <- path:
				}
				break
			}

			if mode&os.ModeSymlink != 0 {
				if !followSymLinks {
					break
				}

				path, err = filepath.EvalSymlinks(path)
				if err != nil {
					return err
				}
				stat, err = os.Lstat(path)
				if err != nil {
					return err
				}
				mode = stat.Mode()
				if mode&os.ModeSymlink != 0 {
					continue
				}

				pathLen := len(path)
				firstRootLen := len(r.firstRoot)
				if firstRootLen > pathLen {
					continue
				}
				if firstRootLen+1 <= pathLen && !os.IsPathSeparator(path[firstRootLen]) {
					continue
				}
				if root2 := path[:firstRootLen]; strings.EqualFold(root2, r.firstRoot) {
					root2Stat, err := os.Lstat(root2)
					if err != nil {
						return err
					}
					if os.SameFile(root2Stat, r.firstRootStat) {
						break
					}
				}
				continue
			}

			break
		}
	}

	return nil
}
