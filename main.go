package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"syscall"

	"github.com/goinsane/xlog"
	"github.com/rwcarlsen/goexif/exif"
	"github.com/rwcarlsen/goexif/mknote"
)

func init() {
	exif.RegisterParsers(mknote.All...)
}

func main() {
	var (
		verbose        int
		debugMode      bool
		srcDirPath     string
		dstDirPath     string
		followSymLinks bool
		extList        string
		format         string
		rm             bool
	)
	flag.IntVar(&verbose, "v", 0, "verbose level")
	flag.BoolVar(&debugMode, "debug", false, "debug mode")
	flag.StringVar(&srcDirPath, "s", ".", "source directory")
	flag.StringVar(&dstDirPath, "d", ".", "destination directory")
	flag.BoolVar(&followSymLinks, "l", false, "follow symbolic links")
	flag.StringVar(&extList, "e", "JPG,JPEG,PNG,TIFF,CR2,NEF", "extention list")
	flag.StringVar(&format, "f", "%Y/%m/%d", "destination directory format")
	flag.BoolVar(&rm, "r", false, "remove source")
	flag.Parse()

	xlogOutputFlags := xlog.OutputFlagDate |
		xlog.OutputFlagTime |
		xlog.OutputFlagSeverity |
		xlog.OutputFlagPadding |
		//xlog.OutputFlagShortFunc |
		//xlog.OutputFlagShortFile |
		xlog.OutputFlagFields
	if debugMode {
		xlog.SetSeverity(xlog.SeverityDebug)
		xlogOutputFlags |= xlog.OutputFlagStackTrace
	}
	xlog.SetStackTraceSeverity(xlog.SeverityWarning)
	xlog.SetVerbose(xlog.Verbose(verbose))
	xlog.SetOutputWriter(os.Stderr)
	xlog.SetOutputFlags(xlogOutputFlags)

	ctx, ctxCancel := context.WithCancel(context.Background())
	defer ctxCancel()
	go func() {
		ch := make(chan os.Signal, 1)
		signal.Notify(ch, os.Interrupt, os.Kill, syscall.SIGTERM)
		<-ch
		ctxCancel()
	}()

	var err error

	var stat os.FileInfo
	srcDirPath, err = filepath.Abs(srcDirPath)
	if err != nil {
		xlog.Fatalf("source directory %q abs error: %v", srcDirPath, err)
		return
	}
	stat, err = os.Lstat(srcDirPath)
	if err != nil {
		xlog.Fatalf("source directory %q stat error: %v", srcDirPath, err)
		return
	}
	if !stat.IsDir() {
		xlog.Fatalf("source directory %q is not directory", srcDirPath)
		return
	}

	dstDirPath, err = filepath.Abs(dstDirPath)
	if err != nil {
		xlog.Fatalf("destination directory %q abs error: %v", dstDirPath, err)
		return
	}
	stat, err = os.Lstat(dstDirPath)
	if err != nil {
		if !os.IsNotExist(err) {
			xlog.Fatalf("destination directory %q stat error: %v", dstDirPath, err)
			return
		}
		err = os.Mkdir(dstDirPath, 0755)
		if err != nil {
			xlog.Fatalf("destination directory %q create error: %v", dstDirPath, err)
			return
		}
		stat, err = os.Lstat(dstDirPath)
		if err != nil {
			xlog.Fatalf("destination directory %q stat error: %v", dstDirPath, err)
			return
		}
	}
	if !stat.IsDir() {
		xlog.Fatalf("destination directory %q is not directory", dstDirPath)
		return
	}

	tmpDirPath := dstDirPath +"/tmp"
	if err := os.MkdirAll(tmpDirPath, 0755); err != nil {
		xlog.Fatalf("temp directories %q create error: %v", tmpDirPath, err)
		return
	}

	extList = strings.ToUpper(extList)
	extentions := make(map[string]struct{}, 128)
	for _, ext := range strings.Split(extList, ",") {
		ext = strings.TrimSpace(ext)
		if ext == "" {
			continue
		}
		extentions[ext] = struct{}{}
	}

	xlog.Info("cpic started")

	wg := new(sync.WaitGroup)

	//wg.Add(1)
	go func() {
		//defer wg.Done()
		<-ctx.Done()
		xlog.Info("cpic terminating")
	}()

	srcFilePathCh := make(chan string)

	wg.Add(1)
	go fileScan(ctx, wg, srcDirPath, srcFilePathCh, followSymLinks, extentions)

	for i := 0; i < runtime.NumCPU(); i++ {
		wg.Add(1)
		go copyFiles(ctx, wg, srcFilePathCh, dstDirPath, format, rm, tmpDirPath)
	}

	wg.Wait()
	xlog.Info("cpic terminated")
}
