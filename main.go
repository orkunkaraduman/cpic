package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/goinsane/xlog"
	"github.com/rwcarlsen/goexif/exif"
	"github.com/rwcarlsen/goexif/mknote"

	"gitlab.com/orkunkaraduman/cpic/catalog"
)

func init() {
	exif.RegisterParsers(mknote.All...)
}

func main() {
	var (
		verbose   int
		debugMode bool
		workDir   string
	)
	flag.IntVar(&verbose, "v", 0, "verbose level")
	flag.BoolVar(&debugMode, "debug", false, "debug mode")
	flag.StringVar(&workDir, "w", ".", "working directory")
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
	xlog.SetOutputWriter(os.Stdout)
	xlog.SetOutputFlags(xlogOutputFlags)

	ctx, ctxCancel := context.WithCancel(context.Background())
	defer ctxCancel()
	go func() {
		ch := make(chan os.Signal, 1)
		signal.Notify(ch, os.Interrupt, os.Kill, syscall.SIGTERM)
		<-ch
		ctxCancel()
	}()

	workDir = prepareWorkDir(workDir)

	tmpDir := workDir +"/tmp"
	if err := os.MkdirAll(tmpDir, 0755); err != nil {
		xlog.Fatalf("temp directory create error: %v", err)
	}

	cat, err := catalog.New(workDir + "/cpic/catalog.sqlite3")
	if err != nil {
		xlog.Fatalf("catalog initialize error: %v", err)
	}
	defer cat.Close()

	args := flag.Args()
	if len(args) < 1 {
		xlog.Fatal("command required")
	}
	var cmd interface {
		Command() *command
		Prepare()
		Run(ctx context.Context)
	}
	cmdName := args[0]
	args = args[1:]
	flagSet := flag.NewFlagSet(cmdName, flag.ExitOnError)
	switch cmdName {
	case "import":
		c := &importCommand{}
		flagSet.StringVar(&c.Format, "f", "%Y/%Y-%m/%Y-%m-%d/%Y%m%d-%H%M%S", "destination file format")
		flagSet.BoolVar(&c.Remove, "r", false, "remove source")
		flagSet.StringVar(&c.ExtList, "e", "JPG,JPEG,PNG,TIFF,CR2,NEF", "extension list")
		flagSet.BoolVar(&c.FollowSymLinks, "l", false, "follow symbolic links")
		flagSet.Parse(args)
		c.SrcDirs = flagSet.Args()
		cmd = c
	default:
		xlog.Fatalf("command %q unknown", cmdName)
		return
	}

	*cmd.Command() = command{
		WorkDir: workDir,
		TmpDir:  tmpDir,
		Catalog: cat,
	}
	cmd.Prepare()

	xlog.Info("started")

	wg := new(sync.WaitGroup)

	wg.Add(1)
	go func() {
		defer wg.Done()
		cmd.Run(ctx)
		ctxCancel()
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		<-ctx.Done()
		xlog.Info("terminating")
	}()

	wg.Wait()
	xlog.Info("terminated")
	time.Sleep(250 * time.Millisecond)
}

func prepareWorkDir(workDir string) (absWorkDir string) {
	absWorkDir, err := filepath.Abs(workDir)
	if err != nil {
		xlog.Fatalf("working directory abs error: %v", err)
	}
	stat, err := os.Lstat(workDir)
	if err != nil {
		if !os.IsNotExist(err) {
			xlog.Fatalf("working directory stat error: %v", err)
		}
		err = os.Mkdir(workDir, 0755)
		if err != nil {
			xlog.Fatalf("working directory create error: %v", err)
		}
		stat, err = os.Lstat(workDir)
		if err != nil {
			xlog.Fatalf("working directory stat error: %v", err)
		}
	}
	if !stat.IsDir() {
		xlog.Fatalf("working directory %q is not a directory", workDir)
	}
	return
}
