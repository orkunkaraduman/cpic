package exiftool

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"strings"
	"time"
	"unicode"
)

var (
	ErrInvalidFormat    = errors.New("invalid format")
	ErrDuplicateTag     = errors.New("duplicate tag")
	ErrDateTimeNotFound = errors.New("datetime not found")
	ErrTimeZoneNotFound = errors.New("timezone not found")
)

type Tags map[string]string

func ReadTagsFromFile(path string) (Tags, error) {
	return ReadTagsFromFileContext(context.Background(), path)
}

func ReadTagsFromFileContext(ctx context.Context, path string) (Tags, error) {
	cmd := exec.CommandContext(ctx, "exiftool", "-s2", path)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	defer stdout.Close()
	err = cmd.Start()
	if err != nil {
		return nil, err
	}
	t, err := ReadTags(stdout)
	if err != nil {
		return nil, err
	}
	err = cmd.Wait()
	if err, ok := err.(*exec.ExitError); ok && err.ExitCode() != 0 && t["Error"] != "" {
		return nil, fmt.Errorf("%w: %s", err, t["Error"])
	}
	if err != nil {
		return nil, err
	}
	return t, nil
}

func ReadTags(r io.Reader) (Tags, error) {
	t := make(Tags, 128)
	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		s := scanner.Text()
		i := strings.Index(s, ":")
		if i < 0 || i >= len(s)-1 || s[i+1] != ' ' {
			return nil, ErrInvalidFormat
		}
		name := strings.TrimRightFunc(s[:i], unicode.IsSpace)
		value := s[i+2:]
		if _, ok := t[name]; ok {
			return nil, ErrDuplicateTag
		}
		t[name] = value
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return t, nil
}

func (t Tags) DateTime() (tm time.Time, err error) {
	_, _, loc, _ := t.TimeZone()
	if loc == nil {
		loc = time.Local
	}
	for _, tagName := range []string{"SubSecDateTimeOriginal", "SubSecCreateDate", "DateTimeOriginal", "CreateDate"} {
		tagValue, ok := t[tagName]
		if !ok {
			continue
		}
		tm, err := time.ParseInLocation("2006:01:02 15:04:05", tagValue, loc)
		if err != nil {
			continue
		}
		return tm, nil
	}
	return time.Time{}, ErrDateTimeNotFound
}

func (t Tags) TimeZone() (name string, offset int, loc *time.Location, err error) {
	for _, tagName := range []string{"TimeZone"} {
		tagValue, ok := t[tagName]
		if !ok {
			continue
		}
		tm, err := time.Parse("-07:00", tagValue)
		if err != nil {
			continue
		}
		name, offset := tm.Zone()
		return name, offset, tm.Location(), nil
	}
	return "", 0, nil, ErrTimeZoneNotFound
}
