package util

import (
	"context"
	"hash"
	"io"
)

func Copy(ctx context.Context, w io.Writer, r io.Reader, buf []byte, sums []hash.Hash) (written int64, err error) {
	if buf == nil {
		buf = make([]byte, 32*1024)
	}
	for {
		select {
		case <-ctx.Done():
			err = ctx.Err()
			return
		default:
		}
		nr, er := r.Read(buf)
		if nr > 0 {
			nw, ew := w.Write(buf[:nr])
			if nw > 0 {
				written += int64(nw)
				for _, h := range sums {
					h.Write(buf[:nw])
				}
			}
			if ew != nil {
				err = ew
				return
			}
			if nr != nw {
				err = io.ErrShortWrite
				return
			}
		}
		if er != nil {
			if er != io.EOF {
				err = er
			}
			return
		}
	}
}
