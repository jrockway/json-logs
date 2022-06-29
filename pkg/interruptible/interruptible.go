package interruptible

import (
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
)

var (
	ErrInterrupted = errors.New("interrupted")
	ErrClosed      = errors.New("read on closed reader")
)

// Reader is an io.ReadCloser that can be interrupted at will.
type Reader struct {
	r      io.ReadCloser
	sigCh  chan os.Signal
	closed bool
}

var _ io.ReadCloser = new(Reader)

// NewReader returns a new reader that will be interrupted by the provided signals.
func NewReader(r io.ReadCloser, sigs ...os.Signal) *Reader {
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, sigs...)
	return &Reader{
		r:     r,
		sigCh: sigCh,
	}
}

// Read implements io.Reader
func (r *Reader) Read(p []byte) (int, error) {
	if r.closed {
		return 0, ErrClosed
	}
	var n int
	var err error
	ch := make(chan struct{})
	buf := make([]byte, len(p), cap(p))
	go func() {
		n, err = r.r.Read(buf)
		close(ch)
	}()

	select {
	case <-ch:
		copy(p, buf[:n])
		return n, err
	case <-r.sigCh:
		if err := r.Close(); err != nil {
			return 0, fmt.Errorf("close after interrupt: %v (was %w)", err, ErrInterrupted)
		}
		// ch gets closed whenever its Read finally returns.
		return 0, ErrInterrupted
	}
}

// Close implements io.Closer.
func (r *Reader) Close() error {
	r.closed = true
	signal.Stop(r.sigCh)
	if err := r.r.Close(); err != nil {
		return fmt.Errorf("close underlying reader: %w", err)
	}
	return nil
}
