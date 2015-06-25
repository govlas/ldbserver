package ldbserver

import (
	"errors"
	"net"
	"time"
)

type DeadLineListener interface {
	SetDeadline(t time.Time) (err error)
}

type stoppableListener struct {
	net.Listener
	stop chan int
}

var ErrStopped = errors.New("listener stopped")

func newStoppableListener(nl net.Listener) *stoppableListener {
	return &stoppableListener{nl, make(chan int)}
}

func (sl *stoppableListener) Stop() {
	close(sl.stop)
}

func (sl *stoppableListener) Accept() (c net.Conn, err error) {
	for {
		dl, ok := sl.Listener.(DeadLineListener)
		if !ok {
			return nil, errors.New("listener don't have SetDeadline method")
		}
		dl.SetDeadline(time.Now().Add(time.Second))
		c, err = sl.Listener.Accept()

		select {
		case <-sl.stop:
			return nil, ErrStopped
		default:
		}

		if err != nil {
			netErr, ok := err.(net.Error)
			if ok && netErr.Timeout() && netErr.Temporary() {
				continue
			}
			return nil, err
		}
		return

	}
}
