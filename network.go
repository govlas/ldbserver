package ldbserver

import (
	"bytes"
	"errors"
	"io"
	"net"
	"net/http"

	"github.com/govlas/logger"
)

type NetworkServer struct {
	netName string
	host    string
	stop    chan int
}

func checkNetworkName(n string) bool {
	switch n {
	case "unix", "tcp", "http":
		return true
	default:
		return false
	}
}

func NewNetworkServer(nName string, host string) *NetworkServer {
	ret := new(NetworkServer)
	ret.netName = nName
	ret.host = host
	ret.stop = make(chan int)
	return ret
}

func (serv *NetworkServer) Stop() {
	close(serv.stop)
}

func (serv *NetworkServer) ListenAndServe(db DBServer, tf TransporterFactory) error {
	var network string
	if serv.netName == "http" {
		network = "tcp"
	} else {
		network = serv.netName
	}
	oln, err := net.Listen(network, serv.host)
	if err != nil {
		return err
	}
	ln := newStoppableListener(oln)
	defer ln.Close()
	go func() {
		<-serv.stop
		close(ln.stop)
	}()

	switch serv.netName {
	case "unix", "tcp":
		for {
			conn, err := ln.Accept()
			if err != nil {
				if err == ErrStopped {
					return err
				}
				logger.Warning("error on accept stream socket %v", err)
				continue
			}

			go func(conn net.Conn) {
				defer conn.Close()
				tr := tf.NewTransporter(conn, conn)
				for {
					err := db.serve(tr)
					if err != nil {
						if err != io.EOF {
							logger.Warning("warning on read/write stream socket: %v", err)
						}
						break
					}
				}
			}(conn)
		}
	case "http":
		handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			buf := bytes.NewBuffer(nil)
			tr := tf.NewTransporter(r.Body, buf)
			err := db.serve(tr)

			if err != nil {
				logger.Warning("warning on read/write http: %v", err)
				w.WriteHeader(http.StatusInternalServerError)
			} else {
				w.Write(buf.Bytes())
			}

		})
		s := http.Server{Handler: handler}
		return s.Serve(ln)
	default:
		return errors.New("unsupported network")
	}
}
