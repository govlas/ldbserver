package api

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"runtime"

	pio "github.com/gogo/protobuf/io"
	"github.com/govlas/ldbserver"
)

type Client struct {
	network    string
	host       string
	marshaling ldbserver.MarshalingType
	conn       io.ReadWriteCloser
}

func NewClient(network string, host string, mt ldbserver.MarshalingType) (cl *Client, err error) {
	cl = new(Client)
	cl.network = network
	cl.host = host
	cl.marshaling = mt

	switch network {
	case "unix", "tcp":
		cl.conn, err = net.Dial(network, host)
		if err != nil {
			return nil, err
		}
		runtime.SetFinalizer(cl, func(c *Client) {
			c.Close()
		})
	}

	return
}

func (cl *Client) doRequest(req *ldbserver.TransportRequest) (resp *ldbserver.TransportResponse, err error) {

	if cl == nil {
		return nil, errors.New("client.DoRequest: call of nil reference")
	}

	var (
		w io.Writer
		r io.Reader
	)

	if cl.network == "http" {
		w = bytes.NewBuffer(nil)
	} else {
		r = cl.conn
		w = cl.conn
	}

	if w != nil {
		switch cl.marshaling {
		case ldbserver.MarshalingTypeJson:
			enc := json.NewEncoder(w)
			err = enc.Encode(req)
		case ldbserver.MarshalingTypeProtobuf:
			enc := pio.NewUint32DelimitedWriter(w, binary.LittleEndian)
			err = enc.WriteMsg(req)
		}
		if err != nil {
			return
		}
		if cl.network == "http" {
			hresp, err := http.Post("http://"+cl.host, "", w.(io.Reader))
			if err != nil {
				return nil, err
			}
			defer hresp.Body.Close()
			r = hresp.Body
		}
		resp = &ldbserver.TransportResponse{}
		switch cl.marshaling {
		case ldbserver.MarshalingTypeJson:
			dec := json.NewDecoder(r)
			err = dec.Decode(resp)
		case ldbserver.MarshalingTypeProtobuf:
			dec := pio.NewUint32DelimitedReader(r, binary.LittleEndian, 1024*1024)
			err = dec.ReadMsg(resp)
		}

	} else {
		return nil, errors.New("client.DoRequest: no connection")
	}
	return
}

func (cl *Client) Close() {
	if cl != nil && cl.conn != nil {
		cl.conn.Close()
	}
}

func (cl *Client) Get(key []byte) (value []byte, err error) {
	req := ldbserver.TransportRequest{
		Id:      key,
		Command: ldbserver.TransportRequest_GET.Enum(),
	}

	if resp, err := cl.doRequest(&req); err == nil {

		if *resp.Status != ldbserver.TransportResponse_OK {
			return nil, errors.New(string(resp.Body.Data))
		}

		if ldbserver.CheckBody(resp.Body) {
			value = resp.Body.Data
		} else {
			return nil, errors.New("client.Get: bad checksum for returning data")
		}
	} else {
		return nil, err
	}
	return
}

func (cl *Client) Put(key, value []byte) error {
	req := ldbserver.TransportRequest{
		Id:      key,
		Command: ldbserver.TransportRequest_PUT.Enum(),
		Body:    &ldbserver.TransportBody{Data: value},
	}
	ldbserver.SetBodyChecksum(req.Body)

	if resp, err := cl.doRequest(&req); err == nil {
		if *resp.Status != ldbserver.TransportResponse_OK {
			return errors.New(string(resp.Body.Data))
		}

	} else {
		return err
	}
	return nil
}

func (cl *Client) Delete(key []byte) error {
	req := ldbserver.TransportRequest{
		Id:      key,
		Command: ldbserver.TransportRequest_DELETE.Enum(),
	}

	if resp, err := cl.doRequest(&req); err == nil {
		if *resp.Status != ldbserver.TransportResponse_OK {
			return errors.New(string(resp.Body.Data))
		}

	} else {
		return err
	}
	return nil
}
