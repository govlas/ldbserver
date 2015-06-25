package ldbserver

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"hash/crc32"
	"io"

	pio "github.com/gogo/protobuf/io"
	"github.com/gogo/protobuf/proto"
)

//go:generate protoc --gogo_out=. -I.:$GOPATH/src:/usr/local/include transport.proto

type Transporter interface {
	GetRequest() (*TransportRequest, error)
	SendResponse(*TransportResponse) error
}

type MarshalingType int

const (
	MarshalingTypeJson MarshalingType = iota
	MarshalingTypeProtobuf
)

type rwTransporter struct {
	mt   MarshalingType
	req  io.Reader
	resp io.Writer
}

func newRwTransporter(r io.Reader, w io.Writer, mt MarshalingType) *rwTransporter {
	ret := new(rwTransporter)
	ret.mt = mt
	ret.req = r
	ret.resp = w
	return ret
}

func (rw *rwTransporter) GetRequest() (req *TransportRequest, err error) {
	req = &TransportRequest{}
	switch rw.mt {
	case MarshalingTypeJson:
		dec := json.NewDecoder(rw.req)
		err = dec.Decode(req)
		return
	case MarshalingTypeProtobuf:
		dec := pio.NewUint32DelimitedReader(rw.req, binary.LittleEndian, 1024*1024)
		err = dec.ReadMsg(req)
		return
	}
	return nil, errors.New("unsupported marshaling type")
}
func (rw *rwTransporter) SendResponse(resp *TransportResponse) error {
	switch rw.mt {
	case MarshalingTypeJson:
		enc := json.NewEncoder(rw.resp)
		return enc.Encode(resp)
	case MarshalingTypeProtobuf:
		enc := pio.NewUint32DelimitedWriter(rw.resp, binary.LittleEndian)
		return enc.WriteMsg(resp)
	}
	return errors.New("unsupported marshaling type")
}

// ------------

func MakeErrorResponse(code TransportResponse_Status, err error) *TransportResponse {
	if err == nil {
		return &TransportResponse{Status: code.Enum()}
	}
	return &TransportResponse{Status: code.Enum(), Body: &TransportBody{Data: []byte(err.Error())}}
}

func CheckBody(body *TransportBody) bool {
	if body == nil {
		return true
	}
	data := body.GetData()
	if data == nil {
		if body.GetChecksum() == 0 {
			return true
		} else {
			return false
		}
	}
	chk := crc32.ChecksumIEEE(data)
	return chk == body.GetChecksum()
}

func SetBodyChecksum(body *TransportBody) {
	if body == nil {
		return
	}
	data := body.GetData()
	if data == nil {
		body.Checksum = proto.Uint32(0)
		return
	}
	body.Checksum = proto.Uint32(crc32.ChecksumIEEE(data))
}