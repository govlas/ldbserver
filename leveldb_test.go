package ldbserver

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	pio "github.com/gogo/protobuf/io"
	"github.com/stretchr/testify/assert"
)

func serveCommand(t *testing.T, db DBServer, command TransportRequest_Command, key, value []byte, mt MarshalingType, checkOk bool) *TransportResponse {

	var (
		out = bytes.NewBuffer(nil)
		in  = bytes.NewBuffer(nil)

		tr = newRwTransporter(out, in, mt)
	)

	req := &TransportRequest{
		Id:      key,
		Command: command.Enum(),
		Body:    &TransportBody{Data: value},
	}
	SetBodyChecksum(req.Body)

	switch mt {
	case MarshalingTypeJson:
		enc := json.NewEncoder(out)
		assert.NoError(t, enc.Encode(req), "Json")
	case MarshalingTypeProtobuf:
		enc := pio.NewUint32DelimitedWriter(out, binary.LittleEndian)
		assert.NoError(t, enc.WriteMsg(req), "Protobuf")
	}

	if assert.NoError(t, db.serve(tr), "db.serve") {

		resp := &TransportResponse{}
		switch mt {
		case MarshalingTypeJson:
			dec := json.NewDecoder(in)
			assert.NoError(t, dec.Decode(resp), "Json")
		case MarshalingTypeProtobuf:
			dec := pio.NewUint32DelimitedReader(in, binary.LittleEndian, 1024)
			assert.NoError(t, dec.ReadMsg(resp), "Protobuf")
		}
		if checkOk {
			assert.Equal(t, resp.Id, key, "Response")
			assert.Equal(t, resp.GetStatus(), TransportResponse_OK, "Response")
			assert.True(t, CheckBody(resp.Body), "Check resp body")
		}
		return resp
	}
	return nil
}

func TestLevelDB(t *testing.T) {

	tempdir := os.TempDir()
	path := filepath.Join(tempdir, fmt.Sprintf("goleveldb-test%d0%d", os.Getuid(), os.Getpid()))
	//	t.Log(path)
	db, err := NewLevelDbServer(path)
	if assert.NoError(t, err, "NewLevelDbServer") {
		defer func() {
			db.Close()
			os.RemoveAll(path)
		}()

		var (
			key   = []byte("hello")
			value = []byte("world")
		)
		serveCommand(t, db, TransportRequest_PUT, key, value, MarshalingTypeJson, true)
		serveCommand(t, db, TransportRequest_GET, key, nil, MarshalingTypeJson, true)
		serveCommand(t, db, TransportRequest_DELETE, key, nil, MarshalingTypeJson, true)

		serveCommand(t, db, TransportRequest_PUT, key, value, MarshalingTypeProtobuf, true)
		serveCommand(t, db, TransportRequest_GET, key, nil, MarshalingTypeProtobuf, true)
		serveCommand(t, db, TransportRequest_DELETE, key, nil, MarshalingTypeProtobuf, true)

		if resp := serveCommand(t, db, TransportRequest_GET, key, nil, MarshalingTypeProtobuf, false); resp != nil {
			assert.Equal(t, string(resp.Body.Data), "leveldb: not found", "Not found error")
		}
	}
}
