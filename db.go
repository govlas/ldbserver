package ldbserver

import (
	"errors"

	"github.com/syndtr/goleveldb/leveldb"
)

type DbServer struct {
	db *leveldb.DB
}

func NewDbServer(dbname string) (s *DbServer, err error) {
	s = new(DbServer)
	s.db, err = leveldb.OpenFile(dbname, nil)
	if err != nil {
		return
	}
	return
}

func (s *DbServer) Close() {
	if s != nil && s.db != nil {
		s.db.Close()
		s.db = nil
	}
}

func (s *DbServer) serve(tr Transporter) error {
	if s == nil || s.db == nil {
		return errors.New("ldbserver.Server.Serve: uninitialized server, please use ldbserver.NewServer to create server")
	}
	req, err := tr.GetRequest()
	if err != nil {
		return err
	}

	var resp *TransportResponse
	reqId := req.GetId()
	if reqId == nil {
		resp = MakeErrorResponse(TransportResponse_FAIL, errors.New("no id in request"))

	} else {

		resp = &TransportResponse{}

		switch *req.Command {

		case TransportRequest_GET:
			if val, err := s.db.Get(reqId, nil); err == nil {
				resp.Status = TransportResponse_OK.Enum()
				resp.Body = &TransportBody{Data: val}
			} else {
				resp = MakeErrorResponse(TransportResponse_FAIL, err)
			}

		case TransportRequest_PUT:
			if req.Body != nil && CheckBody(req.Body) {
				if err := s.db.Put(reqId, req.Body.Data, nil); err == nil {
					resp.Status = TransportResponse_OK.Enum()
				} else {
					resp = MakeErrorResponse(TransportResponse_FAIL, err)
				}
			} else {
				resp = MakeErrorResponse(TransportResponse_FAIL, errors.New("Bad data in request"))
			}

		case TransportRequest_DELETE:
			if err := s.db.Delete(reqId, nil); err == nil {
				resp.Status = TransportResponse_OK.Enum()
			} else {
				resp = MakeErrorResponse(TransportResponse_FAIL, err)
			}

		default:
			resp = MakeErrorResponse(TransportResponse_FAIL, errors.New("unsupported command"))
		}
		resp.Id = append([]byte(nil), reqId...)
	}
	SetBodyChecksum(resp.Body)
	if err := tr.SendResponse(resp); err != nil {
		return err
	}

	return nil
}
