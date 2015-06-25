package api_test

import (
	"testing"

	"github.com/govlas/ldbserver"
	"github.com/govlas/ldbserver/api"
	"github.com/stretchr/testify/assert"
)

func testClient(t *testing.T, nt string, host string, mt ldbserver.MarshalingType) {
	cli, err := api.NewClient(nt, host, mt)
	assert.NoError(t, err, "api.NewClient")
	assert.NoError(t, cli.Put([]byte("hello"), []byte("world")), "api.client.Put")

	res, err := cli.Get([]byte("hello"))
	assert.NoError(t, err, "api.client.Get")

	assert.Equal(t, res, []byte("world"), "api.client.Get")

	err = cli.Delete([]byte("hello"))
	assert.NoError(t, err, "api.client.Delete")

	_, err = cli.Get([]byte("hello"))
	assert.Error(t, err, "api.client.Get empty")
}

func TestClient(t *testing.T) {
	//testClient(t, "http", "localhost:8080", ldbserver.MarshalingTypeJson)
	testClient(t, "unix", "/tmp/ldbserver.sock", ldbserver.MarshalingTypeJson)
}
