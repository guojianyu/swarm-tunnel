package downstream

import (
	"context"
	"io"

	"github.com/gorilla/websocket"
)

type WsClient struct {
	Socket *websocket.Conn
	out    *io.PipeWriter
	in     *io.PipeReader
	host   string
	path   string
}
type DownStream interface {
	Connector() error
	Processor(context.Context) error
}

type SshClient struct{}
type ExecClient struct{}
