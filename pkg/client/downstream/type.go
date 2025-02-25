package downstream

import (
	tunnelPkg "github.com/guojianyu/swarm-tunnel/pkg"
)

/*
The protocol extension needs to implement the following methods
*/
type DownStream interface {
	//	Protocol() string
	//Receives messages from the upper stream
	Receive([]byte) error
	//Send a message to the upper stream
	Send() ([]byte, error)
	//close connection
	Close() error
}

// Extend the downstream protocol
func NewDownStream(tunnelMessage *tunnelPkg.TunnelMessage) (downstream DownStream, err error) {
	if tunnelMessage.Protocol == tunnelPkg.ProtocolWebsocket {
		downstream, err = NewWsClient(tunnelMessage)

	} else if tunnelMessage.Protocol == tunnelPkg.ProtocolSSH {
		downstream, err = NewSSHClient(tunnelMessage)

	}
	return
}
