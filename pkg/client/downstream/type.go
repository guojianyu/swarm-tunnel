/*
Copyright 2025 The Guojianyu Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
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
