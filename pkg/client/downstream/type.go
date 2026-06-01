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
	"fmt"

	tunnelPkg "github.com/guojianyu/swarm-tunnel/pkg"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

var (
	Kubeconfig = ""
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
	switch tunnelMessage.Protocol {
	case tunnelPkg.ProtocolWebsocket:
		downstream, err = NewWsClient(tunnelMessage)
		return
	case tunnelPkg.ProtocolSSH:
		downstream, err = NewSSHClient(tunnelMessage)
		return
	case tunnelPkg.ProtocolLogs:
		downstream, err = NewLogsClient(tunnelMessage)
		return
	case tunnelPkg.ProtocolExec:
		downstream, err = NewExecClient(tunnelMessage)
		return
	default:
		return nil, fmt.Errorf("do not support %v protocol!", tunnelMessage.Protocol)
	}
}

func createClientset() (*rest.Config, *kubernetes.Clientset, error) {
	var cfg *rest.Config
	var err error
	if Kubeconfig != "" {
		cfg, err = clientcmd.BuildConfigFromFlags("", Kubeconfig)
	} else {
		cfg, err = rest.InClusterConfig()
	}
	if err != nil {
		return cfg, nil, err
	}
	return cfg, kubernetes.NewForConfigOrDie(cfg), nil
}
