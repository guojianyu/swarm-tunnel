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
	"bufio"
	"context"
	"fmt"
	"io"

	"k8s.io/klog"

	tunnelPkg "github.com/guojianyu/swarm-tunnel/pkg"
	corev1 "k8s.io/api/core/v1"
)

type LogsClient struct {
	stream io.ReadCloser
	reader *bufio.Reader
}

func NewLogsClient(tunnelMessage *tunnelPkg.TunnelMessage) (*LogsClient, error) {
	_, clientset, err := createClientset()
	if err != nil {
		return nil, fmt.Errorf("create clientset error: %v", err)
	}
	namespace := tunnelMessage.Logs.Namespace
	podName := tunnelMessage.Logs.Pod
	containerName := tunnelMessage.Logs.Container
	if namespace == "" {
		namespace = "default"
	}
	req := clientset.CoreV1().
		Pods(namespace).
		GetLogs(podName, &corev1.PodLogOptions{
			Container: containerName,
			Follow:    true,
			TailLines: int64Ptr(100),
		})

	stream, err := req.Stream(context.Background())
	if err != nil {
		return nil, fmt.Errorf("start stream error: %v", err)
	}

	return &LogsClient{
		stream: stream,
		reader: bufio.NewReader(stream),
	}, nil

}

func (logs *LogsClient) Receive(data []byte) error {
	return nil
}

func (logs *LogsClient) Send() (data []byte, err error) {
	message, err := logs.reader.ReadBytes('\n')
	klog.V(4).Infof("end message:%v", string(message))
	return message, err
}

func (logs *LogsClient) Close() error {
	return logs.stream.Close()
}

func int64Ptr(i int64) *int64 {
	return &i
}
