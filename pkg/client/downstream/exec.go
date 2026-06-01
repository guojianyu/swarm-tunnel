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
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/remotecommand"
	"k8s.io/klog"

	tunnelPkg "github.com/guojianyu/swarm-tunnel/pkg"
)

type stream struct {
	inWriter  *io.PipeWriter
	inReader  *io.PipeReader
	outWriter *io.PipeWriter
	outReader *io.PipeReader
}

type ExecClient struct {
	*stream
	config    *rest.Config
	clientset *kubernetes.Clientset
	ctx       context.Context
	cancel    context.CancelFunc
}

func NewExecClient(tunnelMessage *tunnelPkg.TunnelMessage) (*ExecClient, error) {
	stream := &stream{}
	stream.inReader, stream.inWriter = io.Pipe()
	stream.outReader, stream.outWriter = io.Pipe()
	config, clientset, err := createClientset()
	if err != nil {
		return nil, fmt.Errorf("create clientset error: %v", err)
	}
	client := &ExecClient{
		stream:    stream,
		config:    config,
		clientset: clientset,
	}
	client.ctx, client.cancel = context.WithCancel(context.Background())
	namespace := tunnelMessage.Exec.Namespace
	podName := tunnelMessage.Exec.Pod
	containerName := tunnelMessage.Exec.Container
	command := tunnelMessage.Exec.Command
	if command == "" {
		command = "sh"
	}
	klog.Infof("command: %v", command)
	req := clientset.CoreV1().
		RESTClient().
		Post().
		Resource("pods").
		Name(podName).
		Namespace(namespace).
		SubResource("exec")

	req.VersionedParams(&corev1.PodExecOptions{
		Container: containerName,
		Command: []string{
			command,
		},
		Stdin:  true,
		Stdout: true,
		Stderr: true,
		TTY:    true,
	}, scheme.ParameterCodec)
	exec, err := remotecommand.NewSPDYExecutor(
		config,
		http.MethodPost,
		req.URL(),
	)
	if err != nil {
		return nil, err
	}
	go func() {
		err = exec.StreamWithContext(
			client.ctx,
			remotecommand.StreamOptions{
				Stdin:  stream,
				Stdout: stream,
				Stderr: stream,
				Tty:    true,
			},
		)

		if err != nil {
			stream.outWriter.Write([]byte(err.Error()))
			if strings.Contains(err.Error(), "failed to start exec") {
				showshell, _ := client.execCommand(namespace, podName, containerName, []string{"cat", "/etc/shells"})
				stream.outWriter.Write([]byte(showshell))
			}
			client.Close()
			klog.V(4).Infof("StreamWithContext exit: %v", err)
		}
	}()

	return client, nil
}

func (s *stream) Read(p []byte) (int, error) {
	return s.inReader.Read(p)
}

func (s *stream) Write(p []byte) (int, error) {
	return s.outWriter.Write(p)
}

func (s *stream) Close() error {
	s.inReader.Close()
	s.outReader.Close()
	s.inWriter.Close()
	return s.outWriter.Close()
}

func (exec *ExecClient) Receive(data []byte) error {
	_, err := exec.stream.inWriter.Write(data)
	return err
}

func (exec *ExecClient) Send() (data []byte, err error) {
	buffer := make([]byte, 1024)
	n, err := exec.outReader.Read(buffer)
	if err != nil {
		klog.V(4).Infof("Error reading from stdout: %v", err)
		return nil, err
	}
	return buffer[:n], err
}

func (exec *ExecClient) Close() error {
	exec.cancel()
	return exec.stream.Close()
}

func (e *ExecClient) execCommand(namespace, podName, containerName string, cmd []string) (string, error) {
	ctx := context.Background()

	req := e.clientset.CoreV1().RESTClient().Post().
		Resource("pods").
		Name(podName).
		Namespace(namespace).
		SubResource("exec").
		VersionedParams(&corev1.PodExecOptions{
			Container: containerName,
			Command:   cmd,
			Stdout:    true,
			Stderr:    true,
		}, scheme.ParameterCodec)

	exec, err := remotecommand.NewSPDYExecutor(e.config, "POST", req.URL())
	if err != nil {
		return "", fmt.Errorf("failed to create executor: %w", err)
	}

	var stdout, stderr strings.Builder
	err = exec.StreamWithContext(ctx, remotecommand.StreamOptions{
		Stdout: &stdout,
		Stderr: &stderr,
		Tty:    false,
	})

	if err != nil {
		return "", fmt.Errorf("exec failed: %w, stderr: %s", err, stderr.String())
	}

	if stderr.Len() > 0 {
		return stdout.String(), fmt.Errorf("command stderr: %s", stderr.String())
	}

	return stdout.String(), nil
}
