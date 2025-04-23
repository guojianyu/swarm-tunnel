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
package client

import (
	"bufio"
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"sync"
	"time"

	tunnelPkg "github.com/guojianyu/swarm-tunnel/pkg"
	"k8s.io/klog"

	"github.com/gorilla/websocket"
)

type Client tunnelPkg.Client

type Session tunnelPkg.Session

func (s *Session) setStatus(status tunnelPkg.SessionStatus) {
	s.Mu.Lock()
	defer s.Mu.Unlock()
	if s.Status == tunnelPkg.SessionClosed || s.Status == tunnelPkg.SessionDownstreamClosed || s.Status == tunnelPkg.SessionUpstreamClosed {
		return
	}
	s.Status = status
}

func (s *Session) setAnnotaion(annotation string) {
	s.Mu.Lock()
	defer s.Mu.Unlock()
	if s.Status == tunnelPkg.SessionClosed {
		return
	}
	s.Annotation = annotation
}

type Hub struct {
	// Register requests from the clients.
	// Unregister requests from clients.
	SessionRegister   chan *Session
	SessionUnregister chan *Session
	c                 *Client
	Sessions          sync.Map
}

type TunnelAgent struct {
	hub       *Hub
	webserver *tunnelPkg.WebServer
	ClientID  string
}

func (ta *TunnelAgent) WithTLS(cafile, certFile, keyFile string) {
	ta.webserver.EnableTLS = true
	ta.webserver.CaFile = cafile
	ta.webserver.CertFile = certFile
	ta.webserver.KeyFile = keyFile
}
func NewHub() *Hub {
	return &Hub{
		SessionRegister:   make(chan *Session),
		SessionUnregister: make(chan *Session),
		Sessions:          sync.Map{},
	}
}

func NewTunnelAgent(addr, clientID string) *TunnelAgent {
	return &TunnelAgent{
		hub: NewHub(),
		webserver: &tunnelPkg.WebServer{
			Addr: addr,
		},
		ClientID: clientID,
	}
}

type pump struct {
	in  *io.PipeWriter
	out *io.PipeReader
}

func (session *Session) close() {
	session.UpAgent.(*pump).in.Close()
	session.UpAgent.(*pump).out.Close()
	session.DownAgent.(*pump).in.Close()
	session.DownAgent.(*pump).out.Close()
	session.Cancel()
}

func (session *Session) upAgent() *pump {
	return session.UpAgent.(*pump)
}

func (session *Session) downAgent() *pump {
	return session.DownAgent.(*pump)
}

func (client *Client) WsLockWriteMessage(messageType int, data []byte) error {
	client.Mu.Lock()
	defer client.Mu.Unlock()
	client.Socket.SetWriteDeadline(time.Now().Add(tunnelPkg.WriteWait))
	return client.Socket.WriteMessage(messageType, data)
}

func (ta *TunnelAgent) close() {
	ta.hub.c.Socket.Close()
}

func (ta *TunnelAgent) newWebSocketClient() error {
	u, err := url.Parse(ta.webserver.Addr)
	if err != nil {
		return fmt.Errorf("invalid URL: %v", err)
	}
	query := u.Query()
	query.Set("clientid", ta.ClientID)
	u.RawQuery = query.Encode()
	dialer := websocket.Dialer{}
	if ta.webserver.EnableTLS {
		caCert, err := ioutil.ReadFile(ta.webserver.CaFile)
		if err != nil {
			return fmt.Errorf("failed to read CA cert: %v", err)
		}
		caCertPool := x509.NewCertPool()
		caCertPool.AppendCertsFromPEM(caCert)

		// load client certificate and key
		clientCert, err := tls.LoadX509KeyPair(ta.webserver.CertFile, ta.webserver.KeyFile)
		if err != nil {
			return fmt.Errorf("failed to load client cert/key: %v", err)
		}

		// configure TLS
		tlsConfig := &tls.Config{
			RootCAs:      caCertPool,                    // CA certificate
			Certificates: []tls.Certificate{clientCert}, // client certificate
		}
		dialer.TLSClientConfig = tlsConfig
	}
	conn, _, err := dialer.Dial(u.String(), nil)
	if err != nil {
		return fmt.Errorf("failed to connect: %v", err)
	}
	ta.hub.c = &Client{Socket: conn, Send: make(chan *tunnelPkg.TunnelMessage), Mu: sync.Mutex{}}
	return nil
}

// deserialize request
func wsToLocalRequest(websocketReq *tunnelPkg.HttpRequest) (*http.Request, error) {
	r := bufio.NewReader(bytes.NewReader([]byte(websocketReq.HttpRequest)))
	localRequest, err := http.ReadRequest(r)
	if err != nil {
		klog.V(4).Infof("deserialize request error: %v", err)
		return localRequest, err
	}

	localRequest.RequestURI = ""
	u, err := url.Parse(websocketReq.URL)
	if err != nil {
		klog.V(4).Infof("parse url error: %v", err)
		return nil, err
	}
	localRequest.URL = u
	localRequest.URL.Scheme = u.Scheme
	localRequest.URL.Host = u.Host

	klog.V(4).Infof("localRequest.URL: %v", localRequest.URL)
	klog.V(4).Infof("localRequest.URL.Host: %v", localRequest.URL.Host)
	return localRequest, nil
}

func localResponseToWebSocketResponse(resp *http.Response) (*tunnelPkg.HttpResponse, error) {
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		klog.V(4).Infof("read local response error ", err)
		return nil, err
	}
	resp.Body.Close()
	wsRes := &tunnelPkg.HttpResponse{Status: resp.Status, StatusCode: resp.StatusCode,
		Proto: resp.Proto, Header: resp.Header, Body: body, ContentType: resp.Header.Get("Content-Type")}
	return wsRes, nil
}
