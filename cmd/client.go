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
package main

import (
	"flag"

	"github.com/guojianyu/swarm-tunnel/pkg/client"

	"k8s.io/klog"
)

func main() {
	klog.InitFlags(nil)
	flag.Set("v", "3")
	flag.Parse()
	addr := "ws://localhost:8080/register"
	clientId := "guojy1"
	tunnelAgent := client.NewTunnelAgent(addr, clientId)
	// {
	// 	addr := "wss://localhost:8080/register"
	// 	ca := "D:/workspace/go/src/test/multi-cluster/cert/ca_cert.pem"
	// 	cert := "D:/workspace/go/src/test/multi-cluster/cert/client_cert.pem"
	// 	key := "D:/workspace/go/src/test/multi-cluster/cert/client_key.pem"
	// 	tunnelAgent.WithTLS(ca, cert, key)

	// }
	tunnelAgent.Run()
}
