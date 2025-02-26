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
	"fmt"

	"github.com/guojianyu/swarm-tunnel/pkg/server"

	"k8s.io/klog"
)

func main() {
	klog.InitFlags(nil)
	flag.Set("v", "3")
	flag.Parse()
	tunnelServer := server.NewTunnelServer(":8080")
	tunnelServer.SetRegisterClientCallback(func(clientID string) {
		fmt.Printf("dynamic callback：client[%s] register!\n", clientID)
	})
	tunnelServer.SetUnregisterClientCallback(func(clientID string) {
		fmt.Printf("dynamic callback：client[%s] unregister!\n", clientID)
	})
	tunnelServer.SetRegisterSessionCallback(func(sessionID string) {
		fmt.Printf("dynamic callback：session[%s] register!\n", sessionID)
	})
	tunnelServer.SetUnregisterSessionCallback(func(sessionID string) {
		fmt.Printf("dynamic callback：session[%s] unregister!\n", sessionID)
	})

	// {
	// 	ca := "D:/workspace/go/src/test/multi-cluster/cert/ca_cert.pem"
	// 	cert := "D:/workspace/go/src/test/multi-cluster/cert/server_cert.pem"
	// 	key := "D:/workspace/go/src/test/multi-cluster/cert/server_key.pem"
	// 	tunnelServer.WithTLS(ca, cert, key)

	// }

	tunnelServer.Run()
}
