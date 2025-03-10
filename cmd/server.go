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
	"crypto/x509"
	"encoding/asn1"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"

	"github.com/guojianyu/swarm-tunnel/pkg/server"

	"k8s.io/klog"
)

func main() {
	klog.InitFlags(nil)
	flag.Set("v", "4")
	flag.Parse()
	tunnelServer := server.NewTunnelServer(":8080")

	tunnelServer.SetRegisterClientHandle(func(clientID string, r *http.Request) error {
		fmt.Printf("dynamic Handle[%s]\n", clientID)
		if r.TLS == nil {
			return nil
		}
		if len(r.TLS.PeerCertificates) < 1 {
			return nil
		}
		tlsConn := r.TLS.PeerCertificates[0]
		return extractUUIDFromCert(tlsConn)

	})

	tunnelServer.SetRegisterdClientCallback(func(clientID string) {
		fmt.Printf("dynamic callback：client[%s] register!\n", clientID)
	})
	tunnelServer.SetUnregisterdClientCallback(func(clientID string) {
		fmt.Printf("dynamic callback：client[%s] unregister!\n", clientID)
	})
	tunnelServer.SetRegisterdSessionCallback(func(sessionID string) {
		fmt.Printf("dynamic callback：session[%s] register!\n", sessionID)
	})
	tunnelServer.SetUnregisterdSessionCallback(func(sessionID string) {
		fmt.Printf("dynamic callback：session[%s] unregister!\n", sessionID)
	})

	{
		ca := "D:/workspace/go/src/test/multi-cluster/cert/ca_cert.pem"
		cert := "D:/workspace/go/src/test/multi-cluster/cert/server_cert.pem"
		key := "D:/workspace/go/src/test/multi-cluster/cert/server_key.pem"
		tunnelServer.WithTLS(ca, cert, key)

	}

	tunnelServer.Run()
}

var uuidOID = asn1.ObjectIdentifier{1, 2, 840, 113549, 1, 9, 8} // 自定义 UUID 的 OID
type ExtraInformation struct {
	ClientUUID  string `json:"clientuuid"`
	JwtToken    string `json:"jwttoken"`
	Alias       string `json:"alias"`
	Location    string `json:"location"`
	Description string `json:"description"`
}

func extractUUIDFromCert(cert *x509.Certificate) error {
	for _, ext := range cert.Extensions {
		if ext.Id.Equal(uuidOID) { // 检查自定义 UUID OID
			var extra ExtraInformation
			err := json.Unmarshal(ext.Value, &extra)
			if err != nil {
				log.Printf("Failed to parse UUID from certificate: %v", err)
				return err
			}
			fmt.Printf("ExtraInformation:%v", extra)
			if extra.ClientUUID == "guojy" {
				return fmt.Errorf("I do not like guojy!")
			}
			return nil
		}
	}
	return nil
}
