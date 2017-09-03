package main

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import (
	"KVServer"
	"context"
	"crypto/tls"
	"fmt"

	"git.apache.org/thrift.git/lib/go/thrift"
)

func handleClient(client *KVServer.KVServerClient) (err error) {
	var kvobj = KVServer.KVObject{"foo", "bar"}
  fmt.Println("Sending obj:", kvobj)
	var ctx = context.Background()
	key, err := client.SetKey(ctx, &kvobj)
	if err != nil {
		fmt.Println("Set key:", key)
	}
	val, err := client.GetVal(ctx, "foo")
	if err != nil {
		fmt.Println("Got value:", val)
	}
	return err
}

func runClient(transportFactory thrift.TTransportFactory, protocolFactory thrift.TProtocolFactory, addr string, secure bool) error {
	var transport thrift.TTransport
	var err error
	if secure {
		cfg := new(tls.Config)
		cfg.InsecureSkipVerify = true
		transport, err = thrift.NewTSSLSocket(addr, cfg)
	} else {
		transport, err = thrift.NewTSocket(addr)
	}
	if err != nil {
		fmt.Println("Error opening socket:", err)
		return err
	}
	if transport == nil {
		return fmt.Errorf("Error opening socket, got nil transport. Is server available?")
	}
	transport, _ = transportFactory.GetTransport(transport)
	if transport == nil {
		return fmt.Errorf("Error from transportFactory.GetTransport(), got nil transport. Is server available?")
	}

	err = transport.Open()
	if err != nil {
		return err
	}
	defer transport.Close()

	return handleClient(KVServer.NewKVServerClientFactory(transport, protocolFactory))
}
