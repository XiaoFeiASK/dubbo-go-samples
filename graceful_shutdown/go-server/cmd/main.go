/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"time"
)

import (
	"dubbo.apache.org/dubbo-go/v3"
	"dubbo.apache.org/dubbo-go/v3/graceful_shutdown"
	_ "dubbo.apache.org/dubbo-go/v3/imports"
	"dubbo.apache.org/dubbo-go/v3/protocol"

	"github.com/dubbogo/gost/log/logger"
)

import (
	greet "github.com/apache/dubbo-go-samples/graceful_shutdown/proto"
)

type GreetProvider struct {
	fixedDelay          time.Duration
	ignoreContextCancel bool
}

func (p *GreetProvider) Greet(ctx context.Context, req *greet.GreetRequest) (*greet.GreetResponse, error) {
	start := time.Now()
	logger.Infof("Handling greet request, name=%s delay=%s", req.Name, p.fixedDelay)

	if p.fixedDelay > 0 {
		if p.ignoreContextCancel {
			time.Sleep(p.fixedDelay)
		} else {
			timer := time.NewTimer(p.fixedDelay)
			defer timer.Stop()

			select {
			case <-timer.C:
			case <-ctx.Done():
				logger.Warnf("Greet request canceled before completion, name=%s err=%v", req.Name, ctx.Err())
				return nil, ctx.Err()
			}
		}
	}

	resp := &greet.GreetResponse{
		Greeting: fmt.Sprintf("%s response after %s", req.Name, time.Since(start).Truncate(time.Millisecond)),
	}
	logger.Infof("Greet request finished, name=%s cost=%s", req.Name, time.Since(start).Truncate(time.Millisecond))
	return resp, nil
}

func main() {
	port := flag.Int("port", 20000, "triple listen port")
	timeout := flag.Duration("timeout", 60*time.Second, "overall graceful shutdown timeout budget")
	stepTimeout := flag.Duration("step-timeout", 3*time.Second, "timeout for waiting provider and consumer in-flight requests")
	notifyTimeout := flag.Duration("notify-timeout", 5*time.Second, "timeout for notifying consumers during graceful shutdown")
	consumerUpdateWait := flag.Duration("consumer-update-wait", 3*time.Second, "time to wait for consumers to observe instance changes")
	offlineWindow := flag.Duration("offline-window", 3*time.Second, "time window for observing late requests after offline")
	requestDelay := flag.Duration("delay", 0, "artificial delay added to each greet request")
	ignoreContextCancel := flag.Bool("ignore-context-cancel", false, "continue the artificial delay even if the request context is canceled")
	rejectRequest := flag.Bool("reject-request", false, "start with framework request rejection enabled for integration validation")
	flag.Parse()

	shutdownOpts := []graceful_shutdown.Option{
		graceful_shutdown.WithTimeout(*timeout),
		graceful_shutdown.WithStepTimeout(*stepTimeout),
		graceful_shutdown.WithNotifyTimeout(*notifyTimeout),
		graceful_shutdown.WithConsumerUpdateWaitTime(*consumerUpdateWait),
		graceful_shutdown.WithOfflineRequestWindowTimeout(*offlineWindow),
	}
	if *rejectRequest {
		shutdownOpts = append(shutdownOpts, graceful_shutdown.WithRejectRequest())
	}

	ins, err := dubbo.NewInstance(
		dubbo.WithShutdown(shutdownOpts...),
		dubbo.WithProtocol(
			protocol.WithProtocol("tri"),
			protocol.WithPort(*port),
			protocol.WithID("tri"),
		),
	)
	if err != nil {
		panic(fmt.Sprintf("failed to create dubbo instance: %v", err))
	}
	logger.Infof("Graceful shutdown configured, timeout=%s step-timeout=%s notify-timeout=%s consumer-update-wait=%s offline-window=%s request-delay=%s ignore-context-cancel=%v reject-request=%v",
		timeout.String(), stepTimeout.String(), notifyTimeout.String(), consumerUpdateWait.String(), offlineWindow.String(), requestDelay.String(), *ignoreContextCancel, *rejectRequest)

	srv, err := ins.NewServer()
	if err != nil {
		panic(fmt.Sprintf("failed to create server: %v", err))
	}
	logger.Infof("Exposing Triple on port %d", *port)

	provider := &GreetProvider{
		fixedDelay:          *requestDelay,
		ignoreContextCancel: *ignoreContextCancel,
	}
	if err := greet.RegisterGreetServiceHandler(srv, provider); err != nil {
		panic(fmt.Sprintf("failed to register greet service handler: %v", err))
	}

	logger.Info("Triple server started, press Ctrl+C to trigger graceful shutdown")

	if err := srv.Serve(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		panic(fmt.Sprintf("failed to serve: %v", err))
	}
}
