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
	"os"
	"sync"
	"time"
)

import (
	"dubbo.apache.org/dubbo-go/v3"
	"dubbo.apache.org/dubbo-go/v3/global"
	"dubbo.apache.org/dubbo-go/v3/graceful_shutdown"
	_ "dubbo.apache.org/dubbo-go/v3/imports"
	"dubbo.apache.org/dubbo-go/v3/protocol"
	"dubbo.apache.org/dubbo-go/v3/server"

	"github.com/dubbogo/gost/log/logger"
)

import (
	greet "github.com/apache/dubbo-go-samples/graceful_shutdown/proto"
)

type GreetProvider struct {
	fixedDelay           time.Duration
	ignoreContextCancel  bool
	shutdownOnFirstGreet bool
	rejectProbeWindow    time.Duration
	shutdownOnce         sync.Once
	shutdownConfig       *global.ShutdownConfig
}

func (p *GreetProvider) Greet(ctx context.Context, req *greet.GreetRequest) (*greet.GreetResponse, error) {
	start := time.Now()
	logger.Infof("Handling greet request, name=%s delay=%s", req.Name, p.fixedDelay)
	p.triggerShutdownOnFirstGreet()

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

func (p *GreetProvider) triggerShutdownOnFirstGreet() {
	if !p.shutdownOnFirstGreet {
		return
	}

	p.shutdownOnce.Do(func() {
		go func() {
			time.Sleep(200 * time.Millisecond)
			p.holdRejectProbeWindow()
			proc, err := os.FindProcess(os.Getpid())
			if err != nil {
				panic(fmt.Sprintf("failed to find current process for shutdown trigger: %v", err))
			}
			if err := proc.Signal(os.Interrupt); err != nil {
				panic(fmt.Sprintf("failed to trigger graceful shutdown: %v", err))
			}
			logger.Info("Triggered graceful shutdown after first greet request entered provider")
		}()
	})
}

func (p *GreetProvider) holdRejectProbeWindow() {
	if p.rejectProbeWindow <= 0 || p.shutdownConfig == nil {
		return
	}

	p.shutdownConfig.ConsumerActiveCount.Inc()
	logger.Infof("Holding graceful shutdown reject stage for integration probe, window=%s", p.rejectProbeWindow)
	go func() {
		time.Sleep(p.rejectProbeWindow)
		p.shutdownConfig.ConsumerActiveCount.Dec()
		logger.Info("Released graceful shutdown reject stage integration probe window")
	}()
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
	shutdownOnFirstGreet := flag.Bool("shutdown-on-first-greet", false, "trigger graceful shutdown after the first greet request enters the provider")
	rejectProbeWindow := flag.Duration("reject-probe-window", 0, "keep the framework reject stage open long enough for an integration probe")
	flag.Parse()

	shutdownConfig := graceful_shutdown.NewOptions(
		graceful_shutdown.WithTimeout(*timeout),
		graceful_shutdown.WithStepTimeout(*stepTimeout),
		graceful_shutdown.WithNotifyTimeout(*notifyTimeout),
		graceful_shutdown.WithConsumerUpdateWaitTime(*consumerUpdateWait),
		graceful_shutdown.WithOfflineRequestWindowTimeout(*offlineWindow),
	).Shutdown

	ins, err := dubbo.NewInstance(
		dubbo.WithShutdown(graceful_shutdown.SetShutdownConfig(shutdownConfig)),
		dubbo.WithProtocol(
			protocol.WithProtocol("tri"),
			protocol.WithPort(*port),
			protocol.WithID("tri"),
		),
	)
	if err != nil {
		panic(fmt.Sprintf("failed to create dubbo instance: %v", err))
	}
	logger.Infof("Graceful shutdown configured, timeout=%s step-timeout=%s notify-timeout=%s consumer-update-wait=%s offline-window=%s request-delay=%s ignore-context-cancel=%v shutdown-on-first-greet=%v reject-probe-window=%s",
		timeout.String(), stepTimeout.String(), notifyTimeout.String(), consumerUpdateWait.String(), offlineWindow.String(), requestDelay.String(), *ignoreContextCancel, *shutdownOnFirstGreet, rejectProbeWindow.String())

	srv, err := ins.NewServer(server.SetServerShutdown(shutdownConfig))
	if err != nil {
		panic(fmt.Sprintf("failed to create server: %v", err))
	}
	logger.Infof("Exposing Triple on port %d", *port)

	provider := &GreetProvider{
		fixedDelay:           *requestDelay,
		ignoreContextCancel:  *ignoreContextCancel,
		shutdownOnFirstGreet: *shutdownOnFirstGreet,
		rejectProbeWindow:    *rejectProbeWindow,
		shutdownConfig:       shutdownConfig,
	}
	if err := greet.RegisterGreetServiceHandler(srv, provider); err != nil {
		panic(fmt.Sprintf("failed to register greet service handler: %v", err))
	}

	logger.Info("Triple server started, press Ctrl+C to trigger graceful shutdown")

	if err := srv.Serve(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		panic(fmt.Sprintf("failed to serve: %v", err))
	}
}
