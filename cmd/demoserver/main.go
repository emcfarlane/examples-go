// Copyright 2022-2023 The Connect Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package main implements connectrpc.com's demo server, which exposes the
// Eliza service over Connect, gRPC, and gRPC-Web.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"connectrpc.com/connect/v2"
	"connectrpc.com/connect/v2/connecthttp"
	"connectrpc.com/grpchealth/v2"
	"connectrpc.com/grpcreflect/v2"
	"github.com/rs/cors"
	"github.com/spf13/pflag"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"

	"connect-examples-go/internal/eliza"
	elizav1 "connect-examples-go/internal/gen/connectrpc/eliza/v1"
	"connect-examples-go/internal/gen/connectrpc/eliza/v1/elizav1connect"
)

const (
	// Responses smaller than this are sent uncompressed.
	compressMinBytes = 1024
	// Generous timeouts for the long-lived streams this demo serves.
	streamTimeout = 5 * time.Minute
	// Maximum size of the request headers the server will parse.
	maxHeaderBytes = 8 * 1024 // 8KiB
	// How long browsers may cache the CORS preflight response. Firefox caps
	// this at 24h, and modern Chrome caps it at 2h.
	corsMaxAge = 2 * time.Hour
)

// ElizaServer implements the Eliza service.
type ElizaServer struct {
	streamDelay time.Duration // sleep between streaming response messages
}

// NewElizaServer returns a new Eliza implementation which sleeps for the
// provided duration between streaming responses.
func NewElizaServer(streamDelay time.Duration) *ElizaServer {
	return &ElizaServer{streamDelay: streamDelay}
}

func (e *ElizaServer) Say(
	_ context.Context,
	req *elizav1.SayRequest,
) (*elizav1.SayResponse, error) {
	reply, _ := eliza.Reply(req.GetSentence()) // ignore end-of-conversation detection

	return &elizav1.SayResponse{
		Sentence: reply,
	}, nil
}

func (e *ElizaServer) Converse(
	ctx context.Context,
	stream elizav1connect.ElizaServiceConverseServerStream,
) error {
	for {
		err := ctx.Err()
		if err != nil {
			return fmt.Errorf("context canceled: %w", err)
		}

		request, err := stream.Receive()
		if err != nil && errors.Is(err, io.EOF) {
			return nil
		} else if err != nil {
			return fmt.Errorf("receive request: %w", err)
		}

		reply, endSession := eliza.Reply(request.GetSentence())

		err = stream.Send(&elizav1.ConverseResponse{Sentence: reply})
		if err != nil {
			return fmt.Errorf("send response: %w", err)
		}

		if endSession {
			return nil
		}
	}
}

func (e *ElizaServer) Introduce(
	ctx context.Context,
	req *elizav1.IntroduceRequest,
	stream elizav1connect.ElizaServiceIntroduceServerStream,
) error {
	name := req.GetName()
	if name == "" {
		name = "Anonymous User"
	}

	intros := eliza.GetIntroResponses(name)

	var ticker *time.Ticker
	if e.streamDelay > 0 {
		ticker = time.NewTicker(e.streamDelay)
		defer ticker.Stop()
	}

	for _, resp := range intros {
		if ticker != nil {
			select {
			case <-ctx.Done():
				return fmt.Errorf("context canceled: %w", ctx.Err())
			case <-ticker.C:
			}
		}

		err := stream.Send(&elizav1.IntroduceResponse{Sentence: resp})
		if err != nil {
			return fmt.Errorf("send response: %w", err)
		}
	}

	return nil
}

func newCORS() *cors.Cors {
	// To let web developers play with the demo service from browsers, we need a
	// very permissive CORS setup.
	return cors.New(cors.Options{ //exhaustruct:ignore
		AllowedMethods: []string{
			http.MethodHead,
			http.MethodGet,
			http.MethodPost,
			http.MethodPut,
			http.MethodPatch,
			http.MethodDelete,
		},
		AllowOriginFunc: func(_ /* origin */ string) bool {
			// Allow all origins, which effectively disables CORS.
			return true
		},
		AllowedHeaders: []string{"*"},
		ExposedHeaders: []string{
			// Content-Type is in the default safelist.
			"Accept",
			"Accept-Encoding",
			"Accept-Post",
			"Connect-Accept-Encoding",
			"Connect-Content-Encoding",
			"Content-Encoding",
			"Grpc-Accept-Encoding",
			"Grpc-Encoding",
			"Grpc-Message",
			"Grpc-Status",
			"Grpc-Status-Details-Bin",
		},
		// Caching CORS information reduces the number of preflight requests. Any
		// changes to ExposedHeaders won't take effect until the cached data
		// expires.
		MaxAge: int(corsMaxAge / time.Second),
	})
}

// newHTTPServer builds the demo service's HTTP server, serving Eliza over
// Connect, gRPC, and gRPC-Web alongside health and reflection endpoints.
func newHTTPServer(streamDelay time.Duration) *http.Server {
	mux := http.NewServeMux()
	mux.Handle(
		"/",
		http.RedirectHandler("https://connectrpc.com/demo", http.StatusFound),
	)

	compress1KB := connecthttp.WithCompressMinBytes(compressMinBytes)
	server := connect.NewServer()
	elizav1connect.RegisterElizaServiceHandler(server, NewElizaServer(streamDelay))

	grpchealth.Register(server, grpchealth.NewStaticChecker(elizav1connect.ElizaServiceName))
	grpcreflect.Register(server)
	connecthttp.Mount(mux, server, compress1KB)

	addr := "localhost:8080"
	if port := os.Getenv("PORT"); port != "" {
		addr = ":" + port
	}

	return &http.Server{ //exhaustruct:ignore
		Addr: addr,
		Handler: h2c.NewHandler(
			newCORS().Handler(mux),
			&http2.Server{}, //exhaustruct:ignore
		),
		ReadHeaderTimeout: time.Second,
		ReadTimeout:       streamTimeout,
		WriteTimeout:      streamTimeout,
		MaxHeaderBytes:    maxHeaderBytes,
	}
}

func main() {
	helpArg := pflag.BoolP("help", "h", false, "")
	streamDelayArg := pflag.DurationP(
		"server-stream-delay",
		"d",
		0,
		"The duration to delay sending responses on the server stream.",
	)

	pflag.Parse()

	if *helpArg {
		pflag.PrintDefaults()

		return
	}

	if *streamDelayArg < 0 {
		log.Printf("Server stream delay cannot be negative.")

		return
	}

	srv := newHTTPServer(*streamDelayArg)

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)

	go func() {
		err := srv.ListenAndServe()
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("HTTP listen and serve: %v", err)
		}
	}()

	<-signals

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	err := srv.Shutdown(ctx)
	if err != nil {
		log.Fatalf("HTTP shutdown: %v", err) //nolint:gocritic
	}
}
