/*
* Copyright 2020, Offchain Labs, Inc.
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
 */

package utils

import (
	"context"
	"net/http"

	"github.com/ethereum/go-ethereum/rpc"
	"github.com/rs/zerolog/log"

	"github.com/gorilla/handlers"
	"github.com/gorilla/mux"

	"github.com/offchainlabs/arbitrum/packages/arb-util/configuration"
)

var logger = log.With().Caller().Stack().Str("component", "rpc").Logger()

func addRPC(r *mux.Router, handler http.Handler, path string) {
	r.Handle(path, handler).Methods("GET", "POST", "OPTIONS")
}

func addWS(r *mux.Router, server *rpc.Server, path string) {
	r.Handle(path, server.WebsocketHandler([]string{"*"}))
}

func LaunchRPC(ctx context.Context, handler http.Handler, endpoint configuration.Endpoint) error {
	r := mux.NewRouter()
	addRPC(r, handler, endpoint.Path)
	return launchServer(ctx, r, endpoint.Addr, endpoint.Port, "rpc")
}

func LaunchWS(ctx context.Context, server *rpc.Server, endpoint configuration.Endpoint) error {
	r := mux.NewRouter()
	addWS(r, server, endpoint.Path)
	return launchServer(ctx, r, endpoint.Addr, endpoint.Port, "websocket")
}

func LaunchRPCAndWS(ctx context.Context, server *rpc.Server, addr, port, rpcPath, wsPath string) error {
	r := mux.NewRouter()
	addRPC(r, server, rpcPath)
	addWS(r, server, wsPath)
	return launchServer(ctx, r, addr, port, "rpc and websocket")
}

func launchServer(ctx context.Context, handler http.Handler, addr string, port string, serverType string) error {
	headersOk := handlers.AllowedHeaders(
		[]string{"X-Requested-With", "Content-Type", "Authorization"},
	)
	originsOk := handlers.AllowedOrigins([]string{"*"})
	methodsOk := handlers.AllowedMethods(
		[]string{"GET", "HEAD", "POST", "PUT", "OPTIONS"},
	)
	h := handlers.CORS(headersOk, originsOk, methodsOk)(handler)

	logger.Info().Str("port", port).Msgf("Launching %s server over http", serverType)
	server := &http.Server{Addr: addr + ":" + port, Handler: h}

	errChan := make(chan error, 1)
	defer close(errChan)
	go func() {
		err := server.ListenAndServe()
		if err != nil && err.Error() == http.ErrServerClosed.Error() {
			errChan <- nil
		}
	}()

	select {
	case <-ctx.Done():
		return server.Shutdown(context.Background())
	case err := <-errChan:
		return err
	}
}
