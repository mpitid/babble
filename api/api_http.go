/*
 * Copyright 2017 Mosaic Networks Ltd
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

package api

import (
	"github.com/Sirupsen/logrus"
	"net"
	"net/http"
	"bufio"
	"fmt"
	"strconv"
	"encoding/hex"
)

/** A single-listener HTTP API. */
type HttpReplicaAPI struct {
	*ReplicaAPI

	// The other ends of the Replica channels.
	inputWriter chan <- Command
	outputReader <- chan Notification

	listener net.Listener

	// TODO: I don't like how this pollutes the struct.
	logger *logrus.Entry
}

func Http(address string, buffering int, logger *logrus.Entry) (*HttpReplicaAPI, error) {
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return nil, err
	}

	ic := make(chan Command, buffering)
	oc := make(chan Notification, buffering)
	api := HttpReplicaAPI{
		ReplicaAPI: New(ic, oc),
		inputWriter: ic,
		outputReader: oc,
		listener: listener,
		logger: logger,
	}
	return &api, nil
}

func (api *HttpReplicaAPI) Serve() error {
	api.logger.Debugf("starting HTTP API on %s", api.listener.Addr())
	http.HandleFunc("/", api.defaultHandler)
	if err := http.Serve(api.listener, nil); err != nil {
		api.logger.Errorf("failed to start HTTP server: %v", err)
		return err
	}
	return nil
}

/**
 * This defines a simple line-based text protocol for submitting transaction
 * batches and listening on commits. Each transaction batch is preceded by the
 * number of transactions  in decimal ASCII on the first line, followed by one
 * transaction per line, encoded as a hex string. This allows for easily
 * streaming requests or responses over a persistent connection (one for each
 * endpoint).
 */
func (api *HttpReplicaAPI) defaultHandler(w http.ResponseWriter, r *http.Request) {
	m, p := r.Method, r.URL.Path
	switch {

	case m == "POST" && p == "/api/submit":
		scanner := bufio.NewScanner(r.Body)
		for {
			transactions, err := ParseBatch(scanner, 10000)
			if err != nil {
				api.logger.WithField("error", err).Warn("error processing submit request")
				if len(transactions) > 0 {
					api.inputWriter <- SubmitTxCmd{Transactions: transactions}
				}
				return
			}
			if transactions == nil {
				// TODO: Perhaps write how many processed in the response.
				return
			}
			api.inputWriter <- SubmitTxCmd{Transactions: transactions}
		}

		// TODO: Perhaps honor maxSize in output too.
	case m == "GET" && p == "/api/events":
		w.Header().Set("X-Content-Type-Options", "nosniff")
		flusher, ok := w.(http.Flusher)
		if !ok {
			panic("expected http.ResponseWriter to be an http.Flusher")
		}
		// ensure the nosniff header is sent quickly
		flusher.Flush()
		// stop streaming if client closes the connection
		notifier := w.(http.CloseNotifier).CloseNotify()
		for {
			select {
			case msg := <-api.outputReader:
				event := msg.(CommitTxEv)
				fmt.Fprintf(w, "%d\n", len(event.Transactions))
				for _, data := range event.Transactions {
					fmt.Fprintf(w, "%x\n", data)
				}
				flusher.Flush()
			case closed := <-notifier:
				if closed {
					api.logger.Debug("connection closed by client")
					return
				}
			}
		}

	default:
		api.logger.Infof("invalid method/endpoint combination: %s %s", m, p)
		http.Error(w, "invalid method or endpoint", http.StatusBadRequest)
		return
	}
}


func ParseBatch(scanner *bufio.Scanner, maxSize int) ([][]byte, error) {
	var err error
	var data [][]byte
	i, pending := 0, 0
	for scanner.Scan() {
		line := scanner.Text()
		if pending == 0 {
			pending, err = strconv.Atoi(line)
			if err != nil {
				return nil, err
			}
			if pending < 1 {
				return nil, fmt.Errorf("invalid length %s (min length 1)", line)
			}
			if pending > maxSize {
				return nil, fmt.Errorf("invalid length %s (max length %d)", line, maxSize)
			}
			data = make([][]byte, pending)
			continue
		}
		tx, err := hex.DecodeString(line)
		if err != nil {
			return data[:i], fmt.Errorf("invalid hex-encoded transaction %s", line)
		}
		data[i] = tx
		pending -= 1
		i += 1
		if pending == 0 {
			return data, nil
		}
	}
	if pending > 0 {
		err = fmt.Errorf("stream closed after %d/%d items", i, pending)
	}
	return data[:i], err
}
