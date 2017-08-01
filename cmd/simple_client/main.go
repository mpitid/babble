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

package main

import (
	"gopkg.in/urfave/cli.v1"
	"os"
	"github.com/Sirupsen/logrus"
	"time"
	"bufio"
	"fmt"
	"crypto/sha256"
	"bytes"
	"github.com/babbleio/babble/api"
	"sync"
	"net/http"
	"encoding/hex"
	"strconv"
)

var (
	log = logrus.New()
)

func main() {
	app := cli.NewApp()
	app.Name = "hgc"
	app.Usage = "submit transactions to a babble cluster"
	app.Version = "0.0.1"
	app.Flags = []cli.Flag {
		cli.StringFlag{
			Name: "name, n",
			Usage: "client name",
			Value: "hgc",
		},
		cli.StringFlag{
			Name: "connect, c",
			Usage: "address of babble node to connect to",
		},
		cli.StringFlag{
			Name: "file, f",
			Usage: "read messages from `FILE` one per line",
		},
		//cli.BoolFlag{
		//	Name: "interactive, i",
		//	Usage: "start in interactive mode",
		//},
		//cli.StringFlag{
		//	Name: "message, m",
		//	Usage: "send a single message",
		//},
		//cli.BoolFlag{
		//	Name: "times, t",
		//	Usage: "add timing information to messages",
		//},
	}
	app.Action = func (c *cli.Context) error {
		name := c.String("name")
		log.Level = logrus.DebugLevel
		log.WithField("name", name).Debug("client starting")

		submitCh := make(chan api.SubmitTxCmd)
		commitCh := make(chan Commit)
		messageCh := make(chan []string)
		file, err := os.Open(c.String("file"))
		if err != nil {
			return err
		}
		address := c.String("connect")
		var wg sync.WaitGroup
		wg.Add(4)
		go process(wg, messageCh, submitCh, commitCh)
		go sendTransactions(wg, submitCh, address)
		go readCommits(wg, commitCh, address)
		go readTransactions(wg, messageCh, *bufio.NewScanner(file), 2)
		log.Debugf("waiting for go routines")
		wg.Wait()
		return nil
	}
	app.Run(os.Args)
}

type Transaction struct {
	Id string
	Message string
	Timestamp time.Time
}

func transaction(message string, ts time.Time) Transaction {
	// TODO hash timestamp too
	id := sha256.Sum256([]byte(message))
	return Transaction{Id: fmt.Sprintf("%x", id), Message: message, Timestamp: ts}
}

func (t Transaction) bytes() []byte {
	ts, err := t.Timestamp.MarshalBinary()
	if err != nil {
		fail(err, "could not marshal timestamp")
	}
	buf := new(bytes.Buffer)
	buf.WriteByte(byte(len(ts)))
	buf.Write(ts)
	buf.WriteString(t.Message)
	return buf.Bytes()
}

/**
 * Read and submit transactions and print committed events.
 */
func process(wg sync.WaitGroup, messages <- chan []string, requests chan <- api.SubmitTxCmd, commits <- chan Commit) {
	defer wg.Done()
	msg_closed, com_closed:= false, false
	for {
		if msg_closed && com_closed {
			return
		}
		select {
		case batch, open := <- messages:
			if !open {
				if !msg_closed {
					log.Debug("message channel closed")
					close(requests)
				}
				msg_closed = true
			} else {
				log.WithField("size", len(batch)).Debugf("received tx batch")
				ts := time.Now()
				txs:= make([][]byte, len(batch))
				for i, msg := range batch {
					txs[i] = transaction(msg, ts).bytes()
					log.Debugf("appending tx %x", txs[i])
				}
				requests <- api.SubmitTxCmd{Transactions: txs[:]}
			}
		case ev, open := <- commits:
			if !open {
				log.Debug("com_closed")
				com_closed = true
			} else {
				log.Infof("transactions %s %d", ev.Timestamp, len(ev.Transactions))
			}
		}
	}
}

func readTransactions(wg sync.WaitGroup, messages chan <- []string, scanner bufio.Scanner, batchSize int) {
	defer wg.Done()
	buf := make([]string, batchSize)
	i := 0
	for scanner.Scan() {
		line := scanner.Text()
		buf[i] = line
		i += 1
		if i >= batchSize {
			log.WithField("size", i).Debugf("sending tx batch")
			messages <- buf[:i]
			i = 0
		}
	}
	if i > 0 {
		log.WithField("size", i).Debugf("sending tx batch")
		messages <- buf[:i]
	}
	close(messages)
}

func sendTransactions(wg sync.WaitGroup, requests <- chan api.SubmitTxCmd, address string) error {
	defer wg.Done()
	for {
		select {
		// TODO chunked request over persistent connection.
		case tx, open := <-requests:
			if !open {
				return nil
			}
			log.WithField("size", len(tx.Transactions)).Debug("processing request")
			body := prepareBody(tx)
			resp, err := http.Post(fmt.Sprintf("%s/api/submit", address), "text/plain", body)
			if err != nil {
				fail(err, "submitting HTTP request")
			}
			log.WithField("status", resp.Status).Debug("submitted tx")
		}
	}
}

func prepareBody(req api.SubmitTxCmd) *bytes.Reader {
	buf := bytes.NewBufferString(fmt.Sprintf("%d\n", len(req.Transactions)))
	for _, tx := range req.Transactions {
		buf.WriteString(fmt.Sprintf("%x\n", tx))
	}
	log.Infof("DATA: %s", buf.Bytes())
	return bytes.NewReader(buf.Bytes())
}

type Commit struct {
	Transactions []Transaction
	Timestamp time.Time
}

// TODO Some way to signal completion of events (e.g. count assuming no other clients)
func readCommits(wg sync.WaitGroup, commits chan <- Commit, address string) error {
	defer wg.Done()
	resp, err := http.Get(fmt.Sprintf("%s/api/events", address))
	if err != nil {
		fail(err, "connecting to %s/api/events", address)
	}
	if resp.StatusCode != 200 {
		fail(err, "unexpected response status %d", resp.Status)
	}
	ts := time.Now()
	scanner := bufio.NewScanner(resp.Body)
	i, pending := 0, 0
	var txs []Transaction
	// TODO reuse ParseBatch from http API
	for scanner.Scan() {
		line := scanner.Text()
		log.WithField("pending", pending).Debugf("scanned line `%s`", line)
		if pending == 0 {
			pending, err = strconv.Atoi(line)
			if err != nil {
				fail(err, "expected ascii integer got `%s`", line)
			}
			txs = make([]Transaction, pending)
			i = 0
			continue
		}
		raw, err := hex.DecodeString(line)
		if err != nil {
			fail(err, "failed to decode hex transaction `%s`", line)
		}
		// decode and write into commits channel
		n := int(raw[0])
		if err := ts.UnmarshalBinary(raw[1:n+1]); err != nil {
			fail(err, "failed to decode timestamp")
		}
		tx := transaction(string(raw[n+1:]), ts)
		log.Debugf("scanned transaction `%s`", string(tx.Message))
		txs[i] = tx
		i += 1
		pending -= 1

		// Commit any scanned transactions
		if pending == 0 && len(txs) > 0 {
			log.Debugf("committing %d transactions", len(txs))
			commits <- Commit{txs[:], time.Now()}
		}
	}
	return nil
}


func fail(err error, fmt string, args ...interface{}) {
	log.WithField("error", err).Panicf(fmt, args...)
}

