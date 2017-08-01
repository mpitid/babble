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
	"fmt"
)

/** ReplicaAPI interface. **/

type ReplicaAPI struct {

	// Replicas receive commands from clients
	input <-chan Command

	// Replicas send notifications to clients
	output chan<- Notification
}

func New(input <-chan Command, output chan<- Notification) *ReplicaAPI {
	return &ReplicaAPI{input: input, output: output}
}

func (api *ReplicaAPI) InputChannel() <- chan Command {
	return api.input
}

func (api *ReplicaAPI) OutputChannel() chan <- Notification {
	return api.output
}

func (api *ReplicaAPI) Close() {
	close(api.output)
}


// Represent commands and notifications as sum types.
// http://www.jerf.org/iri/post/2917
type Command interface {
	isCommand()
}

type Notification interface {
	isNotification()
}


type SubmitTxCmd struct {
	Transactions [][]byte
}
func (c SubmitTxCmd) isCommand() {}

// TODO: This should probably include the whole hashgraph event.
type CommitTxEv struct {
	Transactions [][]byte
}
func (n CommitTxEv) isNotification() {}


type CommandHandler func (Command) error

/**
 * Send notifications through without blocking by dropping events when the
 * channel is not ready.
 */
func (api *ReplicaAPI) NotifyAsync(msg Notification, logger *logrus.Entry) error {
	select {
	case api.output <- msg:
		logger.Debugf("successfully sent notification %v", msg)
	default:
		err := fmt.Errorf("dropped notification %v", msg)
		logger.Error(err)
		return err
	}
	return nil
}

// Fail-fast command loop.
func (api *ReplicaAPI) CommandLoop(handler CommandHandler) error {
	for cmd := range api.input {
		if err := handler(cmd); err != nil {
			return err
		}
	}
	return nil
}

