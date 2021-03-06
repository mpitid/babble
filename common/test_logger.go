/*
Copyright 2017 Mosaic Networks Ltd

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
package common

import (
	"testing"

	"github.com/Sirupsen/logrus"
)

// This can be used as the destination for a logger and it'll
// map them into calls to testing.T.Log, so that you only see
// the logging for failed tests.
type testLoggerAdapter struct {
	t      *testing.T
	prefix string
}

func (a *testLoggerAdapter) Write(d []byte) (int, error) {
	if d[len(d)-1] == '\n' {
		d = d[:len(d)-1]
	}
	if a.prefix != "" {
		l := a.prefix + ": " + string(d)
		a.t.Log(l)
		return len(l), nil
	}
	a.t.Log(string(d))
	return len(d), nil
}

func NewTestLogger(t *testing.T) *logrus.Logger {
	logger := logrus.New()
	logger.Out = &testLoggerAdapter{t: t}
	logger.Level = logrus.DebugLevel
	return logger
}

type benchmarkLoggerAdapter struct {
	b      *testing.B
	prefix string
}

func (b *benchmarkLoggerAdapter) Write(d []byte) (int, error) {
	if d[len(d)-1] == '\n' {
		d = d[:len(d)-1]
	}
	if b.prefix != "" {
		l := b.prefix + ": " + string(d)
		b.b.Log(l)
		return len(l), nil
	}

	b.b.Log(string(d))
	return len(d), nil
}

func NewBenchmarkLogger(b *testing.B) *logrus.Logger {
	logger := logrus.New()
	logger.Out = &benchmarkLoggerAdapter{b: b}
	logger.Level = logrus.DebugLevel
	return logger
}
