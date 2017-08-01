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
package main

import (
	"fmt"
	"os"
	"os/user"
	"path/filepath"
	"runtime"
	"time"

	_ "net/http/pprof"

	"github.com/Sirupsen/logrus"
	"gopkg.in/urfave/cli.v1"

	"github.com/babbleio/babble/crypto"
	"github.com/babbleio/babble/net"
	"github.com/babbleio/babble/node"
	"github.com/babbleio/babble/proxy"
	aproxy "github.com/babbleio/babble/proxy/app"
	"github.com/babbleio/babble/service"
	"runtime/pprof"
	"os/signal"
	"github.com/babbleio/babble/api"
)

var (
	DataDirFlag = cli.StringFlag{
		Name:  "datadir",
		Usage: "Directory for the configuration",
		Value: defaultDataDir(),
	}
	NodeAddressFlag = cli.StringFlag{
		Name:  "node_addr",
		Usage: "IP:Port to bind Babble",
		Value: "127.0.0.1:1337",
	}
	NoClientFlag = cli.BoolFlag{
		Name:  "no_client",
		Usage: "Run Babble with dummy in-memory App client",
	}
	ProxyAddressFlag = cli.StringFlag{
		Name:  "proxy_addr",
		Usage: "IP:Port to bind Proxy Server",
		Value: "127.0.0.1:1338",
	}
	ClientAddressFlag = cli.StringFlag{
		Name:  "client_addr",
		Usage: "IP:Port of Client App",
		Value: "127.0.0.1:1339",
	}
	ServiceAddressFlag = cli.StringFlag{
		Name:  "service_addr",
		Usage: "IP:Port of HTTP Service",
		Value: "127.0.0.1:80",
	}
	ApiAddressFlag = cli.StringFlag{
		Name: "api",
		Usage: "HOST:PORT for HTTP API",
		Value: "127.0.0.01:5252",
	}
	LogLevelFlag = cli.StringFlag{
		Name:  "log_level",
		Usage: "debug, info, warn, error, fatal, panic",
		Value: "debug",
	}
	HeartbeatFlag = cli.IntFlag{
		Name:  "heartbeat",
		Usage: "Heartbeat timer milliseconds (time between gossips)",
		Value: 1000,
	}
	MaxPoolFlag = cli.IntFlag{
		Name:  "max_pool",
		Usage: "Max number of pooled connections",
		Value: 2,
	}
	TcpTimeoutFlag = cli.IntFlag{
		Name:  "tcp_timeout",
		Usage: "TCP timeout milliseconds",
		Value: 1000,
	}
	CacheSizeFlag = cli.IntFlag{
		Name:  "cache_size",
		Usage: "Number of items in LRU caches",
		Value: 500,
	}
	CpuProfileFlag = cli.StringFlag{
		Name: "cpu_profile",
		Usage: "file to write CPU profiling info",
		Value: "",
	}
	MemProfileFlag = cli.StringFlag{
		Name: "mem_profile",
		Usage: "file to write heap profiling info",
		Value: "",
	}
)

func main() {
	app := cli.NewApp()
	app.Name = "babble"
	app.Usage = "hashgraph consensus"
	app.Commands = []cli.Command{
		{
			Name:   "keygen",
			Usage:  "Dump new key pair",
			Action: keygen,
		},
		{
			Name:   "run",
			Usage:  "Run node",
			Action: run,
			Flags: []cli.Flag{
				DataDirFlag,
				NodeAddressFlag,
				NoClientFlag,
				ProxyAddressFlag,
				ClientAddressFlag,
				ServiceAddressFlag,
				ApiAddressFlag,
				LogLevelFlag,
				HeartbeatFlag,
				MaxPoolFlag,
				TcpTimeoutFlag,
				CacheSizeFlag,
				CpuProfileFlag,
				MemProfileFlag,
			},
		},
	}
	app.Run(os.Args)
}

func keygen(c *cli.Context) error {
	pemDump, err := crypto.GeneratePemKey()
	if err != nil {
		fmt.Println("Error generating PemDump")
		os.Exit(2)
	}

	fmt.Println("PublicKey:")
	fmt.Println(pemDump.PublicKey)
	fmt.Println("PrivateKey:")
	fmt.Println(pemDump.PrivateKey)

	return nil
}

func run(c *cli.Context) error {
	logger := logrus.New()
	logger.Level = logLevel(c.String(LogLevelFlag.Name))

	datadir := c.String(DataDirFlag.Name)
	addr := c.String(NodeAddressFlag.Name)
	noclient := c.Bool(NoClientFlag.Name)
	proxyAddress := c.String(ProxyAddressFlag.Name)
	clientAddress := c.String(ClientAddressFlag.Name)
	serviceAddress := c.String(ServiceAddressFlag.Name)
	apiAddress := c.String(ApiAddressFlag.Name)
	heartbeat := c.Int(HeartbeatFlag.Name)
	maxPool := c.Int(MaxPoolFlag.Name)
	tcpTimeout := c.Int(TcpTimeoutFlag.Name)
	cacheSize := c.Int(CacheSizeFlag.Name)
	cpuProfile := c.String(CpuProfileFlag.Name)
	memProfile := c.String(MemProfileFlag.Name)
	logger.WithFields(logrus.Fields{
		"datadir":      datadir,
		"node_addr":    addr,
		"no_client":    noclient,
		"proxy_addr":   proxyAddress,
		"client_addr":  clientAddress,
		"service_addr": serviceAddress,
		"api_address":  apiAddress,
		"heartbeat":    heartbeat,
		"max_pool":     maxPool,
		"tcp_timeout":  tcpTimeout,
		"cache_size":   cacheSize,
		"cpu_profile":  cpuProfile,
		"mem_profile":  memProfile,
	}).Debug("RUN")

	if cpuProfile != "" {
		f, err := os.Create(cpuProfile)
		if err != nil {
			return cli.NewExitError(fmt.Sprintf("failed to open CPU profile file %v", cpuProfile), 2)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
		c := make(chan os.Signal, 1)
		signal.Notify(c, os.Interrupt)
		go func(){
			for range c {
				// sig is a ^C, handle it
				pprof.StopCPUProfile()
				writeMemProfile(memProfile)
				os.Exit(127)
			}
		}()
	}

	conf := node.NewConfig(time.Duration(heartbeat)*time.Millisecond,
		time.Duration(tcpTimeout)*time.Millisecond,
		cacheSize, logger)

	// Create the PEM key
	pemKey := crypto.NewPemKey(datadir)

	// Try a read
	key, err := pemKey.ReadKey()
	if err != nil {
		return err
	}

	// Create the peer store
	store := net.NewJSONPeers(datadir)

	// Try a read
	peers, err := store.Peers()
	if err != nil {
		return err
	}

	trans, err := net.NewTCPTransport(addr,
		nil, maxPool, conf.TCPTimeout, logger)
	if err != nil {
		return err
	}

	var prox proxy.AppProxy
	if noclient {
		prox = aproxy.NewInmemAppProxy(logger)
	} else {
		prox = aproxy.NewSocketAppProxy(clientAddress, proxyAddress,
			conf.TCPTimeout, logger)
	}

	nodeApi, err := api.Http(apiAddress, 2, logger.WithField("listening", apiAddress))
	if err != nil {
		return err
	}
	go nodeApi.Serve()
	node := node.NewNode(conf, key, peers, trans, prox, nodeApi.ReplicaAPI)
	node.Init()

	serviceServer := service.NewService(serviceAddress, &node, logger)
	go serviceServer.Serve()

	node.Run(true)

	return writeMemProfile(memProfile)
}

func writeMemProfile(filename string) error {
	if filename == "" {
		return nil
	}
	f, err := os.Create(filename)
	if err != nil {
		return cli.NewExitError(fmt.Sprintf("failed to open heap profile file %v: %v", filename, err), 2)
	}
	defer f.Close()
	runtime.GC() // get up-to-date statistics
	if err := pprof.WriteHeapProfile(f); err != nil {
		return cli.NewExitError(fmt.Sprintf("failed to write heap profile data: %v", err), 2)
	}
	return nil
}

func defaultDataDir() string {
	// Try to place the data folder in the user's home dir
	home := homeDir()
	if home != "" {
		if runtime.GOOS == "darwin" {
			return filepath.Join(home, "Library", "BABBLE")
		} else if runtime.GOOS == "windows" {
			return filepath.Join(home, "AppData", "Roaming", "BABBLE")
		} else {
			return filepath.Join(home, ".babble")
		}
	}
	// As we cannot guess a stable location, return empty and handle later
	return ""
}

func homeDir() string {
	if home := os.Getenv("HOME"); home != "" {
		return home
	}
	if usr, err := user.Current(); err == nil {
		return usr.HomeDir
	}
	return ""
}

func logLevel(l string) logrus.Level {
	switch l {
	case "debug":
		return logrus.DebugLevel
	case "info":
		return logrus.InfoLevel
	case "warn":
		return logrus.WarnLevel
	case "error":
		return logrus.ErrorLevel
	case "fatal":
		return logrus.FatalLevel
	case "panic":
		return logrus.PanicLevel
	default:
		return logrus.DebugLevel
	}
}
