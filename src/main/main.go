package main

import (
	"carrier/cluster/cluster"
	_ "carrier/command/set"
	"carrier/logger"
	"flag"
	"github.com/go-ini/ini"
	"log"
	"net/http"
	_ "net/http/pprof"
	"runtime"
	"time"
	"util"
	"socket"
	"net"
	carrierConn "carrier/conn"
)

var (
	confpath      = flag.String("conf", "conf/carrier.ini", "配置文件路径")
	cfg           *ini.File
	carrierLogger *util.Logger
)

func init() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	flag.Parse()

	var (
		err error
	)
	if cfg, err = ini.Load(*confpath); err != nil {
		panic(err)
	}

	if cfg.Section("PPROF").Key("UsePprof").MustBool(false) {
		go func() {
			log.Println(http.ListenAndServe(cfg.Section("PPROF").Key("PprofAddr").MustString(":6063"), nil))
		}()
	}
}

func ServeForClient(listenAddr string) {
	var (
		serverSocket *socket.TServerSocket
		serverConn   net.Conn
		err          error
	)
	if serverSocket, err = socket.NewTServerSocket(listenAddr); err != nil {
		panic(err)
	}
	if err = serverSocket.Listen(); err != nil {
		panic(err)
	}
	defer serverSocket.Close()

	// AcceptLoop
	for {
		if serverConn, err = serverSocket.Accept(); err != nil {
			logger.Notice("Socket To Client Accept: ", err)
			continue
		}
		go func(connClient net.Conn) {
			defer func() {
				logger.Info("客户端断开连接: ", connClient.RemoteAddr().String())
				connClient.Close()
			}()
			logger.Info("客户端新连接: ", connClient.RemoteAddr().String())
			ClientLogic(connClient)
		}(serverConn)
	}
}


func ClientLogic(connClient net.Conn) {
	// TODO 对连接做处理(IP限制什么鬼的)
	// TODO AUTH

	var (
		connCarrier carrierConn.Conn
	)
	connCarrier = carrierConn.NewConn(connClient)
	<- connCarrier.IsClosed()
}


func main() {
	carrierLogger = util.NewLogger(cfg.Section("LOG").Key("Path").MustString(util.HomeDir()+"/logs/carrier.log"), util.FilenameSuffixInDay)
	logger.SetLogger(carrierLogger)

	var (
		clusterServerList []string
		err               error
		addr              *ini.Key
	)
	for _, addr = range cfg.Section("CLUSTER.HOST").Keys() {
		clusterServerList = append(clusterServerList, addr.Value())
	}

	// 初始化 cluster
	if err = cluster.InitClusterParameter(
		clusterServerList,
		time.Duration(cfg.Section("CLUSTER").Key("RefreshInterval").MustInt(300))*time.Second,
		cfg.Section("CLUSTER").Key("MaxIdle").MustInt(50),
		time.Duration(cfg.Section("CLUSTER").Key("TestOnBorrowTimeout").MustInt(150))*time.Second,
		time.Duration(cfg.Section("CLUSTER").Key("ConnectTimeout").MustInt(3))*time.Second,
		time.Duration(cfg.Section("CLUSTER").Key("ReadTimeout").MustInt(3))*time.Second,
		time.Duration(cfg.Section("CLUSTER").Key("WriteTimeout").MustInt(3))*time.Second,
	); err != nil {
		panic(err)
	}

	// 服务端口 client
	ServeForClient(cfg.Section("").Key("ClientAddr").MustString(":6679"))
}
