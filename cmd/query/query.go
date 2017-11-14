package main

import (
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/fagongzi/goetty"
	"github.com/fagongzi/goetty/protocol/redis"
)

var (
	readTimeout    = flag.Int("rt", 10, "The timeout for read in seconds")
	connectTimeout = flag.Int("ct", 10, "The timeout for connect to server")
	addr           = flag.String("addr", "127.0.0.1:6379", "The target address.")
)

func main() {
	flag.Parse()
	arg := strings.Join(flag.Args(), " ")
	if err := query(arg); err != nil {
		fmt.Printf("%+v\n", err)
		os.Exit(1)
	}
}

func query(arg string) (err error) {
	var rsp interface{}
	conn := goetty.NewConnector(&goetty.Conf{
		Addr: *addr,
		TimeoutConnectToServer: time.Second * time.Duration(*connectTimeout),
	}, redis.NewRedisReplyDecoder(), goetty.NewEmptyEncoder())
	if _, err = conn.Connect(); err != nil {
		return
	}

	redis.InitRedisConn(conn)

	fmt.Printf("QUERY %+v\n", arg)
	if err = redis.WriteCommand(conn, "QUERY", arg); err != nil {
		return
	}
	if err = conn.WriteOutBuf(); err != nil {
		return
	}
	if rsp, err = conn.ReadTimeout(time.Second * time.Duration(*readTimeout)); err != nil {
		return
	}
	fmt.Printf("response %+v\n", rsp)
	return
}
