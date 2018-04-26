package main

import (
	"flag"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/fagongzi/goetty"
	"github.com/fagongzi/goetty/protocol/redis"
)

var (
	readTimeout    = flag.Int("rt", 10, "The timeout for read in seconds")
	connectTimeout = flag.Int("ct", 10, "The timeout for connect to server")
	addr           = flag.String("addr", "127.0.0.1:6379", "The target address.")
	numKeys        = 10000 //number of keys
)

func main() {
	flag.Parse()
	if err := fill(); err != nil {
		fmt.Printf("%+v\n", err)
		os.Exit(1)
	}
}

func fill() (err error) {
	conn := goetty.NewConnector(*addr,
		goetty.WithClientConnectTimeout(time.Second*time.Duration(*connectTimeout)),
		goetty.WithClientDecoder(redis.NewRedisReplyDecoder()),
		goetty.WithClientEncoder(goetty.NewEmptyEncoder()))
	if _, err = conn.Connect(); err != nil {
		return
	}

	redis.InitRedisConn(conn)
	complete := &sync.WaitGroup{}
	complete.Add(2)

	go func() {
		for i := 0; i < numKeys; i++ {
			args := []string{
				fmt.Sprintf("book_%08d", i),
				"price", fmt.Sprintf("%v", 0.3+float32(i)),
				"count", fmt.Sprintf("%d", i),
				//"title", fmt.Sprintf("Exploring AI, volume %d", i),
				"author", "Mark Chen",
			}
			argInts := make([]interface{}, len(args))
			for i, v := range args {
				argInts[i] = v
			}
			if i%10000 == 0 {
				fmt.Println(args)
			}
			if err = redis.WriteCommand(conn, "hmset", argInts...); err != nil {
				return
			}
			if err = conn.Flush(); err != nil {
				return
			}
		}
		complete.Done()
	}()
	go func() {
		var rsp interface{}
		for i := 0; i < numKeys; i++ {
			if rsp, err = conn.ReadTimeout(time.Second * time.Duration(*readTimeout)); err != nil {
				return
			}
			if i%10000 == 0 {
				fmt.Printf("response %+v\n", rsp)
			}
		}
		complete.Done()
	}()
	complete.Wait()
	fmt.Println("done")
	return
}
