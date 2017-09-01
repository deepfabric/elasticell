package main

import (
	"flag"
	"fmt"
	"math"
	"math/rand"
	"os"
	"sync"
	"time"

	"github.com/fagongzi/goetty"
	"github.com/fagongzi/goetty/protocol/redis"
)

var (
	con            = flag.Int64("c", 0, "The concurrency.")
	num            = flag.Int64("n", 0, "The total number.")
	size           = flag.Int("s", 256, "The value size.")
	batch          = flag.Int("b", 64, "The command batch size.")
	readTimeout    = flag.Int("rt", 30, "The timeout for read in seconds")
	writeTimeout   = flag.Int("wt", 30, "The timeout for read in seconds")
	connectTimeout = flag.Int("ct", 10, "The timeout for connect to server")
	addr           = flag.String("addr", "127.0.0.1:6379", "The target address.")
)

func main() {
	flag.Parse()

	gCount := *con
	total := *num

	ready := make(chan struct{}, gCount)
	complate := &sync.WaitGroup{}
	wg := &sync.WaitGroup{}

	countPerG := total / gCount

	ans := newAnalysis()

	var index int64
	for index = 0; index < gCount; index++ {
		start := index * countPerG
		end := (index + 1) * countPerG
		if index == gCount-1 {
			end = total
		}

		wg.Add(1)
		complate.Add(1)
		go startG(end-start, wg, complate, ready, ans)
	}

	wg.Wait()

	ans.start()

	for index = 0; index < gCount; index++ {
		ready <- struct{}{}
	}

	complate.Wait()

	ans.end(total)
	ans.print()
}

func startG(total int64, wg, complate *sync.WaitGroup, ready chan struct{}, ans *analysis) {
	conn := goetty.NewConnector(&goetty.Conf{
		Addr: *addr,
		TimeoutConnectToServer: time.Second * time.Duration(*connectTimeout),
	}, redis.NewRedisReplyDecoder(), goetty.NewEmptyEncoder())
	_, err := conn.Connect()
	if err != nil {
		fmt.Printf("%+v\n", err)
		os.Exit(1)
	}

	redis.InitRedisConn(conn)

	wg.Done()
	<-ready

	q := newQueue(total)

	go func() {
		defer complate.Done()
		var received int64
		var min, max int64
		max = 0
		min = math.MaxInt64

		for {
			_, err := conn.ReadTimeout(time.Second * time.Duration(*readTimeout))
			if err != nil {
				fmt.Printf("%+v\n", err)
				os.Exit(1)
			}

			now := time.Now()
			cu := now.Sub(q.get(received)).Nanoseconds()
			if cu > max {
				max = cu
			}

			if cu < min {
				min = cu
			}

			received++

			if received == total {
				ans.set(min, max)
				return
			}
		}
	}()

	value := make([]byte, *size)
	var index int64

	start := time.Now()
	size := 0
	for ; index < total; index++ {
		key := fmt.Sprintf("%d", rand.Int63())

		redis.WriteCommand(conn, "set", key, value)
		size++

		if size == *batch {
			flush(conn, q, size)
			size = 0
		}
	}

	if size > 0 {
		flush(conn, q, size)
	}
	end := time.Now()
	fmt.Printf("%s sent %d reqs\n", end.Sub(start), total)
}

func flush(conn goetty.IOSession, q *queue, size int) {
	err := conn.WriteOutBuf()
	if err != nil {
		fmt.Printf("%+v\n", err)
		os.Exit(1)
	}

	n := time.Now()
	for index := 0; index < size; index++ {
		q.add(n)
	}
}

type analysis struct {
	sync.RWMutex
	startAt       time.Time
	max, min, avg int64
}

func newAnalysis() *analysis {
	return &analysis{
		max: 0,
		min: math.MaxInt64,
	}
}

func (a *analysis) set(min, max int64) {
	a.Lock()
	if max > a.max {
		a.max = max
	}

	if min < a.min {
		a.min = min
	}
	a.Unlock()
}

func (a *analysis) start() {
	a.startAt = time.Now()
}

func (a *analysis) end(count int64) {
	a.avg = time.Now().Sub(a.startAt).Nanoseconds() / count
}

func (a *analysis) print() {
	fmt.Printf("min: <%s>\n", time.Duration(a.min))
	fmt.Printf("max: <%s>\n", time.Duration(a.max))
	fmt.Printf("avg: <%s>\n", time.Duration(a.avg))
}

type queue struct {
	sync.RWMutex
	starts []time.Time
	index  int64
}

func newQueue(count int64) *queue {
	return &queue{
		starts: make([]time.Time, count),
	}
}

func (q *queue) add(start time.Time) {
	q.Lock()
	q.starts[q.index] = start
	q.index++
	q.Unlock()
}

func (q *queue) get(index int64) time.Time {
	q.Lock()
	value := q.starts[index]
	q.Unlock()
	return value
}
