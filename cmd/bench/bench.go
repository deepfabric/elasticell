package main

import (
	"flag"
	"fmt"
	"math"
	"math/rand"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/fagongzi/goetty"
	"github.com/fagongzi/goetty/protocol/redis"
)

var (
	con            = flag.Int64("c", 0, "The clients.")
	cn             = flag.Int64("cn", 100, "The concurrency per client.")
	readWeight     = flag.Int("r", 1, "read weight")
	writeWeight    = flag.Int("w", 1, "write weight")
	num            = flag.Int64("n", 0, "The total number.")
	size           = flag.Int("v", 256, "The value size.")
	batch          = flag.Int("b", 64, "The command batch size.")
	readTimeout    = flag.Int("rt", 30, "The timeout for read in seconds")
	writeTimeout   = flag.Int("wt", 30, "The timeout for read in seconds")
	connectTimeout = flag.Int("ct", 10, "The timeout for connect to server")
	addrs          = flag.String("addrs", "127.0.0.1:6379", "The target address.")
)

func main() {
	flag.Parse()

	if *readWeight == 0 && *writeWeight == 0 {
		fmt.Printf("read and write cann't be both zero")
		os.Exit(1)
	}

	gCount := *con
	total := *num
	if total < 0 {
		total = 0
	}

	ready := make(chan struct{}, gCount)
	complate := &sync.WaitGroup{}
	wg := &sync.WaitGroup{}

	countPerG := total / gCount

	ans := newAnalysis()

	var index int64
	proxies := strings.Split(*addrs, ",")
	for index = 0; index < gCount; index++ {
		start := index * countPerG
		end := (index + 1) * countPerG
		if index == gCount-1 {
			end = total
		}

		wg.Add(1)
		complate.Add(1)
		proxy := proxies[index%int64(len(proxies))]
		go startG(end-start, wg, complate, ready, ans, proxy)
	}

	wg.Wait()

	ans.start()

	for index = 0; index < gCount; index++ {
		ready <- struct{}{}
	}

	go func() {
		for {
			ans.print()
			time.Sleep(time.Second * 1)
		}
	}()

	complate.Wait()
	ans.print()
}

func startG(total int64, wg, complate *sync.WaitGroup, ready chan struct{}, ans *analysis, proxy string) {
	var onlyRead, onlyWrite bool

	if *readWeight == 0 {
		onlyWrite = true
	} else if *writeWeight == 0 {
		onlyRead = true
	}

	if total <= 0 {
		total = math.MaxInt64
	}

	conn := goetty.NewConnector(&goetty.Conf{
		Addr: proxy,
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

	q := newQueue()

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
			cu := now.Sub(*q.pop()).Nanoseconds()
			if cu > max {
				max = cu
			}

			if cu < min {
				min = cu
			}

			received++
			ans.incrRecv()

			if received == total {
				ans.set(min, max)
				return
			} else if received%10000 == 0 {
				ans.set(min, max)
			}
		}
	}()

	value := make([]byte, *size)
	for i := 0; i < *size; i++ {
		value[i] = '0'
	}

	var index, lastIndex int64

	start := time.Now()
	st := start
	b := 0
	ctl := 0
	doWrite := true
	rc := *readWeight * 100
	wc := *writeWeight * 100

	for ; index < total; index++ {
		key := fmt.Sprintf("%d", rand.Int63())

		if onlyRead {
			redis.WriteCommand(conn, "get", key)
		} else if onlyWrite {
			redis.WriteCommand(conn, "set", key, value)
		} else {
			limit := rc

			if doWrite {
				limit = wc
				redis.WriteCommand(conn, "get", key)
			} else {
				redis.WriteCommand(conn, "set", key, value)
			}

			if ctl == limit {
				ctl = 0
				doWrite = !doWrite
			}

			ctl++
		}

		b++
		if b == *batch {
			flush(conn, q, b)
			ans.incrSent(int64(b))
			b = 0
		}

		if index-lastIndex >= *cn {
			n := time.Now()
			d := n.Sub(st)
			st = n
			time.Sleep(time.Second - d)
			lastIndex = index
		}
	}

	if b > 0 {
		ans.incrSent(int64(b))
		flush(conn, q, b)
	}
	end := time.Now()
	fmt.Printf("%s sent %d reqs\n", end.Sub(start), total)
}

func flush(conn goetty.IOSession, q *queue, b int) {
	err := conn.WriteOutBuf()
	if err != nil {
		fmt.Printf("%+v\n", err)
		os.Exit(1)
	}

	n := time.Now()
	for index := 0; index < b; index++ {
		q.add(&n)
	}
}

type analysis struct {
	sync.RWMutex
	startAt                             time.Time
	max, min, avg, recv, sent, prevRecv int64
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

func (a *analysis) incrRecv() {
	atomic.AddInt64(&a.recv, 1)
}

func (a *analysis) incrSent(n int64) {
	atomic.AddInt64(&a.sent, n)
}

func (a *analysis) calc(total int64) {
	if total != 0 {
		a.avg = time.Second.Nanoseconds() / total
	}
}

func (a *analysis) print() {
	prev := atomic.LoadInt64(&a.prevRecv)
	recv := atomic.LoadInt64(&a.recv)
	sent := atomic.LoadInt64(&a.sent)
	atomic.StoreInt64(&a.prevRecv, recv)

	a.calc(recv - prev)

	fmt.Printf("[%d, %d, %d], tps: <%d>/s, avg: %s \n",
		sent,
		recv,
		(sent - recv),
		(recv - prev),
		time.Duration(a.avg))
}

type queue struct {
	sync.RWMutex
	starts []*time.Time
	index  int64
}

func newQueue() *queue {
	return &queue{}
}

func (q *queue) add(start *time.Time) {
	q.Lock()
	q.starts = append(q.starts, start)
	q.index++
	q.Unlock()
}

func (q *queue) pop() *time.Time {
	q.Lock()
	value := q.starts[0]
	q.starts[0] = nil
	q.starts = q.starts[1:]
	q.Unlock()
	return value
}
