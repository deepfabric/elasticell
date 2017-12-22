package main

import (
	"flag"
	"fmt"
	"math"
	"math/rand"
	_ "net/http/pprof"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/fagongzi/goetty"
	"github.com/fagongzi/goetty/protocol/redis"
)

var (
	con            = flag.Int64("c", 0, "The clients.")
	cn             = flag.Int64("cn", 64, "The concurrency per client.")
	readWeight     = flag.Int("r", 1, "read weight")
	writeWeight    = flag.Int("w", 1, "write weight")
	num            = flag.Int64("n", 0, "The total number.")
	size           = flag.Int("v", 256, "The value size.")
	readTimeout    = flag.Int("rt", 30, "The timeout for read in seconds")
	writeTimeout   = flag.Int("wt", 30, "The timeout for read in seconds")
	connectTimeout = flag.Int("ct", 10, "The timeout for connect to server")
	addrs          = flag.String("addrs", "127.0.0.1:6379", "The target address.")
	indexWeight    = flag.String("indexWeight", "0.0,0.0", "The weight of keys which match the index definion. Supported indices are orders and books.")
)

var (
	idxWeightsSum []int64
)

func main() {
	flag.Parse()

	if *readWeight == 0 && *writeWeight == 0 {
		fmt.Printf("read and write cann't be both zero")
		os.Exit(1)
	}

	idxWeights := make([]float64, 0)
	var f float64
	var err error
	for _, strF := range strings.Split(*indexWeight, ",") {
		if f, err = strconv.ParseFloat(strF, 64); err != nil {
			panic(fmt.Sprintf("failed to parse %s as float64", strF))
		}
		idxWeights = append(idxWeights, f)
	}
	idxWeightsSum = make([]int64, len(idxWeights))
	for i, f := range idxWeights {
		val := int64(f * float64(1<<63))
		if i != 0 {
			val += idxWeightsSum[i-1]
		}
		idxWeightsSum[i] = val
	}
	fmt.Printf("idxWeightsSum: %v\n", idxWeightsSum)

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

	value := make([]byte, *size)
	doRead := true
	c := *readWeight * 100
	start := time.Now()
	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	for index := int64(0); index < total; index += *cn {
		for k := int64(0); k < *cn; k++ {
			rnd := r.Int63()
			key := fmt.Sprintf("%d", rnd)

			for {
				if c > 0 {
					break
				} else {
					doRead = !doRead
					if doRead {
						c = *readWeight * 100
					} else {
						c = *writeWeight * 100
					}
				}
			}

			if doRead {
				redis.WriteCommand(conn, "get", key)
			} else {
				no := sort.Search(len(idxWeightsSum), func(i int) bool { return idxWeightsSum[i] > rnd })
				if no < 2 {
					rnd2 := r.Int63()
					argInts := getIdxArgs(no, rnd2)
					redis.WriteCommand(conn, "hmset", argInts...)
				} else {
					for i := 0; i < *size; i++ {
						value[i] = byte((index + k) % 0xff)
					}
					redis.WriteCommand(conn, "set", key, value)
				}

			}
			c--
		}

		err := conn.WriteOutBuf()
		if err != nil {
			fmt.Printf("%+v\n", err)
			os.Exit(1)
		}
		s := time.Now()
		ans.incrSent(*cn)

		for k := int64(0); k < *cn; k++ {
			_, err = conn.ReadTimeout(time.Second * time.Duration(*readTimeout))
			if err != nil {
				fmt.Printf("%+v\n", err)
				os.Exit(1)
			}

			ans.incrRecv(time.Now().Sub(s).Nanoseconds())
		}
	}

	end := time.Now()
	fmt.Printf("%s sent %d reqs\n", end.Sub(start), total)
}

func getIdxArgs(no int, i int64) (argInts []interface{}) {
	var args []string
	switch no {
	case 0:
		args = []string{
			fmt.Sprintf("book_%08d", i),
			"price", fmt.Sprintf("%v", 0.3+float32(i)),
			"count", fmt.Sprintf("%d", i),
			"author", "Mark Chen",
		}
	case 1:
		args = []string{
			fmt.Sprintf("order_%08d", i),
			"product", fmt.Sprintf("%v", i),
			"count", fmt.Sprintf("%d", i*2),
			"description", fmt.Sprintf("order_%08d", i),
		}
	default:
		panic(fmt.Sprintf("invalid no %v", no))
	}
	argInts = make([]interface{}, len(args))
	for i, v := range args {
		argInts[i] = v
	}
	return
}

type analysis struct {
	sync.RWMutex
	startAt                            time.Time
	recv, sent, prevRecv               int64
	avgLatency, maxLatency, minLatency int64
	totalCost, prevCost                int64
}

func newAnalysis() *analysis {
	return &analysis{}
}

func (a *analysis) setLatency(latency int64) {
	if a.minLatency == 0 || a.minLatency > latency {
		a.minLatency = latency
	}

	if a.maxLatency == 0 || a.maxLatency < latency {
		a.maxLatency = latency
	}

	a.totalCost += latency
	a.avgLatency = a.totalCost / a.recv
}

func (a *analysis) reset() {
	a.Lock()
	a.maxLatency = 0
	a.minLatency = 0
	a.Unlock()
}

func (a *analysis) start() {
	a.startAt = time.Now()
}

func (a *analysis) incrRecv(latency int64) {
	a.Lock()
	a.recv++
	a.setLatency(latency)
	a.Unlock()
}

func (a *analysis) incrSent(n int64) {
	a.Lock()
	a.sent += n
	a.Unlock()
}

func (a *analysis) print() {
	a.Lock()
	fmt.Printf("[%d, %d, %d](%d s), tps: <%d>/s, avg: %s, min: %s, max: %s \n",
		a.sent,
		a.recv,
		(a.sent - a.recv),
		int(time.Now().Sub(a.startAt).Seconds()),
		(a.recv - a.prevRecv),
		time.Duration(a.avgLatency),
		time.Duration(a.minLatency),
		time.Duration(a.maxLatency))
	a.prevRecv = a.recv
	a.prevCost = a.totalCost
	a.Unlock()
}
