package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/deepfabric/elasticell/pkg/pb/pdpb"
	"github.com/deepfabric/elasticell/pkg/pd"
	"golang.org/x/net/context"
	"google.golang.org/grpc/grpclog"
)

var (
	target = flag.String("target", "", "target pd")
)

func main() {
	grpclog.SetLogger(log.New(os.Stderr, "", log.LstdFlags))

	flag.Parse()

	c, err := pd.NewClient("test-cli", *target)
	if err != nil {
		fmt.Println(err)
		return
	}

	for {
		fmt.Println("start.........")
		resp, err := c.IsClusterBootstrapped(context.TODO(), new(pdpb.IsClusterBootstrapReq))
		if err != nil {
			fmt.Println(err)
			fmt.Println("end-error.........")
			time.Sleep(time.Second * 5)
			continue
		}

		fmt.Printf("%v\n", resp)
		fmt.Println("end.........")
		time.Sleep(time.Second * 5)
	}
}
