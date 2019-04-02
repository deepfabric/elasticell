package proxy

import (
	"encoding/json"
	"flag"
	"io/ioutil"

	"github.com/deepfabric/elasticell/pkg/log"
)

var (
	cfgFile = flag.String("cfg", "./cfg.json", "Configuration file of cell kv proxy, base on json formart.")
)

// Cfg returns cfg for proxy
type Cfg struct {
	Addr                string   `json:"addr"`
	AddrNotify          string   `json:"addrNotify"`
	WatcherHeartbeatSec int      `json:"watcherHeartbeatSec"`
	PDAddrs             []string `json:"pdAddrs"`
	MaxRetries          int      `json:"maxRetries"`
	RetryDuration       int64    `json:"retryDuration"`
	WorkerCount         uint64   `json:"workerCount"`
	SupportCMDs         []string `json:"supportCMDs"`
}

// GetCfg get cfg from command
func GetCfg() *Cfg {
	data, err := ioutil.ReadFile(*cfgFile)
	if err != nil {
		log.Fatalf("bootstrap: read configuration file failure, cfg=<%s>, errors:\n %+v",
			*cfgFile,
			err)
		return nil
	}

	cfg, err := unmarshal(data)
	if err != nil {
		log.Fatalf("bootstrap: parse configuration file failure, cfg=<%s>, errors:\n %+v",
			*cfgFile,
			err)
		return nil
	}

	return cfg
}

func unmarshal(data []byte) (*Cfg, error) {
	v := &Cfg{}

	err := json.Unmarshal(data, v)

	if nil != err {
		return nil, err
	}

	return v, nil
}
