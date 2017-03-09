package pdserver

import (
	"github.com/coreos/etcd/embed"
)

// Server the pd server
type Server struct {
	etcd *embed.Etcd
}

// NewServer create a pd server
func NewServer(cfg *Cfg) *Server {
	return nil
}

func (s *Server) genEmbedEtcdConfig() *embed.Config {
	return nil
}

// // StartEtcd starts an embed etcd server with an user handler.
// func (s *Server) StartEtcd(apiHandler http.Handler) error {
// 	etcdCfg := s.genEmbedEtcdConfig()
// 	etcdCfg.UserHandlers = map[string]http.Handler{
// 		pdRPCPrefix: s,
// 	}

// 	if apiHandler != nil {
// 		etcdCfg.UserHandlers[pdAPIPrefix] = apiHandler
// 	}

// 	log.Info("start embed etcd")

// 	etcd, err := embed.StartEtcd(etcdCfg)
// 	if err != nil {
// 		return errors.Trace(err)
// 	}

// 	// Check cluster ID
// 	urlmap, err := types.NewURLsMap(s.cfg.InitialCluster)
// 	if err != nil {
// 		return errors.Trace(err)
// 	}
// 	if err = etcdutil.CheckClusterID(etcd.Server.Cluster().ID(), urlmap); err != nil {
// 		return errors.Trace(err)
// 	}

// 	endpoints := []string{etcdCfg.LCUrls[0].String()}

// 	log.Infof("create etcd v3 client with endpoints %v", endpoints)
// 	client, err := clientv3.New(clientv3.Config{
// 		Endpoints:   endpoints,
// 		DialTimeout: etcdTimeout,
// 	})
// 	if err != nil {
// 		return errors.Trace(err)
// 	}

// 	if err = etcdutil.WaitEtcdStart(client, endpoints[0]); err != nil {
// 		// See https://github.com/coreos/etcd/issues/6067
// 		// Here may return "not capable" error because we don't start
// 		// all etcds in initial_cluster at same time, so here just log
// 		// an error.
// 		// Note that pd can not work correctly if we don't start all etcds.
// 		log.Errorf("etcd start failed, err %v", err)
// 	}

// 	s.etcd = etcd
// 	s.client = client
// 	s.id = uint64(etcd.Server.ID())

// 	// update advertise peer urls.
// 	etcdMembers, err := etcdutil.ListEtcdMembers(client)
// 	if err != nil {
// 		return errors.Trace(err)
// 	}
// 	for _, m := range etcdMembers.Members {
// 		if s.ID() == m.ID {
// 			etcdPeerURLs := strings.Join(m.PeerURLs, ",")
// 			if s.cfg.AdvertisePeerUrls != etcdPeerURLs {
// 				log.Infof("update advertise peer urls from %s to %s", s.cfg.AdvertisePeerUrls, etcdPeerURLs)
// 				s.cfg.AdvertisePeerUrls = etcdPeerURLs
// 			}
// 		}
// 	}

// 	if err = s.initClusterID(); err != nil {
// 		return errors.Trace(err)
// 	}
// 	log.Infof("init cluster id %v", s.clusterID)

// 	s.rootPath = path.Join(pdRootPath, strconv.FormatUint(s.clusterID, 10))
// 	s.idAlloc = &idAllocator{s: s}
// 	s.kv = newKV(s)
// 	s.cluster = newRaftCluster(s, s.clusterID)

// 	// Server has started.
// 	atomic.StoreInt64(&s.closed, 0)
// 	return nil
// }
