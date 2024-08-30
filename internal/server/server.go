package server

import "raftkv/pkg/rpc"

func StartServer(i int) {
	// a fresh set of ClientEnds.
	ends := make([]*rpc.ClientEnd, cfg.n)
	for j := 0; j < cfg.n; j++ {
		ends[j] = cfg.net.MakeEnd(cfg.endnames[i][j])
		cfg.net.Connect(cfg.endnames[i][j], j)
	}

	cfg.kvservers[i] = StartKVServer(ends, i, persister.MakePersister(), cfg.maxraftstate)

	kvsvc := rpc.MakeService(cfg.kvservers[i])
	rfsvc := rpc.MakeService(cfg.kvservers[i].rf)
	srv := rpc.MakeServer()
	srv.AddService(kvsvc)
	srv.AddService(rfsvc)
	cfg.net.AddServer(i, srv)
}
