package common

type Config struct {
	RPC RPC
}

type RPC struct {
	Cluster []string
}
