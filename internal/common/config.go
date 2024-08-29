package common

type Config struct {
	Cluster   Cluster
	Persister Persister
}

type Cluster struct {
	Nodes []Node
}

type Node struct {
	ID   int
	Host string
	Port string
}

type Persister struct {
	State    string
	Snapshot string
}
