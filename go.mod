module go.nyu.edu/protosim

go 1.18

replace go.etcd.io/etcd/v3 => ../etcd

require (
	github.com/gogo/protobuf v1.3.2
	go.etcd.io/etcd/v3 v3.0.0-00010101000000-000000000000
)

require (
	google.golang.org/genproto/googleapis/rpc v0.0.0-20230711160842-782d3b101e98 // indirect
	google.golang.org/protobuf v1.31.0 // indirect
)

require (
	github.com/akamensky/argparse v1.4.0
	github.com/golang/protobuf v1.5.3 // indirect
	golang.org/x/net v0.17.0 // indirect
	golang.org/x/sys v0.13.0 // indirect
	golang.org/x/text v0.13.0 // indirect
	google.golang.org/grpc v1.58.3 // indirect
)
