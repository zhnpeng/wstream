module github.com/zhnpeng/wstream

go 1.14

require (
	github.com/OneOfOne/xxhash v1.2.5
	github.com/coreos/bbolt v1.3.5 // indirect
	github.com/coreos/etcd v3.3.25+incompatible
	github.com/go-kit/kit v0.8.0
	github.com/golang/protobuf v1.4.2
	github.com/google/go-cmp v0.5.0
	github.com/philhofer/fwd v1.0.0 // indirect
	github.com/pkg/errors v0.8.1
	github.com/sirupsen/logrus v1.4.2
	github.com/spf13/cast v1.3.0
	github.com/tinylib/msgp v1.1.0
	go.etcd.io/etcd v0.0.0-20191023171146-3cf2f69b5738
	google.golang.org/grpc v1.27.0
	google.golang.org/protobuf v1.25.0
	gopkg.in/tomb.v1 v1.0.0-20141024135613-dd632973f1e7
)

replace github.com/coreos/bbolt v1.3.5 => go.etcd.io/bbolt v1.3.5
