#参考 https://grpc.io/docs/protoc-installation/
# https://grpc.io/docs/languages/go/quickstart/

rm -frv ./calm
rm -frv ../client/go_calmclient/proto 
protoc --go_out=. -I src src/core.proto
protoc --go_out=. -I src src/model.proto
protoc --go_out=. --go-grpc_out=. -I src src/calmserver.proto
protoc --go_out=. --go-grpc_out=. -I src src/calmkeeper.proto
protoc --go_out=. --go-grpc_out=. -I src src/calmagent.proto
protoc --go_out=. --go-grpc_out=. -I src src/cluster.proto
mv ./calm/client/* ../client/go_calmclient
rm -frv ./calm 