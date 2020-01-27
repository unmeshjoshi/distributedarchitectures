Download protoc-3.11.2-linux-x86_64.zip from https://github.com/protocolbuffers/protobuf/releases
extract to say ~/softwares/protoc and add to PATH

export PATH=~/softwares/protoc/bin:$PATH
go get github.com/gogo/protobuf/gogoproto

export PATH=$GOPATH/bin:$PATH

cd /consensus/newraft/raftpb
protoc  -I=. -I=$GOPATH/src -I=$GOPATH/src/github.com/gogo/protobuf --gogo_out=. ./*.proto