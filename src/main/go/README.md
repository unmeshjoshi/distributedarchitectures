* Download protoc from https://github.com/protocolbuffers/protobuf/releases
* Extract and add it to path
   
   e.g.  ```unzip -x ~/Downloads/protoc-3.11.4-linux-aarch_64.zip -d ./tmp/```
         ```export PATH=./tmp/bin:$PATH```
* Get gogoproto source and binary

     ```GO111MODULE=off go get -u -d github.com/gogo/protobuf```
* cd newraft/raftpb
* Run

    ```protoc  -I=. -I=$GOPATH/src -I=$GOPATH/src/github.com/gogo/protobuf --gogo_out=. ./*.proto```