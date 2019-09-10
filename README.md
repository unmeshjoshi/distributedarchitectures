A project template with embeddedkafka, spark streaming, kafka streaming
This also includes vagrant setup with hdfs, hbase, zookeeper and spark.
* vagrant up
* open three tabs
* vagrant ssh namenode
* vagrant ssh datanode1
* vagrant ssh datanode2
* In namenode
* start-dfs.sh
* start-yarn.sh
* zkServer.sh start
* hbase start
* To test run following
```spark-submit --deploy-mode client --class org.apache.spark.examples.SparkPi /home/vagrant/spark/examples/jars/spark-examples_2.11-2.3.2.jar 10```
* Starting postgres container
```docker run --name postgres1 -e POSTGRES_PASSWORD=password -d postgres```

