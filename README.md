# Introduction

This little utility scans a given topic and creates a map of MD5 checksums to detect the amount of duplicates present. It can optionally take in --max-messages or --start-time and --end-time
to cut down on the memory usage. The implementation depends on in-memory data structures and will likely not work on really large topics. For those, you may have to read the data into a big
search engine like Elasticsearch to do aggregations. 

# Build

```
> mvn clean install -DskipTests=true
``` 

# Usage

```
Usage: kafka-duplicate-detector [-hV] [-b=<bootstrapServers>]
                                [-c=<commandConfigFile>]
                                [-et=<approximateEndTime>] [-m=<maxMessages>]
                                [-st=<approximateStartTime>] -t=<topic>
A simply utility to detect duplicates in a topic
  -b, --bootstrap-server=<bootstrapServers>
                        List of Kafka Bootstrap servers
                          Default: localhost:9092
  -c, --command-config=<commandConfigFile>
                        Config file containing properties like security
                          credentials, etc
                          Default:
      -et, --end-time=<approximateEndTime>
                        Approximate end time (yyyy-MM-ddTHH:mm:ssZ)
                          Default: null
  -h, --help            Show this help message and exit.
  -m, --max-messages=<maxMessages>
                        Maximum number of messages to scan
                          Default: -1
      -st, --start-time=<approximateStartTime>
                        Approximate start time (yyyy-MM-ddTHH:mm:ssZ)
                          Default: null
  -t, --topic=<topic>   Topic name to scan
                          Default:
  -V, --version         Print version information and exit.
```


# Run
> ./bin/kafka-dupe-detect -b localhost:9092 -t testTopic -c <client config file>

If you have no security on your cluster you can just run it with the bootstrap server and topic. If it is a secure cluster, create a client.properties file like the example below and modify the properties based on your security mechanisms (this will support mTLS too if you provide all the ssl.* properties):

```
security.protocol=SASL_PLAINTEXT
sasl.mechanism=PLAIN
bootstrap.servers=kafka1:9091
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required \
  username="admin" \
  password="admin-secret";
```

# Warning

This tool computes everything in memory so it may run out of memory if the data is too much to scan and keep in memory. In the future we may update this to calculate by using a local database.


