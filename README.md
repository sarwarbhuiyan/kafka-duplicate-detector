# Introduction

This little utility scans a given topic and creates a map of MD5 checksums to detect the amount of duplicates present.

# Build

```
> mvn clean install -DskipTests=true
``` 

# Usage

```

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


