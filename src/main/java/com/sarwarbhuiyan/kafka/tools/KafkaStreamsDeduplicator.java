package com.sarwarbhuiyan.kafka.tools;

import java.io.FileInputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowStore;

public class KafkaStreamsDeduplicator {
  

  private static final String OUTPUT_TOPIC_REPLICATION_FACTOR = "output.topic.replication.factor";
  private static final String OUTPUT_TOPIC_PARTITIONS = "output.topic.partitions";
  private static final String INPUT_TOPIC_REPLICATION_FACTOR = "input.topic.replication.factor";
  private static final String INPUT_TOPIC_PARTITIONS = "input.topic.partitions";
  private static final String EARLIEST = "earliest";
  private static final String BOOTSTRAP_SERVERS = "bootstrap.servers";
  private static final String APPLICATION_ID = "application.id";
  private static final String DEFAULT_DEDUP_STORE_STORE_NAME = "dedup-store";
  private static final String DEDUP_STORE_NAME = "dedup.store.name";
  private static final String RETENTION_MINUTES = "retention.minutes";
  private static final String OUTPUT_TOPIC_NAME = "output.topic.name";
  private static final String INPUT_TOPIC_NAME = "input.topic.name";

  public Topology buildTopology(Properties envProp) {
    final StreamsBuilder builder = new StreamsBuilder();
    
    String inputTopic = envProp.getProperty(INPUT_TOPIC_NAME);
    String outputTopic = envProp.getProperty(OUTPUT_TOPIC_NAME);
    int minutes = envProp.containsKey(RETENTION_MINUTES) ? Integer.parseInt(envProp.getProperty(RETENTION_MINUTES)) : 10;
    final Duration retentionPeriod = Duration.ofMinutes(minutes);
    final String stateStoreName = envProp.containsKey(DEDUP_STORE_NAME) ? envProp.getProperty(DEDUP_STORE_NAME): DEFAULT_DEDUP_STORE_STORE_NAME;

    final StoreBuilder<WindowStore<String, Long>> dedupStoreBuilder = Stores.windowStoreBuilder(
            Stores.persistentWindowStore(stateStoreName,
                                         retentionPeriod,
                                         retentionPeriod,
                                         false
            ),
            Serdes.String(),
            Serdes.Long());

    builder.addStateStore(dedupStoreBuilder);


    final KStream<byte[], byte[]> stream = builder.stream(inputTopic);
    final KStream<byte[], byte[]> deduplicated = stream.process(
        // In this example, we assume that the record value as-is represents a unique event ID by
        // which we can perform de-duplication.  If your records are different, adapt the extractor
        // function as needed.
        () -> new DeduplicationTransformer<byte[], byte[], byte[], byte[]>(stateStoreName,retentionPeriod.toMillis(), (key, value) ->  DigestUtils.md5Hex((byte[])value)),
        stateStoreName);
    deduplicated.to(outputTopic);
    
    return builder.build();
  }
  
  public Properties getStreamProps(Properties envProp) {
    final Properties streamsConfiguration = new Properties();
    //merge from properties file
    streamsConfiguration.putAll(envProp);
    
    
    streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, envProp.get(APPLICATION_ID));
    streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, envProp.get(BOOTSTRAP_SERVERS));
    streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, EARLIEST);
    streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.ByteArray().getClass());
    streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.ByteArray().getClass());
    //streamsConfiguration.put(StreamsConfig.MAX_TASK_IDLE_MS_CONFIG, 20000);
    
    
    // These two settings are only required in this contrived example so that the 
    // streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 0);
    // streamsConfiguration.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
    return streamsConfiguration;
  }

  public void createTopics(final Properties envProps) {
    final Map<String, Object> config = new HashMap<>();
    config.put(BOOTSTRAP_SERVERS, envProps.getProperty(BOOTSTRAP_SERVERS));
    for(Entry<Object, Object> e: envProps.entrySet()) {
      config.put((String)e.getKey(), e.getValue());
    }

    try (final AdminClient client = AdminClient.create(config)) {

      final List<NewTopic> topics = new ArrayList<>();

      topics.add(new NewTopic(envProps.getProperty(INPUT_TOPIC_NAME),
          Integer.parseInt(envProps.getProperty(INPUT_TOPIC_PARTITIONS)),
          Short.parseShort(envProps.getProperty(INPUT_TOPIC_REPLICATION_FACTOR))));

      Map<String,String> configs = new HashMap<>();
//      configs.put(TopicConfig.RETENTION_MS_CONFIG, "5184000000");
      
      topics.add(new NewTopic(envProps.getProperty(OUTPUT_TOPIC_NAME),
          Integer.parseInt(envProps.getProperty(OUTPUT_TOPIC_PARTITIONS)),
          Short.parseShort(envProps.getProperty(OUTPUT_TOPIC_REPLICATION_FACTOR))).configs(configs));

      client.createTopics(topics).all().get();
    } catch(TopicExistsException e) {
      System.out.println("Topic already exists.");
    } catch (InterruptedException e1) {
      // TODO Auto-generated catch block
      e1.printStackTrace();
    } catch (ExecutionException e1) {
      // TODO Auto-generated catch block
      e1.printStackTrace();
    }
  }

  public Properties loadEnvProperties(String fileName) throws IOException {
    final Properties envProps = new Properties();
    final FileInputStream input = new FileInputStream(fileName);
    envProps.load(input);
    input.close();
    

    
    // These two settings are only required in this contrived example so that the 
    // streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 0);
    // streamsConfiguration.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
    return envProps;
  }

  public static void main(String[] args) throws Exception {

    if (args.length < 1) {
      throw new IllegalArgumentException(
          "This program takes one argument: the path to an environment configuration file.");
    }

    final KafkaStreamsDeduplicator instance = new KafkaStreamsDeduplicator();
    final Properties envProps = instance.loadEnvProperties(args[0]);

    // Setup the input topic, table topic, and output topic
    instance.createTopics(envProps);

    // Normally these can be run in separate applications but for the purposes of the demo, we
    // just run both streams instances in the same application

    try (final KafkaStreams streams = new KafkaStreams(instance.buildTopology(envProps), instance.getStreamProps(envProps))) {
     final CountDownLatch startLatch = new CountDownLatch(1);
     // Attach shutdown handler to catch Control-C.
     Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
         @Override
         public void run() {
             //streams.cleanUp();
             streams.close(Duration.ofSeconds(5));
             startLatch.countDown();
         }
     });
     streams.setUncaughtExceptionHandler(e -> StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.REPLACE_THREAD);
     // Start the topology.
     streams.start();
     System.out.println(streams.toString());

     try {
       startLatch.await();
     } catch (final InterruptedException e) {
       Thread.currentThread().interrupt();
       System.exit(1);
     }
    }
    System.exit(0);

  }
}
