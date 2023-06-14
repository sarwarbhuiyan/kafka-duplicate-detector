package com.sarwarbhuiyan.kafka.tools;

import java.io.FileInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Help.Visibility;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.Spec;

@Command(name = "kafka-duplicate-detector",
description = "A simply utility to detect duplicates in a topic", version = "0.0.1",
mixinStandardHelpOptions = true)
public class KafkaDuplicateDetector implements Runnable {

  @Option(required = false, names = {"-b", "--bootstrap-server"},
      description = "List of Kafka Bootstrap servers", defaultValue = "localhost:9092",
      showDefaultValue = Visibility.ALWAYS)
  private String bootstrapServers = "localhost:9092";

  @Option(required = false, names = {"-c", "--command-config"},
      description = "Config file containing properties like security credentials, etc",
      defaultValue = "", showDefaultValue = Visibility.ALWAYS)
  private String commandConfigFile = "";

  @Option(required = true, names = {"-t", "--topic"},
      description = "Topic name to scan", showDefaultValue = Visibility.ALWAYS)
  private String topic = "";
  
  @Spec
  CommandSpec spec;
  
  
  private final AtomicBoolean start = new AtomicBoolean(false);
  
  
  
  public void run() {
    long startTime = System.currentTimeMillis();
    MessageDigest messageDigest;
    try {
      Properties properties = new Properties(); 
      properties.put("bootstrap.servers", this.bootstrapServers);
      properties.put("key.deserializer", ByteArrayDeserializer.class);
      properties.put("value.deserializer", ByteArrayDeserializer.class);
      properties.put("auto.offset.reset", "earliest");
      
      
      
      if (commandConfigFile.length() > 0) {
        Properties fileProps = new Properties();
        try {
          fileProps.load(new FileInputStream(commandConfigFile));
        } catch (Throwable e) {
          throw new CommandLine.ParameterException(spec.commandLine(),
              "Could not find or read specified file");
        }
        fileProps.forEach((key, value) -> properties.put(key, value)); // merge two props
      }
      
      
      Consumer<byte[], byte[]> consumer = new KafkaConsumer<>(properties);
      List<TopicPartition> partitions = consumer.partitionsFor(topic).stream()
          .map(p -> new TopicPartition(topic, p.partition())).collect(Collectors.toList());
      
      consumer.assign(partitions);
      
      Map<TopicPartition, Long> beginningOffsets = consumer.beginningOffsets(partitions);
      Map<Integer, Long> endOffsets = consumer.endOffsets(partitions).entrySet().stream().collect(Collectors.toMap(
          entry -> ((TopicPartition)entry.getKey()).partition(), 
          entry -> entry.getValue()
          ));
      
      Map<String, Integer> messageCount = new HashMap<>();
      Map<Integer, Boolean> partitionDoneStatus = endOffsets.entrySet().stream().collect(
          Collectors.toMap(
                entry -> entry.getKey(),
                entry -> Boolean.FALSE
              ));
      
      int emptyPollCount = 0;
      int totalMessageCount = 0;
      while(true) {
      
        ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(100));
        if(records.isEmpty()) {
          Thread.sleep(1000);
          emptyPollCount++;
        }
        if(emptyPollCount > 10)
          break;
          
        org.apache.kafka.common.serialization.IntegerDeserializer intDeserializer = new org.apache.kafka.common.serialization.IntegerDeserializer();
        
        Map<Integer, Long> pollLatestOffsetsMap = new HashMap<>();
        for(ConsumerRecord<byte[], byte[]> record: records) {
          totalMessageCount++;
          //messageDigest.update(record.value());
          //byte[] md5Hash = messageDigest.digest();
          String md5Hash = DigestUtils.md5Hex(record.value());
          
          

          
          if(messageCount.containsKey(md5Hash)) {
            //System.out.println("Found duplicate "+record.value()+" at partition "+record.partition()+" offset "+record.offset());
            messageCount.put(md5Hash, messageCount.get(md5Hash)+1);
          }
          else {
            messageCount.put(md5Hash, 1);
          }
          pollLatestOffsetsMap.put(record.partition(), record.offset());
          //System.out.println("scanned "+record.partition()+" "+record.offset());
        }
        
        for(Map.Entry<Integer,Long> e: pollLatestOffsetsMap.entrySet()) {
          //System.out.println("e.getValue() "+e.getValue()+" endOffset="+endOffsets.get(e.getKey()));
          if(e.getValue() >= endOffsets.get(e.getKey())-1) {
            
            partitionDoneStatus.put(e.getKey(), Boolean.TRUE);
          }
        }
        
        if(partitionDoneStatus.values().stream().allMatch(x -> x)) {
          System.out.println("Finishing reading till end offsets");
          break;
        }
        
        
      }
      
      long endTime = System.currentTimeMillis();
      System.out.println("Total run time: "+((endTime-startTime)/(1000))+" seconds.");
      // print stats
      Map<String, Integer> dupesCount = messageCount.entrySet().stream().filter(e -> e.getValue() > 1).collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue()));
      System.out.println("Total Messages: "+totalMessageCount);
      System.out.println("Dupes Found: "+dupesCount.size());
      System.out.println("Percentage: "+((100.0*dupesCount.size())/totalMessageCount));

      final AtomicInteger i = new AtomicInteger(2);

      while(true) {

        Map<String, Integer> frequencyCount = messageCount.entrySet().stream().filter(e -> e.getValue() == i.get()).collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue()));
        
        if(frequencyCount.size()>0)
          System.out.println("Frequency (n="+i+")  : "+frequencyCount.size());
        else
          break;
        i.incrementAndGet();
      }
      
      System.out.println();
      
      //messageCount.forEach((k, v) -> { if(v > 1) System.out.println(k+", "+v);} );
      
    } catch (InterruptedException e1) {
      // TODO Auto-generated catch block
      e1.printStackTrace();
    }

   
    
    

    
    
  }

  public static void main(String... args) {
    System.exit(new CommandLine(new KafkaDuplicateDetector()).execute(args));

  }
}
