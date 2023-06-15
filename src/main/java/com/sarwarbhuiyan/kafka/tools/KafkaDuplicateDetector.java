package com.sarwarbhuiyan.kafka.tools;

import java.io.FileInputStream;
import java.security.MessageDigest;
import java.time.Duration;
import java.time.Instant;
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
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.eclipse.collections.api.factory.primitive.ObjectByteMaps;
import org.eclipse.collections.api.map.primitive.MutableObjectByteMap;
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
  
  @Option(required = false, names = {"-m", "--max-messages"},
      description = "Maximum number of messages to scan", showDefaultValue = Visibility.ALWAYS)
  private Integer maxMessages = -1;
  
  @Option(required = false, names = {"-st", "--start-time"},
      description = "Approximate start time (yyyy-MM-ddTHH:mm:ssZ)", showDefaultValue = Visibility.ALWAYS, converter = DateTimeConverter.class)
  private Instant approximateStartTime;
  
  @Option(required = false, names = {"-et", "--end-time"},
      description = "Approximate end time (yyyy-MM-ddTHH:mm:ssZ)", showDefaultValue = Visibility.ALWAYS, converter = DateTimeConverter.class)
  private Instant approximateEndTime;
  
  
  
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
      if(approximateStartTime !=null) 
      {
        Map<TopicPartition, Long> partitionTimestampMap = partitions.stream()
            .collect(Collectors.toMap(tp -> tp, tp -> approximateStartTime.toEpochMilli()-1000)); //1s before
        
        Map<TopicPartition, OffsetAndTimestamp> partitionOffsetMap = consumer.offsetsForTimes(partitionTimestampMap);
        partitionOffsetMap.forEach((tp, offsetAndTimestamp) -> consumer.seek(tp, offsetAndTimestamp.offset()));
      }

      
      Map<TopicPartition, Long> beginningOffsets = consumer.beginningOffsets(partitions);
      Map<Integer, Long> endOffsets = consumer.endOffsets(partitions).entrySet().stream().collect(Collectors.toMap(
          entry -> ((TopicPartition)entry.getKey()).partition(), 
          entry -> entry.getValue()
          ));
      
     // Map<String, Byte> messageCount = new HashMap<>();
      MutableObjectByteMap<String> messageCount = ObjectByteMaps.mutable.empty();
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
          
         
        Map<Integer, Long> pollLatestOffsetsMap = new HashMap<>();
        for(ConsumerRecord<byte[], byte[]> record: records) {
          
          
          if(approximateEndTime!=null && record.timestamp() >= (approximateEndTime.toEpochMilli()+1000)) continue; //+1s more than specified.
          totalMessageCount++;
          //messageDigest.update(record.value());
          //byte[] md5Hash = messageDigest.digest();
          String md5Hash = DigestUtils.md5Hex(record.value());
          
          

          
          if(messageCount.containsKey(md5Hash)) {
            //System.out.println("Found duplicate "+record.value()+" at partition "+record.partition()+" offset "+record.offset());
            
            messageCount.put(md5Hash, (byte)(messageCount.get(md5Hash)+1));
          }
          else {
            
            messageCount.put(md5Hash, (byte) 1);
          }
          pollLatestOffsetsMap.put(record.partition(), record.offset());
          //System.out.println("scanned "+record.partition()+" "+record.offset());
          if(maxMessages > 0 && totalMessageCount >= maxMessages) break;
        }
        
        for(Map.Entry<Integer,Long> e: pollLatestOffsetsMap.entrySet()) {
          //System.out.println("e.getValue() "+e.getValue()+" endOffset="+endOffsets.get(e.getKey()));
          if(e.getValue() >= endOffsets.get(e.getKey())-1) {
            
            partitionDoneStatus.put(e.getKey(), Boolean.TRUE);
          }
        }
        
        if(maxMessages > 0 && totalMessageCount >= maxMessages) break;
        
        if(partitionDoneStatus.values().stream().allMatch(x -> x)) {
          System.out.println("Finishing reading till end offsets");
          break;
        }
        
        
      }
      
      long endTime = System.currentTimeMillis();
      System.out.println("Total run time: "+((endTime-startTime)/(1000))+" seconds.");
      // print stats
      //MutableObjectByteMap dupesCount = messageCount..stream().filter(e -> e.getValue().byteValue() > 1).collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue()));
      int dupesCount = messageCount.select(b -> b > (byte)1).size();
      
      System.out.println("Total Messages: "+totalMessageCount);
      System.out.println("Dupes Found: "+dupesCount);
      System.out.println("Percentage: "+((100.0*dupesCount)/totalMessageCount));

      final AtomicInteger i = new AtomicInteger(2);

      while(true) {
        int frequencyCount =  messageCount.select(b -> b > i.byteValue()).size();
        
        if(frequencyCount>0)
          System.out.println("Frequency (n="+i+")  : "+frequencyCount);
        else
          break;
        i.incrementAndGet();
      }
      
      System.out.println("Memory usage after="+(Runtime.getRuntime().totalMemory()-Runtime.getRuntime().freeMemory())/(1000*1000)+"M");
      
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
