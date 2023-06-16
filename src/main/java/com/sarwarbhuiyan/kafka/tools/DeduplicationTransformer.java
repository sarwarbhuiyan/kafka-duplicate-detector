package com.sarwarbhuiyan.kafka.tools;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;

public class DeduplicationTransformer<K,V,KOut,VOut> implements Processor<K, V, KOut, VOut> {
  
  //private ProcessorContext context;

  private String stateStoreName;
  /**
   * Key: event ID
   * Value: timestamp (event-time) of the corresponding event when the event ID was seen for the
   * first time
   */
  private WindowStore<String, Long> eventIdStore;

  private final long leftDurationMs;
  private final long rightDurationMs;

  private final KeyValueMapper<K, V, String> idExtractor;

  private ProcessorContext context;
  /**
   * @param maintainDurationPerEventInMs how long to "remember" a known event (or rather, an event
   *                                     ID), during the time of which any incoming duplicates of
   *                                     the event will be dropped, thereby de-duplicating the
   *                                     input.
   * @param idExtractor                  extracts a unique identifier from a record by which we de-duplicate input
   *                                     records; if it returns null, the record will not be considered for
   *                                     de-duping but forwarded as-is.
   */
  DeduplicationTransformer(final String stateStoreName, final long maintainDurationPerEventInMs, final KeyValueMapper<K, V, String> idExtractor) {
    this.stateStoreName = stateStoreName;
    if (maintainDurationPerEventInMs < 1) {
      throw new IllegalArgumentException("maintain duration per event must be >= 1");
    }
    leftDurationMs = maintainDurationPerEventInMs / 2;
    rightDurationMs = maintainDurationPerEventInMs - leftDurationMs;
    this.idExtractor = idExtractor;
  }

  @Override
  //@SuppressWarnings("unchecked")
  public void init(ProcessorContext<KOut, VOut> context) {
    this.context = context;
    this.eventIdStore = (WindowStore<String, Long>) context.getStateStore(this.stateStoreName);
  }

  private boolean isDuplicate(final String eventId, final long eventTime) {
    if(eventIdStore == null)
      System.err.println("Event Store is Null");
    final WindowStoreIterator<Long> timeIterator = eventIdStore.fetch(
      eventId,
      eventTime - leftDurationMs,
      eventTime + rightDurationMs);
    final boolean isDuplicate = timeIterator.hasNext();
    timeIterator.close();
    return isDuplicate;
  }

  private void updateTimestampOfExistingEventToPreventExpiry(final String eventId, final long newTimestamp) {
    eventIdStore.put(eventId, newTimestamp, newTimestamp);
  }

  private void rememberNewEvent(final String eventId, final long timestamp) {
    eventIdStore.put(eventId, timestamp, timestamp);
  }

  @Override
  public void close() {
    // Note: The store should NOT be closed manually here via `eventIdStore.close()`!
    // The Kafka Streams API will automatically close stores when necessary.
  }

  @Override
  public void process(Record<K,V> record) {
    
    final String eventId = idExtractor.apply(record.key(), record.value());
    if (eventId == null) {
      context.forward(record);
    } else {
      if (isDuplicate(eventId, record.timestamp())) {
        updateTimestampOfExistingEventToPreventExpiry(eventId, record.timestamp());
      } else {
        context.forward(record);
        rememberNewEvent(eventId, record.timestamp());
      }
    }
  }

}
