package com.nucleodb.library.mqs.local;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.nucleodb.library.database.modifications.Modify;
import com.nucleodb.library.database.utils.Serializer;
import com.nucleodb.library.mqs.ProducerHandler;
import com.nucleodb.library.mqs.config.MQSSettings;
import org.apache.kafka.clients.producer.Callback;
import java.util.logging.Logger;

public class LocalProducerHandler extends ProducerHandler{
  private static Logger logger = Logger.getLogger(LocalProducerHandler.class.getName());
  LocalConsumerHandler localConsumerHandler;
  public LocalProducerHandler(MQSSettings settings, LocalConsumerHandler localConsumerHandler) {
    super(settings);
    this.localConsumerHandler = localConsumerHandler;
    logger.info("local producer handler started for "+super.getTopic());
  }

  @Override
  public void push(String key, long version, Modify modify, Callback callback) {
    //logger.info("message pushed to local producer, sending to consumer");
    try {
      localConsumerHandler.getQueue().add(
          modify.getClass().getSimpleName() + Serializer.getObjectMapper().getOm().writeValueAsString(modify)
      );
      localConsumerHandler.getLeftToRead().incrementAndGet();
      synchronized (localConsumerHandler.getQueue()) {
        localConsumerHandler.getQueue().notifyAll();
      }
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void push(String key, String message) {
    localConsumerHandler.getQueue().add(message);
    localConsumerHandler.getLeftToRead().incrementAndGet();
    synchronized (localConsumerHandler.getQueue()) {
      localConsumerHandler.getQueue().notifyAll();
    }
  }
}
