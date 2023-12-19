package com.nucleodb.library.mqs.kafka;

import com.nucleodb.library.mqs.ConsumerHandler;
import com.nucleodb.library.mqs.ProducerHandler;
import com.nucleodb.library.mqs.config.MQSConfiguration;
import com.nucleodb.library.mqs.config.MQSConstructorSettings;
import com.nucleodb.library.mqs.config.MQSSettings;

public class KafkaConfiguration extends MQSConfiguration{
  public KafkaConfiguration() {
    super(
        new MQSConstructorSettings<>(
            KafkaConsumerHandler.class,
            new String[]{"servers", "groupName", "table"},
            new Class[]{MQSSettings.class, String.class, String.class, String.class}
        ),
        new MQSConstructorSettings<>(
            KafkaProducerHandler.class,
            new String[]{"servers", "table"},
            new Class[]{MQSSettings.class, String.class, String.class}
        ),
        KafkaSettings.class
    );
  }
}
