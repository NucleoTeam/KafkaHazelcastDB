package com.nucleodb.library.mqs.config;

import com.nucleodb.library.database.utils.Serializer;
import com.nucleodb.library.mqs.ConsumerHandler;
import com.nucleodb.library.mqs.ProducerHandler;
import com.nucleodb.library.mqs.kafka.KafkaConsumerHandler;

import java.beans.IntrospectionException;
import java.beans.PropertyDescriptor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

public class MQSConfiguration {
  private static Logger logger = Logger.getLogger(MQSConfiguration.class.getName());
  MQSConstructorSettings<? extends ConsumerHandler> consumer;
  MQSConstructorSettings<? extends ProducerHandler> producer;

  Class<? extends MQSSettings> settingClass;

  public MQSConfiguration(
      MQSConstructorSettings<? extends ConsumerHandler> consumer,
      MQSConstructorSettings<? extends ProducerHandler> producer,
      Class<? extends MQSSettings> settingClass
  ) {
    this.settingClass = settingClass;
    this.consumer = consumer;
    this.producer = producer;
  }

  public MQSConstructorSettings<? extends ConsumerHandler> getConsumer() {
    return consumer;
  }

  public void setConsumer(MQSConstructorSettings<? extends ConsumerHandler> consumer) {
    this.consumer = consumer;
  }

  public MQSConstructorSettings<? extends ProducerHandler> getProducer() {
    return producer;
  }

  public void setProducer(MQSConstructorSettings<? extends ProducerHandler> producer) {
    this.producer = producer;
  }

  public MQSSettings getSettings(Map<String, Object> settingsMap) throws NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException {
    return settingClass.getDeclaredConstructor(Map.class).newInstance(settingsMap);
  }

  public ConsumerHandler createConsumerHandler(Map<String, Object> settingsMap) throws NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException, IntrospectionException {
    List<Object> objects = new LinkedList<>();
    MQSSettings settings = getSettings(settingsMap);
    objects.add(settings);
    for (int i = 0; i < consumer.getConstructorGetterElements().length; i++) {
      Method readMethod = new PropertyDescriptor(consumer.getConstructorGetterElements()[i], settings.getClass()).getReadMethod();
      if(readMethod!=null){
        objects.add(readMethod.invoke(settings));
      }else{
        logger.severe(String.format("No value set for MQS setting %s", consumer.getConstructorGetterElements()[i]));
        System.exit(1);
      }
    }

    return consumer.getClazz().getDeclaredConstructor(consumer.getConstructorTypes()).newInstance(
        objects.toArray()
    );
  }
  public ProducerHandler createProducerHandler(Map<String, Object> settingsMap) throws NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException, IntrospectionException {
    List<Object> objects = new LinkedList<>();
    MQSSettings settings = getSettings(settingsMap);
    objects.add(settings);
    for (int i = 0; i < producer.getConstructorGetterElements().length; i++) {
      Method readMethod = new PropertyDescriptor(producer.getConstructorGetterElements()[i], settings.getClass()).getReadMethod();
      if(readMethod!=null){
        objects.add(readMethod.invoke(settings));
      }else{
        logger.severe(String.format("No value set for MQS setting %s", consumer.getConstructorGetterElements()[i]));
        System.exit(1);
      }
    }
    return producer.getClazz().getDeclaredConstructor(producer.getConstructorTypes()).newInstance(
        objects.toArray()
    );
  }
}
