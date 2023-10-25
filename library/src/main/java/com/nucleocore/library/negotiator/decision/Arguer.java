package com.nucleocore.library.negotiator.decision;

import com.github.f4b6a3.uuid.UuidCreator;
import com.nucleocore.library.NucleoDBNode;
import com.nucleocore.library.negotiator.decision.support.ArgumentAction;
import com.nucleocore.library.negotiator.decision.support.ArgumentCallback;
import com.nucleocore.library.negotiator.decision.support.ArgumentKafkaMessage;
import com.nucleocore.library.negotiator.decision.support.ArgumentProcess;
import com.nucleocore.library.negotiator.decision.support.ArgumentResult;
import com.nucleocore.library.utils.Serializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.lang.reflect.InvocationTargetException;
import java.util.Queue;
import java.util.concurrent.*;

public class Arguer implements Runnable {
  String argumentTopic;
  NucleoDBNode node;

  protected static final Logger log = LogManager.getLogger(Arguer.class);

  public Arguer(KafkaProducer kafkaProducer, NucleoDBNode node, String argumentTopic) {
    this.kafkaProducer = kafkaProducer;
    this.argumentTopic = argumentTopic;
    this.node = node;
  }

  Queue<ArgumentKafkaMessage> argumentMessageQueue = new LinkedBlockingQueue<>();

  KafkaProducer kafkaProducer;


  boolean debug = false;

  @Override
  public void run() {
    try {
      int loop = 1;
      while (isDebug() && loop==1 || !isDebug()) {
        loop--;
        ArgumentKafkaMessage argumentMessage = null;
        while (!argumentMessageQueue.isEmpty()) {
          if ((argumentMessage = argumentMessageQueue.poll()) != null) {
            execute(argumentMessage);
          }
        }
        Thread.sleep(0,200);
      }
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  public void add(ArgumentKafkaMessage argumentMessage){
    argumentMessageQueue.add(argumentMessage);
  }

  public String getArgumentTopic() {
    return argumentTopic;
  }

  public void setArgumentTopic(String argumentTopic) {
    this.argumentTopic = argumentTopic;
  }

  public NucleoDBNode getNode() {
    return node;
  }

  public void setNode(NucleoDBNode node) {
    this.node = node;
  }

  public Queue<ArgumentKafkaMessage> getArgumentMessageQueue() {
    return argumentMessageQueue;
  }

  public void setArgumentMessageQueue(Queue<ArgumentKafkaMessage> argumentMessageQueue) {
    this.argumentMessageQueue = argumentMessageQueue;
  }

  public KafkaProducer getKafkaProducer() {
    return kafkaProducer;
  }

  public void setKafkaProducer(KafkaProducer kafkaProducer) {
    this.kafkaProducer = kafkaProducer;
  }


  public boolean isDebug() {
    return debug;
  }

  public void setDebug(boolean debug) {
    this.debug = debug;
    if(debug)
      log.atLevel(Level.ALL);
  }

  public void execute(ArgumentKafkaMessage argumentMessage) {
    try {
      ArgumentProcess argumentProcess = (ArgumentProcess) argumentMessage.getProcessData().getLogicProcessorClass().getConstructor(int.class).newInstance(45);
      argumentProcess.process(this.node, argumentMessage.getArgumentStep(), argumentMessage.getProcessData(), new ArgumentCallback<>() {
        @Override
        public void callback(ArgumentAction argumentAction, Object obj) {
          switch (argumentAction) {
            case SEND_TO_TOPIC:
              if (!isDebug()) {
                final ProducerRecord<String, byte[]> record = new ProducerRecord<>(argumentTopic, UuidCreator.getTimeBasedWithRandom().toString(), Serializer.write(obj));
                kafkaProducer.send(record);
              } else if (isDebug() && obj instanceof ArgumentKafkaMessage) {
                argumentMessageQueue.add((ArgumentKafkaMessage) obj);
              }else{
                //System.out.println(obj.getClass().getName());
              }
              break;
            case RUN_FINAL_ACTION:
              if (obj instanceof ArgumentResult) {
                argumentProcess.action(node, (ArgumentResult) obj);
              }
              break;
          }
        }
      });
    } catch (NoSuchMethodException e) {
      e.printStackTrace();
    } catch (InvocationTargetException e) {
      e.printStackTrace();
    } catch (InstantiationException e) {
      e.printStackTrace();
    } catch (IllegalAccessException e) {
      e.printStackTrace();
    }
  }
}
