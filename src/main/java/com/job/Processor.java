package com.job;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;

import org.iq80.leveldb.DB;

import com.StartUp;
import com.job.worker.BaseWorker;
import com.util.Log;

/**
 * Topic 消息处理类
 * 
 * 创建 Worker 线程，对外提供当前 Worker 队列的工作状态
 *
 */
public class Processor implements Runnable {

  private final String topic;

  private final List<String> beanids;

  private final ConsumerConnector connector;

  private final ThreadPoolExecutor pool;
  
  private final DB leveldb;

  public Processor(String topic, ConsumerConnector connector, int maxWorkerCount, List<String> beanids, DB leveldb) {
    this.topic = topic;
    this.connector = connector;
    this.pool = (ThreadPoolExecutor) Executors.newFixedThreadPool(maxWorkerCount);
    this.beanids = beanids;
    this.leveldb = leveldb;
  }

  private boolean isWorking = true;

  public boolean getWorkingStatus() {
    return isWorking;
  }

  public void setProcessorWorking(boolean isWorking) {
    this.isWorking = isWorking;
  }

  public Map<String, Object> getStatus() {
    Map<String, Object> status = new HashMap<String, Object>();
    status.put("pool_size", pool.getPoolSize());
    status.put("active_count", pool.getActiveCount());
    status.put("queue_size", pool.getQueue().size());

    return status;
  }

  @Override
  public void run() {

    if (!isWorking) {
      return;
    }

    Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
    topicCountMap.put(topic, 1);
    Map<String, List<KafkaStream<byte[], byte[]>>> kafkaStreamMap = connector.createMessageStreams(topicCountMap);
    KafkaStream<byte[], byte[]> stream = kafkaStreamMap.get(topic).get(0);
    
    // 接收消息，分配相应的 worker 处理
    for (MessageAndMetadata<byte[], byte[]> messageAndMetadata : stream) {
      
      if (!isWorking) {
        Log.i("********* stop consumer ***********");
        break;
      }

      distributeMessage(new String(messageAndMetadata.message()));
    }
  }

  public void distributeMessage(String message) {
    for (String beanid : beanids) {
      BaseWorker worker  = (BaseWorker)StartUp.getContext().getBean(beanid);
      worker.message = message;
      worker.topic = topic;
      worker.leveldb = leveldb;
      
      pool.submit(worker);
    }
  }

}
