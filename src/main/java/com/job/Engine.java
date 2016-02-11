package com.job;

import static org.iq80.leveldb.impl.Iq80DBFactory.asString;
import static org.iq80.leveldb.impl.Iq80DBFactory.factory;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;

import kafka.consumer.ConsumerConfig;
import kafka.javaapi.consumer.ConsumerConnector;

import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBIterator;
import org.iq80.leveldb.Options;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

import com.StartUp;
import com.job.annotation.Worker;
import com.util.Log;

/**
 * 
 * 任务调度 Engine
 * 
 * 初始化 Consumer 链接 为 Topic 分配对应的 Processor 启动 Daemon 处理消费不成功的Message
 *
 */
@Component
public class Engine {

	/**
	 * 1. 初始化线程池 2. 建立与 kafka 服务之间的链接 3. 创建 levelDB 存储空间
	 * 
	 * @throws IOException
	 */
	@PostConstruct
	public void init() throws IOException {
		this.pool = (ThreadPoolExecutor) Executors.newFixedThreadPool(100);
		this.scheduledPool = (ScheduledThreadPoolExecutor) Executors.newScheduledThreadPool(1);
		this.processorMap = new HashMap<>();

		Properties props = new Properties();
		props.put("zookeeper.connect", zkConnectHost);
		props.put("group.id", topicGroupId);
		props.put("zookeeper.session.timeout.ms", zkSessionTimeoutMs);
		props.put("zookeeper.sync.time.ms", zkSyncTimeMs);
		props.put("auto.commit", "true");
		props.put("auto.commit.interval.ms", autoCommitIntervalMs);
		props.put("autooffset.reset", "largest");

		this.connector = kafka.consumer.Consumer.createJavaConsumerConnector(new ConsumerConfig(props));

		Options options = new Options();
		options.createIfMissing(true);
		this.leveldb = factory.open(new File(dbFilePath), options);
	}

	/**
	 * 1. 从内存中加载 Topic 对应的 Worker 实例 2. 根据 Topic 创建对应的 Processor，并提交 Worker
	 * 进入线程池
	 */
	public void process() {

		// 加载 Topic 对应的 Worker
		ApplicationContext context = StartUp.getContext();
		String[] beans = context.getBeanNamesForAnnotation(Worker.class);
		Map<String, List<String>> beanMap = new HashMap<String, List<String>>();
		for (String s : beans) {
			Worker work = context.findAnnotationOnBean(s, Worker.class);

			if (beanMap.containsKey(work.topic())) {
				beanMap.get(work.topic()).add(s);
				continue;
			}

			beanMap.put(work.topic(), Arrays.asList(s));
		}

		// 启动 Topic 对应的 Worker 线程
		for (String topic : topicListStr.split(",")) {

			if (!beanMap.containsKey(topic)) {
				continue;
			}

			Log.i("topic: " + topic);

			Processor processor = new Processor(topic, connector, maxWorkerCount, beanMap.get(topic), leveldb);
			pool.submit(processor);
			processorMap.put(topic, processor);
		}

		// 每五秒扫描一次LevelDB，重试消费不成功的message
		startDaemon();
	}

	/**
	 * 1. 启动守护线程，每5秒扫描一次 levelDB 中存储的 message 2. 根据 message 对应的 Topic，从
	 * processorMap 中找出对应的 Processor，若不存在则创建 3. 调用 Processor 处理 Message
	 */
	private void startDaemon() {
		scheduledPool.scheduleAtFixedRate(new Runnable() {
			@Override
			public void run() {

				DBIterator iterator = leveldb.iterator();
				try {
					for (iterator.seekToFirst(); iterator.hasNext(); iterator.next()) {
						byte[] key = iterator.peekNext().getKey();
						JSONObject message = new JSONObject(asString(iterator.peekNext().getValue()));
						if (!processorMap.containsKey(message.getString("topic"))) {
							continue;
						}

						Log.i("message" + message);
						// 调用 Topic 对应的 Processor 处理 message
						processorMap.get(message.getString("topic")).distributeMessage(message.getString("value"));
						leveldb.delete(key);
					}

				} catch (Exception e) {
					Log.e(e);
				} finally {
					try {
						iterator.close();
					} catch (IOException e) {
						Log.e(e);
					}
				}
			}
		}, 0, 5, TimeUnit.SECONDS);
	}

	/**
	 * 返回 Engine 工作状态
	 */
	public Map<String, Object> getStatus() {
		Map<String, Object> status = new HashMap<>();
		status.put("pool_size", pool.getPoolSize());
		status.put("active_count", pool.getActiveCount());
		status.put("queue_size", pool.getQueue().size());
		return status;
	}

	private ConsumerConnector connector;

	private ThreadPoolExecutor pool;

	private ScheduledThreadPoolExecutor scheduledPool;

	private Map<String, Processor> processorMap;

	private DB leveldb;

	@Value("${TOPIC_LIST}")
	public String topicListStr;

	@Value("${MAX_WORKER_COUNT}")
	private int maxWorkerCount;

	@Value("${ZK_CONNECT_HOST}")
	private String zkConnectHost;

	@Value("${TOPIC_GROUP_ID}")
	private String topicGroupId;

	@Value("${ZK_SESSION_TIMEOUT_MS}")
	private String zkSessionTimeoutMs;

	@Value("${ZK_SYNC_TIME_MS}")
	private String zkSyncTimeMs;

	@Value("${AUTO_COMMIT_INTERVAL_MS}")
	private String autoCommitIntervalMs;

	@Value("${LEVEL_DB_DIR}")
	private String dbFilePath;

}
