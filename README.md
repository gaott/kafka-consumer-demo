# kafka-consumer-demo


#### com.job.Engine 

初始化 Consumer 链接 为 Topic 分配对应的 Processor 启动 Daemon 处理消费不成功的Message

#### com.job.Processor 

消息处理器，创建 Worker 线程，对外提供当前 Worker 队列的工作状态

#### com.job.annotation.Worker 

标注工作线程，Engine 启动时，会自动扫描带有 Worker 标注的线程

#### com.job.worker.BaseWorker 

Worker 模板，统一规划工作线程在处理消息时的前置、后置操作

> Kafka 消费端示例，使用 Delegate 方式简化消息分发，目前使用 levelDB 解决消费失败的消息问题，后期考虑引入监控机制
