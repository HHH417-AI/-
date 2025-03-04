# 实时数据统计
大小写不敏感的实时流式数据统计
# 概述
本示例演示如何通过Kafka生产者生成实时单词数据，并使用Apache Flink进行窗口化单词计数统计。结果以5分钟窗口周期输出到文件系统。

# 架构流程
[Kafka Producer] --> (testtopic) --> [流式计算引|擎(Source -> Map ->KeyBy ->
Window ->Sink)] --> (本地文件系统)
# 环境要求
Java 8+
zookeeper 3.4
Kafka 2.11
三节点集群 (master/slave1/slave2)

# 配置文件说明
Kafka Producer配置
bootstrap.servers=master:9092,slave1:9092,slave2:9092
目标主题：testtopic

Flink Job配置
消费组：flink-consumer-group
输出路径：./output (本地目录)
窗口长度：5分钟滚动窗口
 执行步骤
 1. 启动Kafka集群
# 在所有节点启动zookeeper和Kafka服务
cd /opt/software/flink/zookeeper/bin
./zkServer.sh start
kafka-server-start.sh /opt/software/flink/kafka/config/server.properties

# 创建主题（在任意节点执行）
kafka-topics.sh --create \
--bootstrap-server master:9092,slave1:9092,slave2:9092 \
--topic testtopic \
--partitions 2 \
--replication-factor 2
2. 运行WordCountSource生产数据传入到kafka
输出示例：
Sent: PIE,3
Sent: BANANA,2
Sent: apple,4
...
3. 运行KafkaWordCountFlink从kafka读取数据写入到文本文档中
输出示例：
2025/03/03T21:05:00,banana,47
2025/03/03T21:05:00,pie,43
2025/03/03T21:05:00,apple,31

4. 验证结果
检查输出目录（每5分钟生成一次内容，当超过5分钟没有输出内容，则产生一个新的文档存储）：

./output/
# 输出类似：
.part-01179885-b6d0-4fc1-86c4-03b374fc26d8-0.inprogress.8b142f9b-07e4-41bf-8cc2-bb0f5843286f

# 查看文件内容
在本地找到文档（./output），打开即可查看

# 格式示例：
2023/05/01T12:05:00,apple,7
2023/05/01T12:05:00,banana,12
2023/05/01T12:05:00,pie,9

# 数据处理逻辑
数据标准化：所有单词转为小写

窗口统计：每5分钟统计一次单词出现总数

时间格式：使用窗口结束时间作为触发时间

输出格式：触发时间,单词,总次数

# 注意事项
确保Kafka集群地址与代码配置一致

输出目录需有写入权限

窗口触发依赖处理时间（Processing Time）

文件滚动策略：

每5分钟写入文档一次

当超过5分钟没有输出内容，则产生一个新的文档存储

128MB文件大小限制

