"""
Kafka客户端模块

提供单例模式的Kafka客户端，简化消息收发操作
"""
import logging
from typing import Optional, Any, Dict, List, Callable, Union
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError, KafkaTimeoutError

logger = logging.getLogger(__name__)


class KafkaClient:
    """
    Kafka客户端类，封装Kafka生产者和消费者功能 - 单例模式实现
    """
    # 单例实例
    _instance = None
    
    def __new__(cls, *args, **kwargs):
        """
        单例模式实现
        """
        if cls._instance is None:
            cls._instance = super(KafkaClient, cls).__new__(cls)
        return cls._instance
    
    def __init__(self, bootstrap_servers: Union[str, List[str]] = ['localhost:9092'],
                 client_id: Optional[str] = None):
        """
        初始化Kafka客户端
        
        Args:
            bootstrap_servers: Kafka服务器地址列表或单个地址
            client_id: 客户端ID
        """
        # 避免重复初始化
        if not hasattr(self, '_producer'):
            self.bootstrap_servers = bootstrap_servers
            self.client_id = client_id or 'kafka_client'
            
            self._producer: Optional[KafkaProducer] = None
            self._consumer: Optional[KafkaConsumer] = None
    
    def create_producer(self, acks: str = 'all', 
                       retries: int = 3,
                       retry_backoff_ms: int = 1000,
                       value_serializer: Optional[Callable] = None,
                       key_serializer: Optional[Callable] = None,
                       **kwargs) -> Optional[KafkaProducer]:
        """
        创建Kafka生产者
        
        Args:
            acks: 确认级别，'0', '1', 'all' 或 '-1'
            retries: 重试次数
            retry_backoff_ms: 重试间隔（毫秒）
            value_serializer: 值序列化函数
            key_serializer: 键序列化函数
            **kwargs: 其他KafkaProducer参数
            
        Returns:
            Optional[KafkaProducer]: Kafka生产者实例，如果创建失败则返回None
        """
        try:
            # 如果没有提供序列化器，使用默认的字符串序列化
            if value_serializer is None:
                value_serializer = lambda v: str(v).encode('utf-8')
            if key_serializer is None:
                key_serializer = lambda k: str(k).encode('utf-8') if k is not None else None
            
            self._producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                client_id=self.client_id,
                acks=acks,
                retries=retries,
                retry_backoff_ms=retry_backoff_ms,
                value_serializer=value_serializer,
                key_serializer=key_serializer,
                **kwargs
            )
            
            logger.info(f"成功创建Kafka生产者，连接到: {self.bootstrap_servers}")
            return self._producer
        except KafkaError as e:
            logger.error(f"创建Kafka生产者失败: {str(e)}")
            self._producer = None
            return None
    
    def create_consumer(self, group_id: str,
                       auto_offset_reset: str = 'latest',
                       enable_auto_commit: bool = True,
                       auto_commit_interval_ms: int = 5000,
                       value_deserializer: Optional[Callable] = None,
                       key_deserializer: Optional[Callable] = None,
                       **kwargs) -> Optional[KafkaConsumer]:
        """
        创建Kafka消费者
        
        Args:
            group_id: 消费者组ID
            auto_offset_reset: 自动偏移量重置策略，'latest' 或 'earliest'
            enable_auto_commit: 是否自动提交偏移量
            auto_commit_interval_ms: 自动提交间隔（毫秒）
            value_deserializer: 值反序列化函数
            key_deserializer: 键反序列化函数
            **kwargs: 其他KafkaConsumer参数
            
        Returns:
            Optional[KafkaConsumer]: Kafka消费者实例，如果创建失败则返回None
        """
        try:
            # 如果没有提供反序列化器，使用默认的UTF-8解码
            if value_deserializer is None:
                value_deserializer = lambda v: v.decode('utf-8') if v is not None else None
            if key_deserializer is None:
                key_deserializer = lambda k: k.decode('utf-8') if k is not None else None
            
            self._consumer = KafkaConsumer(
                bootstrap_servers=self.bootstrap_servers,
                client_id=self.client_id,
                group_id=group_id,
                auto_offset_reset=auto_offset_reset,
                enable_auto_commit=enable_auto_commit,
                auto_commit_interval_ms=auto_commit_interval_ms,
                value_deserializer=value_deserializer,
                key_deserializer=key_deserializer,
                **kwargs
            )
            
            logger.info(f"成功创建Kafka消费者，连接到: {self.bootstrap_servers}")
            return self._consumer
        except KafkaError as e:
            logger.error(f"创建Kafka消费者失败: {str(e)}")
            self._consumer = None
            return None
    
    def send_message(self, topic: str, value: Any,
                    key: Optional[Any] = None,
                    partition: Optional[int] = None,
                    timestamp_ms: Optional[int] = None) -> bool:
        """
        发送消息到Kafka主题
        
        Args:
            topic: 主题名称
            value: 消息值
            key: 消息键
            partition: 分区编号
            timestamp_ms: 时间戳（毫秒）
            
        Returns:
            bool: 发送成功返回True，失败返回False
        """
        try:
            if not self._producer:
                self.create_producer()
                
            if not self._producer:
                return False
            
            # 发送消息并等待确认
            future = self._producer.send(
                topic=topic,
                value=value,
                key=key,
                partition=partition,
                timestamp_ms=timestamp_ms
            )
            
            # 等待发送完成
            record_metadata = future.get(timeout=10)  # 等待最多10秒
            logger.debug(f"消息发送成功，主题: {topic}, 分区: {record_metadata.partition}, 偏移量: {record_metadata.offset}")
            return True
            
        except KafkaTimeoutError as e:
            logger.error(f"发送消息超时: {str(e)}")
        except KafkaError as e:
            logger.error(f"发送消息失败: {str(e)}")
        except Exception as e:
            logger.error(f"发送消息发生未知错误: {str(e)}")
        
        return False
    
    def send_messages_batch(self, topic: str, messages: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        批量发送消息到Kafka主题
        
        Args:
            topic: 主题名称
            messages: 消息列表，每个消息是包含value、key、partition等的字典
            
        Returns:
            Dict[str, Any]: 发送结果统计，包含成功和失败的消息数量
        """
        try:
            if not self._producer:
                self.create_producer()
                
            if not self._producer:
                return {"success": 0, "failed": len(messages)}
            
            futures = []
            results = {
                "success": 0,
                "failed": 0,
                "details": []
            }
            
            # 发送所有消息
            for i, msg in enumerate(messages):
                future = self._producer.send(
                    topic=topic,
                    value=msg.get('value'),
                    key=msg.get('key'),
                    partition=msg.get('partition'),
                    timestamp_ms=msg.get('timestamp_ms')
                )
                futures.append((i, future))
            
            # 等待所有消息发送完成
            for i, future in futures:
                try:
                    record_metadata = future.get(timeout=10)
                    results["success"] += 1
                    results["details"].append({
                        "index": i,
                        "status": "success",
                        "partition": record_metadata.partition,
                        "offset": record_metadata.offset
                    })
                except Exception as e:
                    results["failed"] += 1
                    results["details"].append({
                        "index": i,
                        "status": "failed",
                        "error": str(e)
                    })
            
            logger.info(f"批量发送完成，成功: {results['success']}, 失败: {results['failed']}")
            return results
            
        except Exception as e:
            logger.error(f"批量发送消息失败: {str(e)}")
            return {"success": 0, "failed": len(messages), "error": str(e)}
    
    def subscribe(self, topics: Union[str, List[str]]) -> bool:
        """
        订阅Kafka主题
        
        Args:
            topics: 主题名称或主题列表
            
        Returns:
            bool: 订阅成功返回True，失败返回False
        """
        try:
            if not self._consumer:
                logger.error("无法订阅主题，消费者未创建")
                return False
            
            if isinstance(topics, str):
                topics = [topics]
            
            self._consumer.subscribe(topics)
            logger.info(f"成功订阅主题: {topics}")
            return True
        except KafkaError as e:
            logger.error(f"订阅主题失败: {str(e)}")
        return False
    
    def poll_messages(self, timeout_ms: int = 1000, max_records: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        轮询获取消息
        
        Args:
            timeout_ms: 超时时间（毫秒）
            max_records: 最大记录数
            
        Returns:
            List[Dict[str, Any]]: 消息列表，每个消息包含主题、分区、偏移量、键、值等信息
        """
        try:
            if not self._consumer:
                logger.error("无法轮询消息，消费者未创建")
                return []
            
            records = self._consumer.poll(timeout_ms=timeout_ms, max_records=max_records)
            result_messages = []
            
            for topic_partition, messages in records.items():
                for message in messages:
                    msg_data = {
                        "topic": message.topic,
                        "partition": message.partition,
                        "offset": message.offset,
                        "key": message.key,
                        "value": message.value,
                        "timestamp": message.timestamp,
                        "timestamp_type": message.timestamp_type
                    }
                    result_messages.append(msg_data)
            
            return result_messages
            
        except KafkaError as e:
            logger.error(f"轮询消息失败: {str(e)}")
        return []
    
    def commit_offsets(self) -> bool:
        """
        手动提交偏移量
        
        Returns:
            bool: 提交成功返回True，失败返回False
        """
        try:
            if not self._consumer:
                logger.error("无法提交偏移量，消费者未创建")
                return False
            
            self._consumer.commit()
            logger.debug("成功提交偏移量")
            return True
        except KafkaError as e:
            logger.error(f"提交偏移量失败: {str(e)}")
        return False
    
    def close_producer(self) -> None:
        """
        关闭Kafka生产者
        """
        if self._producer:
            try:
                self._producer.flush()  # 确保所有消息都发送出去
                self._producer.close()
                logger.info("已关闭Kafka生产者")
            except KafkaError as e:
                logger.error(f"关闭Kafka生产者失败: {str(e)}")
            finally:
                self._producer = None
    
    def close_consumer(self) -> None:
        """
        关闭Kafka消费者
        """
        if self._consumer:
            try:
                self._consumer.close()
                logger.info("已关闭Kafka消费者")
            except KafkaError as e:
                logger.error(f"关闭Kafka消费者失败: {str(e)}")
            finally:
                self._consumer = None
    
    def close(self) -> None:
        """
        关闭所有连接
        """
        self.close_producer()
        self.close_consumer()
    
    @property
    def producer_connected(self) -> bool:
        """
        检查生产者是否已创建
        
        Returns:
            bool: 生产者已创建返回True，否则返回False
        """
        return self._producer is not None
    
    @property
    def consumer_connected(self) -> bool:
        """
        检查消费者是否已创建
        
        Returns:
            bool: 消费者已创建返回True，否则返回False
        """
        return self._consumer is not None
    
    def __del__(self):
        """
        析构函数，确保关闭所有连接
        """
        self.close()


# 全局Kafka客户端实例
def get_kafka_client() -> KafkaClient:
    """
    获取全局Kafka客户端实例
    
    Returns:
        KafkaClient: Kafka客户端实例
    """
    return KafkaClient()