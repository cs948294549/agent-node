"""
Redis客户端模块

提供单例模式的Redis客户端，使用连接池提高性能
"""
import logging
import redis
from typing import Optional, Any, Dict, List
from redis.client import Pipeline
from redis.exceptions import RedisError

logger = logging.getLogger(__name__)


class RedisClient:
    """
    Redis客户端类 - 单例模式实现
    
    提供Redis连接和基本操作功能的封装，使用连接池提高性能
    全局使用一个Redis客户端实例，简化管理
    """
    # 单例实例
    _instance = None
    
    def __new__(cls, *args, **kwargs):
        """
        单例模式实现
        """
        if cls._instance is None:
            cls._instance = super(RedisClient, cls).__new__(cls)
        return cls._instance
    
    def __init__(self, host: str = 'localhost', port: int = 6379, db: int = 0,
                 password: Optional[str] = None, socket_timeout: Optional[int] = None,
                 decode_responses: bool = True, max_connections: int = 10):
        """
        初始化Redis客户端，创建连接池
        
        Args:
            host: Redis服务器地址
            port: Redis服务器端口
            db: 数据库编号
            password: Redis密码
            socket_timeout: 套接字超时时间（秒）
            decode_responses: 是否自动解码响应为字符串
            max_connections: 连接池最大连接数
        """
        # 避免重复初始化
        if not hasattr(self, '_pool'):
            self.host = host
            self.port = port
            self.db = db
            self.password = password
            self.socket_timeout = socket_timeout
            self.decode_responses = decode_responses
            self.max_connections = max_connections
            
            # 创建连接池配置
            self._pool_config = {
                'host': self.host,
                'port': self.port,
                'db': self.db,
                'password': self.password,
                'socket_timeout': self.socket_timeout,
                'decode_responses': self.decode_responses,
                'max_connections': self.max_connections
            }
            
            self._pool: Optional[redis.ConnectionPool] = None
            self._redis_client: Optional[redis.Redis] = None
            self._connected = False
            
            # 自动连接
            self.connect()
    
    def connect(self) -> bool:
        """
        创建Redis连接池和客户端实例
        
        Returns:
            bool: 连接成功返回True，失败返回False
        """
        try:
            if not self._connected or not self._pool:
                # 创建连接池
                self._pool = redis.ConnectionPool(**self._pool_config)
                
                # 创建客户端实例
                self._redis_client = redis.Redis(connection_pool=self._pool)
                
                # 测试连接
                self._redis_client.ping()
                self._connected = True
                logger.info(f"成功连接到Redis: {self.host}:{self.port} 数据库: {self.db}, 最大连接数: {self.max_connections}")
                return True
            return True
        except RedisError as e:
            logger.error(f"连接Redis失败: {str(e)}")
            self._connected = False
            self._redis_client = None
            return False
    
    def disconnect(self) -> None:
        """
        关闭Redis连接池和客户端
        """
        if self._pool:
            try:
                # 连接池没有直接的close方法，通过释放引用让GC处理
                self._pool.disconnect()
                logger.info("已关闭Redis连接")
            except RedisError as e:
                logger.error(f"关闭Redis连接失败: {str(e)}")
            finally:
                self._pool = None
                self._redis_client = None
                self._connected = False
    
    @property
    def connected(self) -> bool:
        """
        检查连接状态
        
        Returns:
            bool: 连接状态
        """
        if self._connected and self._redis_client:
            try:
                self._redis_client.ping()
                return True
            except RedisError:
                self._connected = False
        return False
    
    def set(self, key: str, value: Any, ex: Optional[int] = None,
           px: Optional[int] = None, nx: bool = False, xx: bool = False) -> bool:
        """
        设置键值对
        
        Args:
            key: 键名
            value: 键值
            ex: 过期时间（秒）
            px: 过期时间（毫秒）
            nx: 如果为True，仅当键不存在时才设置
            xx: 如果为True，仅当键已存在时才设置
            
        Returns:
            bool: 设置成功返回True，失败返回False
        """
        try:
            if not self.connected:
                if not self.connect():
                    return False
            
            return bool(self._redis_client.set(key, value, ex=ex, px=px, nx=nx, xx=xx))
        except RedisError as e:
            logger.error(f"Redis SET操作失败: {str(e)}")
            self._connected = False
        return False
    
    def get(self, key: str) -> Optional[Any]:
        """
        获取键值
        
        Args:
            key: 键名
            
        Returns:
            Optional[Any]: 获取的值，如果键不存在或发生错误则返回None
        """
        try:
            if not self.connected:
                if not self.connect():
                    return None
            
            return self._redis_client.get(key)
        except RedisError as e:
            logger.error(f"Redis GET操作失败: {str(e)}")
            self._connected = False
        return None
    
    def delete(self, *keys: str) -> int:
        """
        删除键
        
        Args:
            *keys: 要删除的键名列表
            
        Returns:
            int: 成功删除的键的数量
        """
        try:
            if not self.connected:
                if not self.connect():
                    return 0
            
            return self._redis_client.delete(*keys)
        except RedisError as e:
            logger.error(f"Redis DELETE操作失败: {str(e)}")
            self._connected = False
        return 0
    
    def exists(self, key: str) -> bool:
        """
        检查键是否存在
        
        Args:
            key: 键名
            
        Returns:
            bool: 键存在返回True，不存在或发生错误返回False
        """
        try:
            if not self.connected:
                if not self.connect():
                    return False
            
            return bool(self._redis_client.exists(key))
        except RedisError as e:
            logger.error(f"Redis EXISTS操作失败: {str(e)}")
            self._connected = False
        return False
    
    def expire(self, key: str, seconds: int) -> bool:
        """
        设置键的过期时间
        
        Args:
            key: 键名
            seconds: 过期时间（秒）
            
        Returns:
            bool: 设置成功返回True，失败返回False
        """
        try:
            if not self.connected:
                if not self.connect():
                    return False
            
            return bool(self._redis_client.expire(key, seconds))
        except RedisError as e:
            logger.error(f"Redis EXPIRE操作失败: {str(e)}")
            self._connected = False
        return False
    
    def ttl(self, key: str) -> int:
        """
        获取键的剩余生存时间
        
        Args:
            key: 键名
            
        Returns:
            int: 剩余生存时间（秒），-1表示键未设置过期时间，-2表示键不存在
        """
        try:
            if not self.connected:
                if not self.connect():
                    return -2
            
            return self._redis_client.ttl(key)
        except RedisError as e:
            logger.error(f"Redis TTL操作失败: {str(e)}")
            self._connected = False
        return -2
    
    def incr(self, key: str, amount: int = 1) -> Optional[int]:
        """
        增加键的整数值
        
        Args:
            key: 键名
            amount: 增加的数量
            
        Returns:
            Optional[int]: 增加后的整数值，如果发生错误则返回None
        """
        try:
            if not self.connected:
                if not self.connect():
                    return None
            
            return self._redis_client.incrby(key, amount)
        except RedisError as e:
            logger.error(f"Redis INCR操作失败: {str(e)}")
            self._connected = False
        return None
    
    def lpush(self, key: str, *values: Any) -> Optional[int]:
        """
        将一个或多个值推入列表头部
        
        Args:
            key: 键名
            *values: 要推入的值
            
        Returns:
            Optional[int]: 操作后的列表长度，如果发生错误则返回None
        """
        try:
            if not self.connected:
                if not self.connect():
                    return None
            
            return self._redis_client.lpush(key, *values)
        except RedisError as e:
            logger.error(f"Redis LPUSH操作失败: {str(e)}")
            self._connected = False
        return None
    
    def rpop(self, key: str) -> Optional[Any]:
        """
        从列表尾部弹出一个值
        
        Args:
            key: 键名
            
        Returns:
            Optional[Any]: 弹出的值，如果列表为空或发生错误则返回None
        """
        try:
            if not self.connected:
                if not self.connect():
                    return None
            
            return self._redis_client.rpop(key)
        except RedisError as e:
            logger.error(f"Redis RPOP操作失败: {str(e)}")
            self._connected = False
        return None
    
    def hset(self, name: str, key: str, value: Any) -> bool:
        """
        设置哈希表字段的值
        
        Args:
            name: 哈希表名称
            key: 字段名
            value: 字段值
            
        Returns:
            bool: 设置成功返回True，失败返回False
        """
        try:
            if not self.connected:
                if not self.connect():
                    return False
            
            return bool(self._redis_client.hset(name, key, value))
        except RedisError as e:
            logger.error(f"Redis HSET操作失败: {str(e)}")
            self._connected = False
        return False
    
    def hget(self, name: str, key: str) -> Optional[Any]:
        """
        获取哈希表字段的值
        
        Args:
            name: 哈希表名称
            key: 字段名
            
        Returns:
            Optional[Any]: 获取的值，如果字段不存在或发生错误则返回None
        """
        try:
            if not self.connected:
                if not self.connect():
                    return None
            
            return self._redis_client.hget(name, key)
        except RedisError as e:
            logger.error(f"Redis HGET操作失败: {str(e)}")
            self._connected = False
        return None
    
    def hgetall(self, name: str) -> Dict[str, Any]:
        """
        获取哈希表的所有字段和值
        
        Args:
            name: 哈希表名称
            
        Returns:
            Dict[str, Any]: 哈希表的所有字段和值，如果发生错误则返回空字典
        """
        try:
            if not self.connected:
                if not self.connect():
                    return {}
            
            return self._redis_client.hgetall(name)
        except RedisError as e:
            logger.error(f"Redis HGETALL操作失败: {str(e)}")
            self._connected = False
        return {}
    
    def publish(self, channel: str, message: Any) -> int:
        """
        发布消息到频道
        
        Args:
            channel: 频道名称
            message: 要发布的消息
            
        Returns:
            int: 接收到消息的订阅者数量，如果发生错误则返回0
        """
        try:
            if not self.connected:
                if not self.connect():
                    return 0
            
            return self._redis_client.publish(channel, message)
        except RedisError as e:
            logger.error(f"Redis PUBLISH操作失败: {str(e)}")
            self._connected = False
        return 0
    
    def subscribe(self, *channels: str) -> Optional[Any]:
        """
        订阅频道
        
        Args:
            *channels: 要订阅的频道名称列表
            
        Returns:
            Optional[Any]: 订阅对象，如果发生错误则返回None
        """
        try:
            if not self.connected:
                if not self.connect():
                    return None
            
            return self._redis_client.pubsub().subscribe(*channels)
        except RedisError as e:
            logger.error(f"Redis SUBSCRIBE操作失败: {str(e)}")
            self._connected = False
        return None
    
    def pipeline(self) -> Optional[Pipeline]:
        """
        创建管道对象
        
        Returns:
            Optional[Pipeline]: 管道对象，如果发生错误则返回None
        """
        try:
            if not self.connected:
                if not self.connect():
                    return None
            
            return self._redis_client.pipeline()
        except RedisError as e:
            logger.error(f"创建Redis管道失败: {str(e)}")
            self._connected = False
        return None
    
    def execute_pipeline(self, pipe: Pipeline) -> Optional[List[Any]]:
        """
        执行管道中的所有命令
        
        Args:
            pipe: 管道对象
            
        Returns:
            Optional[List[Any]]: 命令执行结果列表，如果发生错误则返回None
        """
        try:
            return pipe.execute()
        except RedisError as e:
            logger.error(f"执行Redis管道失败: {str(e)}")
        return None
    
    def __del__(self):
        """
        析构函数，确保断开连接
        """
        self.disconnect()


# 全局Redis客户端实例
def get_redis_client() -> RedisClient:
    """
    获取全局Redis客户端实例
    
    Returns:
        RedisClient: Redis客户端实例
    """
    return RedisClient()