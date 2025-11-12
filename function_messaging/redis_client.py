import redis
from typing import Optional

# Redis连接配置
REDIS_CONFIG = {
    'host': '192.168.56.10',
    'port': 6379,
    'db': 1
}

# 定义redis连接池
_pool = None

# 全局Redis实例
_redis_instance = None

def get_redis(db: Optional[int] = None) -> redis.Redis:
    """
    获取Redis连接实例
    
    Args:
        db: 数据库索引，默认为None（使用配置中的默认值）
    
    Returns:
        redis.Redis: Redis连接实例
    """
    global _pool, _redis_instance
    
    # 如果指定了不同的数据库，创建新的连接
    if db is not None and db != REDIS_CONFIG['db']:
        config = REDIS_CONFIG.copy()
        config['db'] = db
        return redis.Redis(**config)
    
    # 初始化连接池（如果尚未初始化）
    if _pool is None:
        _pool = redis.ConnectionPool(**REDIS_CONFIG)
    
    # 初始化全局Redis实例（如果尚未初始化）
    if _redis_instance is None:
        _redis_instance = redis.Redis(connection_pool=_pool)
    
    return _redis_instance

# 保留原有red变量以保持向后兼容
red = get_redis()
