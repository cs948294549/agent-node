import socket
import logging
import threading
import time
from typing import Dict, Any, Optional, List
from function_messaging.kafka_client import get_syslog_producer
from config import Config

logger = logging.getLogger(__name__)

class SyslogServer:
    """
    Syslog服务器类，仅支持UDP协议接收syslog消息
    """
    def __init__(self, config=None):
        """
        初始化Syslog服务器
        
        Args:
            config: 配置对象，如为None则使用默认配置
        """
        # 使用传入的配置或默认配置
        self.config = config if config else Config()
        
        # 从配置中加载参数
        self.server_ip = getattr(self.config, 'syslog_server_ip', 514)
        self.udp_port = getattr(self.config, 'syslog_udp_port', 514)
        self.max_message_size = getattr(self.config, 'syslog_max_message_size', 1024 * 1024)
        # 对于长期监听场景，建议设置为5-10秒，平衡响应性和CPU资源消耗
        self.receive_timeout = getattr(self.config, 'syslog_receive_timeout', 5)
        
        # Kafka生产者
        self.producer = None
        
        # 服务器线程
        self.udp_thread = None
        
        # 运行状态
        self.running = False
        
        # 初始化Kafka生产者
        self._init_kafka_producer()
    
    def _init_kafka_producer(self):
        """
        初始化Kafka生产者
        """
        try:
            self.producer = get_syslog_producer()
            logger.info(f"Kafka生产者初始化成功: 使用全局syslog_producer实例")
        except Exception as e:
            logger.error(f"Kafka生产者初始化失败: {str(e)}")
            self.producer = None
    
    def start(self):
        """
        启动syslog服务器
        """
        try:
            # 确保只初始化一次Kafka生产者
            if not self.producer:
                self._init_kafka_producer()
            
            self.running = True
            logger.info(f"Starting Syslog Server...")
            
            # 启动UDP服务
            self.udp_thread = threading.Thread(target=self._udp_server)
            self.udp_thread.daemon = True
            self.udp_thread.start()
            logger.info(f"UDP Syslog Server started on port {self.udp_port}")
            
            logger.info("Syslog Server started successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to start Syslog Server: {str(e)}")
            self.running = False
            return False
    
    def stop(self):
        """
        停止syslog服务器
        """
        logger.info("Stopping Syslog Server...")
        
        # 设置停止标志
        self.running = False
        
        # 等待UDP线程结束
        if self.udp_thread and self.udp_thread.is_alive():
            try:
                self.udp_thread.join(timeout=2)
                if self.udp_thread.is_alive():
                    logger.warning("UDP server thread did not terminate properly")
            except Exception as e:
                logger.error(f"Error joining UDP thread: {str(e)}")
        
        # 关闭Kafka生产者
        if self.producer:
            try:
                self.producer.close()
                logger.info("Kafka producer closed successfully")
            except Exception as e:
                logger.error(f"Error closing Kafka producer: {str(e)}")
            finally:
                self.producer = None
        
        # 重置线程引用
        self.udp_thread = None
        
        logger.info("Syslog Server stopped")
    
    def _udp_server(self):
        """
        UDP服务器处理函数
        """
        sock = None
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.settimeout(self.receive_timeout)
            sock.bind((self.server_ip, self.udp_port))
            
            logger.info(f"UDP Syslog Server listening on port {self.udp_port}")
            
            while self.running:
                try:
                    data, addr = sock.recvfrom(self.max_message_size)
                    self._process_message(data, addr)
                except socket.timeout:
                    continue
                except socket.error as e:
                    if self.running:  # 只在服务器运行时记录错误
                        logger.error(f"UDP socket error: {str(e)}")
                    break
                except Exception as e:
                    logger.error(f"UDP server error: {str(e)}")
                    
        except socket.error as e:
            logger.error(f"Failed to create/bind UDP socket: {str(e)}")
        except Exception as e:
            logger.error(f"Failed to start UDP server: {str(e)}")
        finally:
            if sock:
                try:
                    sock.close()
                except Exception:
                    pass
    
    def _process_message(self, data: bytes, addr: tuple):
        """
        处理接收到的syslog消息
        
        Args:
            data: 原始消息数据
            addr: 发送方地址
        """
        try:
            # 检查消息大小
            if len(data) > self.max_message_size:
                logger.warning(f"Message from {addr} exceeds max size, truncating")
                data = data[:self.max_message_size]

            # 按顺序尝试不同的编码方式
            message_text = None
            detected_encoding = None
            
            for encoding in ['utf-8', 'latin-1']:
                try:
                    message_text = data.decode(encoding)
                    detected_encoding = encoding
                    break
                except UnicodeDecodeError:
                    # 如果解码失败，尝试下一种编码
                    continue
            
            # 如果所有编码都失败，使用utf-8并替换无效字符
            if message_text is None:
                message_text = data.decode('utf-8', errors='replace')
                detected_encoding = 'utf-8_replaced'
            
            # 构建简化的消息对象，移除解析步骤
            syslog_message = {
                "raw_message": message_text,
                "source_ip": addr[0],
                "source_port": addr[1],
                "protocol": "udp",
                "receive_timestamp": time.time(),
                "detected_encoding": detected_encoding
            }
            
            # 发送到Kafka
            if self.producer:
                try:
                    # 使用源IP作为消息key，便于在Kafka中分区
                    success = self.producer.send(syslog_message, key=addr[0])
                    if not success:
                        logger.warning(f"Failed to send message to Kafka from {addr}")
                except Exception as e:
                    logger.error(f"Exception when sending to Kafka: {str(e)}")
            else:
                logger.warning("No Kafka producer available, message not forwarded")
                
        except Exception as e:
            logger.error(f"Error processing syslog message from {addr}: {str(e)}")

# 创建全局syslog服务器实例
_syslog_server = None

def get_syslog_server() -> SyslogServer:
    """
    获取全局syslog服务器实例
    
    Returns:
        SyslogServer: syslog服务器实例
    """
    global _syslog_server
    if _syslog_server is None:
        from config import Config
        _syslog_server = SyslogServer(config=Config())
    return _syslog_server


def start_syslog_server() -> bool:
    """
    启动全局syslog服务器
    
    Returns:
        bool: 是否启动成功
    """
    server = get_syslog_server()
    return server.start()


def stop_syslog_server():
    """
    停止全局syslog服务器
    """
    server = get_syslog_server()
    server.stop()