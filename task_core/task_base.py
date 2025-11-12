"""
任务基类模块

定义所有任务的基础接口和通用功能
"""
import logging
import time
import json
from abc import ABC, abstractmethod
from typing import Dict, Any, Optional, List, Union
from function_messaging.kafka_client import get_collect_producer

logger = logging.getLogger('task_base')


class BaseTask(ABC):
    """
    任务基类，所有具体任务都应继承此类
    
    提供任务执行的框架，包括错误处理、日志记录和执行统计
    """
    
    # 任务ID，每个任务子类应该定义唯一的ID
    TASK_ID: str = "base_task"
    
    # 任务名称
    TASK_NAME: str = "基础任务"
    
    # 任务描述
    TASK_DESCRIPTION: str = "任务基类，所有任务的基础"
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        初始化任务
        
        Args:
            config: 任务配置参数
        """
        self.config = config or {}
        self.last_run_time: Optional[float] = None
        self.last_run_success: Optional[bool] = None
        self.last_run_message: Optional[str] = None
        self.run_count: int = 0
        self.success_count: int = 0
        self.failure_count: int = 0
        self.run_time: float = 0.0
        
        # 设置当前任务的日志级别
        self._setup_task_logger()
    
    def _setup_task_logger(self):
        """
        根据配置设置当前任务的日志级别
        可以通过配置中的task_log_level单独设置此任务的日志级别
        """
        task_log_level = self.config.get('task_log_level', None)
        if task_log_level:
            try:
                # 将字符串转换为logging模块的级别常量
                numeric_level = getattr(logging, task_log_level.upper(), None)
                if numeric_level is not None and isinstance(numeric_level, int):
                    # 获取任务特定的logger
                    task_logger = logging.getLogger(f'task_{self.TASK_ID}')
                    task_logger.setLevel(numeric_level)
                    # 同时也设置类实例的logger级别（如果存在）
                    if hasattr(self, 'logger'):
                        self.logger.setLevel(numeric_level)
                    logger.debug(f"已设置任务 {self.TASK_ID} 日志级别为: {task_log_level}")
            except (AttributeError, ValueError) as e:
                logger.warning(f"无效的日志级别配置: {task_log_level}, 任务: {self.TASK_ID}, 错误: {str(e)}")
        
    @abstractmethod
    def execute(self) -> Dict[str, Any]:
        """
        执行任务的核心逻辑，子类必须实现此方法
        
        Returns:
            Dict[str, Any]: 任务执行结果
        """
        pass
    
    def run(self) -> Dict[str, Any]:
        """
        运行任务，包含错误处理、日志记录和自动Kafka消息发送
        
        Returns:
            Dict[str, Any]: 任务执行结果，包括元数据
        """
        start_time = time.time()
        self.run_count += 1
        result = {}
        
        try:
            logger.info(f"开始执行任务: {self.TASK_NAME} ({self.TASK_ID})")
            
            # 执行实际任务逻辑
            execution_result = self.execute()
            
            # 更新任务状态
            self.last_run_time = time.time()
            self.last_run_success = True
            self.success_count += 1
            
            # 构建结果
            result = {
                "success": True,
                "task_id": self.TASK_ID,
                "task_name": self.TASK_NAME,
                "execution_time": round(time.time() - start_time, 3),
                "result": execution_result,
                "message": "任务执行成功"
            }

            warning_interval = self.config.get("warning_interval", None)
            if warning_interval is not None:
                # 安全地将warning_interval转换为整数
                try:
                    warning_interval_int = int(warning_interval)
                    if result["execution_time"] > warning_interval_int:
                        logger.error(f"任务执行超时: {self.TASK_NAME} ({self.TASK_ID}), 耗时: {result['execution_time']}s")
                except (ValueError, TypeError):
                    logger.warning(
                        f"warning_interval配置值无法转换为整数: {warning_interval}, 任务: {self.TASK_NAME} ({self.TASK_ID})")
            
            # 自动处理Kafka消息发送
            self._auto_send_to_kafka(execution_result)

            logger.info(f"任务执行成功: {self.TASK_NAME} ({self.TASK_ID}), 耗时: {result['execution_time']}s")
            
        except Exception as e:
            # 更新任务状态
            self.last_run_time = time.time()
            self.last_run_success = False
            self.failure_count += 1
            error_message = str(e)
            
            # 构建错误结果
            result = {
                "success": False,
                "task_id": self.TASK_ID,
                "task_name": self.TASK_NAME,
                "execution_time": round(time.time() - start_time, 3),
                "error": error_message,
                "message": "任务执行失败"
            }
            
            logger.error(f"任务执行失败: {self.TASK_NAME} ({self.TASK_ID}), 错误: {error_message}")
        self.run_time = round(time.time() - start_time, 3)
        self.last_run_message = result.get("message", "")
        return result
    
    def get_status(self) -> Dict[str, Any]:
        """
        获取任务当前状态信息
        
        Returns:
            Dict[str, Any]: 任务状态信息
        """
        return {
            "task_id": self.TASK_ID,
            "task_name": self.TASK_NAME,
            "description": self.TASK_DESCRIPTION,
            "last_run_time": self.last_run_time,
            "last_run_success": self.last_run_success,
            "last_run_message": self.last_run_message,
            "run_count": self.run_count,
            "success_count": self.success_count,
            "failure_count": self.failure_count,
            "config": self.config,
            "run_time": self.run_time
        }
    
    def update_config(self, config: Dict[str, Any]) -> None:
        """
        更新任务配置
        
        Args:
            config: 新的配置参数
        """
        self.config.update(config)
        logger.info(f"更新任务配置: {self.TASK_NAME} ({self.TASK_ID})")
    
    def _auto_send_to_kafka(self, execution_result: Dict[str, Any]) -> None:
        """
        自动根据配置将执行结果发送到Kafka
        该方法在run方法中自动调用，子类无需手动处理
        
        Args:
            execution_result: 任务执行结果
        """
        try:
            # 读取Kafka相关配置
            send_to_kafka = self.config.get('send_to_kafka', False)
            
            # 如果配置了发送到Kafka
            if send_to_kafka:
                # 获取其他Kafka配置参数

                logger.debug(f"任务 {self.TASK_ID} 准备发送数据到Kafka")
                
                # 调用统一的发送方法
                kafka_result = self.send_collected_data_to_kafka(execution_result)
                
                logger.info(f"任务 {self.TASK_ID} 数据发送到Kafka完成: 成功{kafka_result['success']}, 失败{kafka_result['failed']}")
                
                # 在执行结果中添加Kafka发送状态
                if isinstance(execution_result, dict):
                    execution_result['kafka_status'] = kafka_result
                    
        except Exception as e:
            logger.error(f"任务 {self.TASK_ID} 自动发送Kafka消息时发生错误: {str(e)}")
            # 不抛出异常，避免影响主任务流程

    def send_collected_data_to_kafka(self, result: Dict[str, Any]) -> Dict[str, Any]:
        """
        发送采集任务结果到Kafka的统一接口
        统一按批量方式处理数据
        
        Args:
            result: 采集任务的执行结果
            
        Returns:
            Dict[str, Any]: 发送结果统计
        """
        try:
            # 获取Kafka生产者实例
            producer = get_collect_producer()
            collect_data = result.get('data', {})

            # 如果没有数据，直接返回
            if not collect_data:
                logger.debug(f"任务 {self.TASK_ID} 没有需要发送的数据")
                return {"success": 0, "failed": 0}

            kafka_messages = []

            if isinstance(collect_data, dict):
                for key, value in collect_data.items():
                    kafka_messages.append({
                        "key": key,
                        "value": value
                    })
            else:
                logger.error(f"任务 {self.TASK_ID} 发送的数据格式不正确")

            # 发送批量消息
            result = producer.send_batch(kafka_messages)
            
            # 记录发送结果
            total_sent = result['success'] + result['failed']
            logger.info(f"任务 {self.TASK_ID} 完成批量数据发送，总计 {total_sent} 条消息")
            
            return result
            
        except Exception as e:
            logger.error(f"任务 {self.TASK_ID} 通过统一接口发送数据到Kafka时发生错误: {str(e)}")
            # 估算失败数量
            return {"success": 0, "failed": "-1"}