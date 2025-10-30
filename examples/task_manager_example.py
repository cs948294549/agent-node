#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
任务管理器单独使用示例

这个示例展示如何不依赖Agent，直接使用TaskManager来管理和调度任务
"""

import logging
import time
from task_core.task_manager import TaskManager
from task_core.task_factory import TaskFactory
from task_core.task_base import BaseTask

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('task_manager_example')


# 创建一个自定义任务类，用于演示
class ExampleTask(BaseTask):
    """示例任务类"""
    TASK_ID = "example_task"
    TASK_NAME = "示例任务"
    TASK_DESCRIPTION = "一个用于演示任务管理器的示例任务"
    
    def execute(self):
        """执行任务逻辑"""
        logger.info("执行示例任务...")
        result = {
            "timestamp": time.time(),
            "message": "这是示例任务的执行结果",
            "config_used": self.config or {"default": "config"}
        }
        # 模拟一些处理时间
        time.sleep(1)
        logger.info(result)
        return result


class CounterTask(BaseTask):
    """计数任务类"""
    TASK_ID = "counter_task"
    TASK_NAME = "计数任务"
    TASK_DESCRIPTION = "一个计数的示例任务"
    
    def __init__(self, config=None):
        super().__init__(config)
        self.counter = 0
    
    def execute(self):
        """执行计数任务"""
        self.counter += 1
        logger.info(f"计数: {self.counter}")
        return {"count": self.counter, "timestamp": time.time()}


def main():
    """主函数，演示如何使用TaskManager"""
    
    logger.info("开始演示任务管理器的单独使用...")
    
    # 1. 初始化任务工厂并注册自定义任务类
    logger.info("注册自定义任务类...")
    TaskFactory.register_task_class(ExampleTask.TASK_ID, ExampleTask)
    TaskFactory.register_task_class(CounterTask.TASK_ID, CounterTask)
    
    # 2. 创建任务管理器实例
    logger.info("创建任务管理器实例...")
    task_manager = TaskManager()
    
    # # 3. 注册并调度任务
    # logger.info("注册示例任务...")
    # # 注册示例任务，间隔5秒执行一次
    # task_manager.register_task(
    #     task_id=ExampleTask.TASK_ID,
    #     config={"custom_param": "hello world", "timeout": 10},
    #     schedule_type="interval",
    #     schedule_config={"seconds": 5}
    # )
    
    # 注册计数任务，间隔2秒执行一次
    logger.info("注册计数任务...")
    task_manager.register_task(
        task_id=CounterTask.TASK_ID,
        config={"initial_count": 0},
        schedule_type="interval",
        schedule_config={"seconds": 5}
    )

    time.sleep(20)

    task_manager.update_task_schedule(
        task_id=CounterTask.TASK_ID,
        schedule_type="interval",
        schedule_config={"seconds": 2}
    )


if __name__ == "__main__":
    from core.scheduler import scheduler
    scheduler.start()
    main()


    while True:
        time.sleep(1)