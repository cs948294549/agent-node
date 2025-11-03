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
from task_implements.HeartbeatTask import HeartbeatTask


# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('task_heartbeat')

def main():
    """主函数，演示如何使用TaskManager"""

    logger.info("开始演示任务管理器的单独使用...")

    # 1. 初始化任务工厂并注册自定义任务类
    logger.info("注册自定义任务类...")
    TaskFactory.register_task_class(HeartbeatTask.TASK_ID, HeartbeatTask)

    # 2. 创建任务管理器实例
    logger.info("创建任务管理器实例...")
    task_manager = TaskManager()

    # 注册计数任务，间隔2秒执行一次
    logger.info("注册计数任务...")
    task_manager.register_task(
        task_instance_id="10101",
        task_class_id=HeartbeatTask.TASK_ID,
        config={"initial_count": 0},
        schedule_type="interval",
        schedule_config={"seconds": 5}
    )

    # time.sleep(20)
    #
    # task_manager.update_task_schedule(
    #     task_id=HeartbeatTask.TASK_ID,
    #     schedule_type="interval",
    #     schedule_config={"seconds": 20}
    # )


if __name__ == "__main__":
    from core.scheduler import scheduler

    scheduler.start()
    main()

    while True:
        time.sleep(1)