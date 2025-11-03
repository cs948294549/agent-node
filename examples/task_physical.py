#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
设备基础信息采集任务示例

这个示例展示如何使用DeviceBaseinfoTask来采集网络设备的基础信息
"""

import logging
import time
from task_core.task_manager import TaskManager
from task_core.task_factory import TaskFactory
from task_implements.DevicePhysicalTask import DevicePhysicalTask

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('device_info_example')


def main():
    """
    主函数，演示如何使用DeviceBaseinfoTask
    """

    logger.info("开始演示设备接口信息采集任务...")

    # 1. 初始化任务工厂并注册自定义任务类
    logger.info("注册设备接口信息采集任务类...")
    TaskFactory.register_task_class(DevicePhysicalTask.TASK_ID, DevicePhysicalTask)


    # 2. 创建任务管理器实例
    logger.info("创建任务管理器实例...")
    task_manager = TaskManager()

    # 3. 定义采集设备列表和配置
    task_base_config = {
        "iplist": [
            "10.80.163.98", "10.80.163.99", "10.162.0.14","10.80.163.1",
            "10.80.163.2", "10.162.0.3",
        ],  # 设备IP列表
        'community': 'public',  # SNMP团体字符串
        # 'task_log_level': 'debug'
    }

    # 4. 注册并调度设备信息采集任务
    logger.info(f"注册设备信息采集任务，配置: {task_base_config}")
    task_manager.register_task(
        task_instance_id="1001",
        task_class_id=DevicePhysicalTask.TASK_ID,
        config=task_base_config,
        schedule_type="interval",
        schedule_config={"seconds": 10}  # 每60秒执行一次
    )



if __name__ == "__main__":
    from core.scheduler import scheduler

    # 启动调度器
    scheduler.start()
    logger.info("任务调度器已启动")

    try:
        main()
        logger.info("按Ctrl+C退出...")
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("收到停止信号，正在关闭...")
    finally:
        # 关闭调度器
        scheduler.shutdown()
        logger.info("任务调度器已关闭")