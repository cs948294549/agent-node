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
from task_implements.DeviceBaseinfoTask import DeviceBaseinfoTask

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
    
    logger.info("开始演示设备基础信息采集任务...")
    
    # 1. 初始化任务工厂并注册自定义任务类
    logger.info("注册设备基础信息采集任务类...")
    TaskFactory.register_task_class(DeviceBaseinfoTask.TASK_ID, DeviceBaseinfoTask)
    
    # 2. 创建任务管理器实例
    logger.info("创建任务管理器实例...")
    task_manager = TaskManager()
    
    # 3. 定义采集设备列表和配置
    task_config = {
        'iplist': ['1.1.1.1', '2.2.2.2'],  # 设备IP列表
        'community': 'public'  # SNMP团体字符串
    }
    
    # 4. 注册并调度设备信息采集任务
    logger.info(f"注册设备信息采集任务，配置: {task_config}")
    task_manager.register_task(
        task_id=DeviceBaseinfoTask.TASK_ID,
        config=task_config,
        schedule_type="interval",
        schedule_config={"seconds": 1800}  # 每30分钟执行一次
    )
    
    # 5. 手动执行一次任务以测试
    logger.info("手动执行一次任务以测试...")
    try:
        result = task_manager.execute_task_now(task_id=DeviceBaseinfoTask.TASK_ID)
        
        # 打印执行结果统计
        logger.info(f"任务执行结果: {result['status']}")
        if 'total_devices' in result:
            logger.info(f"总设备数: {result['total_devices']}")
            logger.info(f"成功数量: {result['success_count']}")
            logger.info(f"失败数量: {result['failed_count']}")
        
        # 打印详细的设备信息
        if 'data' in result:
            for device_info in result['data']:
                logger.info(f"设备 {device_info['ip']} 采集状态: {device_info['status']}")
                if device_info['status'] == 'success' and 'data' in device_info:
                    logger.info(f"  设备名称: {device_info['data'].get('sysName', 'unknown')}")
                    logger.info(f"  设备描述: {device_info['data'].get('sysDescr', 'unknown')[:100]}...")
                    logger.info(f"  设备位置: {device_info['data'].get('sysLocation', 'unknown')}")
                elif device_info['status'] == 'failed' and 'error' in device_info:
                    logger.info(f"  错误信息: {device_info['error']}")
    except Exception as e:
        logger.error(f"执行任务时发生错误: {str(e)}")
    
    logger.info("示例演示完成")


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