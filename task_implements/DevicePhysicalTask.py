"""
设备物理模块信息采集任务

负责定时执行设备物理模块信息的采集工作。
"""
import logging
from typing import Dict, List, Any

from task_core.task_base import BaseTask
from collectors.device_physical_collector import collect_physical_module_info
from utils.worker import execAllFunctions

logger = logging.getLogger(__name__)

class DevicePhysicalTask(BaseTask):
    """
    设备物理模块信息采集任务
    采集设备的物理模块信息，如设备类型、描述、位置等
    """
    
    TASK_ID = "device_physical"
    TASK_NAME = "设备物理模块信息采集"
    TASK_DESCRIPTION = "定时采集设备列表的物理模块信息，包含设备类型、描述、位置等信息"

    def __init__(self, config=None):
        super().__init__(config)
        # 日志级别设置已在基类中处理

    def execute(self) -> Dict[str, Any]:
        """
        获取配置中的iplist列表，对列表内的IP进行物理模块信息采集
        Returns:
            Dict[str, Any]: 采集执行结果
        """
        try:
            # 获取配置中的IP列表和SNMP参数
            ip_list = self.config.get('iplist', [])
            community = self.config.get('community', 'public')
            # 获取线程池大小配置，默认使用8
            thread_pool_size = self.config.get('thread_pool_size', 8)
            
            logger.info(f"开始采集{len(ip_list)}个设备的物理模块信息")
            
            if not ip_list:
                logger.warning("配置中未提供IP列表")
                return {
                    'status': 'warning',
                    'message': '配置中未提供IP列表',
                    'data': []
                }
            
            # 创建任务列表
            tasks = []
            for ip in ip_list:
                task = {"name": ip, "func": collect_physical_module_info, "args": (ip, community), "kwargs": {}}
                tasks.append(task)
            
            # 并行执行任务
            results = execAllFunctions(tasks, max_workers=thread_pool_size)
            
            # 统计采集结果
            success_count = sum(1 for r in results.values() if r.get('status') == 'ok')
            failed_count = len(results) - success_count
            
            logger.info(f"设备信息采集完成，成功: {success_count}, 失败: {failed_count}")

            logger.debug("采集结果:{}".format(str(results)))
            # 返回采集结果
            return {
                'status': 'success',
                'total_devices': len(ip_list),
                'success_count': success_count,
                'failed_count': failed_count,
                'data': results
            }
            
        except Exception as e:
                logger.error(f"设备物理模块信息采集任务执行失败: {str(e)}")
                return {
                    'status': 'failed',
                    'error': str(e),
                    'data': []
                }

if __name__ == '__main__':
    import json
    # 测试代码
    
    # 示例1: 使用默认日志级别（通常是INFO）
    print("===== 使用默认日志级别（通常是INFO）=====")
    task = DevicePhysicalTask(config={'iplist': ['10.80.163.98', '10.80.163.99'], 'community': 'public'})
    res = task.execute()
    print(json.dumps(res, indent=4))
    
    # 示例2: 设置日志级别为DEBUG
    print("\n===== 设置日志级别为DEBUG =====")
    # 通过task_log_level配置项设置DEBUG级别日志
    task_debug = DevicePhysicalTask(config={
        'iplist': ['10.80.163.98'], 
        'community': 'public',
        'task_log_level': 'debug'  # 在这里设置日志级别为debug
    })
    # 执行任务，此时会输出debug级别的日志
    res_debug = task_debug.execute()
    print(json.dumps(res_debug, indent=4))