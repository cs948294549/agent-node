"""
具体任务实现模块

包含各种具体任务的实现类
"""
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, Any, List
from task_core.task_base import BaseTask
# 导入设备信息采集模块
from collectors.device_info_collector import collect_device_base_info
from utils.worker import execAllFunctions


logger = logging.getLogger(__name__)


class DeviceBaseinfoTask(BaseTask):
    """
    设备基础信息采集任务，采集完成后发回kafka进行数据存储
    """

    TASK_ID = "device_baseinfo"
    TASK_NAME = "设备基础信息采集"
    TASK_DESCRIPTION = "定时采集设备列表的基础信息，包含设备名、描述、版本、location等信息"

    def __init__(self, config=None):
        super().__init__(config)

    def execute(self) -> Dict[str, Any]:
        """
        获取配置中的iplist列表，对列表内的IP进行采集基础信息
        Returns:
            Dict[str, Any]: 采集执行结果
        """
        try:
            # 获取配置中的IP列表和SNMP参数
            ip_list = self.config.get('iplist', [])
            community = self.config.get('community', 'public')
            # 获取线程池大小配置，默认使用CPU核心数的2倍
            thread_pool_size = self.config.get('thread_pool_size', 8)
            
            if not ip_list:
                logger.warning("配置中未提供IP列表")
                return {
                    'status': 'warning',
                    'message': '配置中未提供IP列表',
                    'data': []
                }
            
            logger.info(f"开始采集{len(ip_list)}个设备的基础信息")
            
            # 使用线程池并行采集设备信息
            tasks = []
            for ip in ip_list:
                task = {"name": ip, "func": collect_device_base_info, "args": (ip,community), "kwargs": {}}
                tasks.append(task)
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
            logger.error(f"设备信息采集任务执行失败: {str(e)}")
            return {
                'status': 'failed',
                'error': str(e),
                'data': []
            }


if __name__ == '__main__':
    import json
    a = DeviceBaseinfoTask(config={"iplist": ["10.80.163.98", "10.80.163.99", "10.162.0.14"]})
    res = a.execute()
    print(json.dumps(res, indent=4))