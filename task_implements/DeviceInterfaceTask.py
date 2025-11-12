"""
设备接口信息采集任务实现模块

负责采集设备的接口信息
"""
import logging
from typing import Dict, Any, List
from task_core.task_base import BaseTask
from collectors.device_interface_collector import collect_interface_basic_info, collect_interface_status, collect_interface_metric
from utils.worker import execAllFunctions

logger = logging.getLogger(__name__)


class DeviceInterfaceBaseTask(BaseTask):
    """
    设备接口信息采集任务
    采集设备的接口信息，包括接口状态、流量等数据
    """

    TASK_ID = "collect_interface_basic_info"
    TASK_NAME = "设备接口基础信息采集"
    TASK_DESCRIPTION = "定时采集设备列表的接口信息，包含接口状态、描述等信息"

    def __init__(self, config=None):
        super().__init__(config)

    def execute(self) -> Dict[str, Any]:
        """
        获取配置中的iplist列表，对列表内的IP进行接口信息采集
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
            
            logger.info(f"开始采集{len(ip_list)}个设备的接口信息, 配置信息{str(self.config)}")

            # 使用线程池并行采集设备信息
            tasks = []
            for ip in ip_list:
                task = {"name": ip, "func": collect_interface_basic_info, "args": (ip,community), "kwargs": {}}
                tasks.append(task)
            results = execAllFunctions(tasks, max_workers=thread_pool_size)

            # 统计采集结果
            success_count = sum(1 for r in results.values() if r.get('status') == 'ok')
            failed_count = len(results) - success_count

            logger.info(f"设备基础信息采集完成，成功: {success_count}, 失败: {failed_count}")

            logger.info("采集结果:{}".format(str(results)))


            collect_data = list(results.values())

            # 返回采集结果
            return {
                'status': 'success',
                'total_devices': len(ip_list),
                'success_count': success_count,
                'failed_count': failed_count,
                'data': collect_data
            }
            
        except Exception as e:
            logger.error(f"设备接口基础信息采集任务执行失败: {str(e)}")
            return {
                'status': 'failed',
                'error': str(e),
                'data': []
            }


class DeviceInterfaceStatusTask(BaseTask):
    """
    设备接口状态采集任务
    采集设备的接口状态
    """

    TASK_ID = "collect_interface_status"
    TASK_NAME = "设备接口状态信息采集"
    TASK_DESCRIPTION = "定时采集设备列表的接口状态"

    def __init__(self, config=None):
        super().__init__(config)

    def execute(self) -> Dict[str, Any]:
        """
        获取配置中的iplist列表，对列表内的IP进行接口信息采集
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

            logger.info(f"开始采集{len(ip_list)}个设备的接口信息, 配置信息{str(self.config)}")

            # 使用线程池并行采集设备端口状态
            tasks = []
            for ip in ip_list:
                task = {"name": ip, "func": collect_interface_status, "args": (ip,community), "kwargs": {}}
                tasks.append(task)
            results = execAllFunctions(tasks, max_workers=thread_pool_size)

            # 统计采集结果
            success_count = sum(1 for r in results.values() if r.get('status') == 'ok')
            failed_count = len(results) - success_count

            logger.info(f"设备接口状态采集完成，成功: {success_count}, 失败: {failed_count}")

            logger.info("采集结果:{}".format(str(results)))


            # 返回采集结果
            return {
                'status': 'success',
                'total_devices': len(ip_list),
                'success_count': success_count,
                'failed_count': failed_count,
                'data': results
            }

        except Exception as e:
            logger.error(f"设备接口状态采集任务执行失败: {str(e)}")
            return {
                'status': 'failed',
                'error': str(e),
                'data': []
            }


class DeviceInterfaceMetricTask(BaseTask):
    """
    设备接口指标采集任务
    采集设备的接口指标
    """

    TASK_ID = "collect_interface_metric"
    TASK_NAME = "设备接口指标信息采集"
    TASK_DESCRIPTION = "定时采集设备列表的接口状态"

    def __init__(self, config=None):
        super().__init__(config)

    def execute(self) -> Dict[str, Any]:
        """
        获取配置中的iplist列表，对列表内的IP进行接口信息采集
        Returns:
            Dict[str, Any]: 采集执行结果
        """
        try:
            # 获取配置中的IP列表和SNMP参数
            ip_list = self.config.get('iplist', [])
            community = self.config.get('community', 'public')
            metric_name = self.config.get('metric_name', None)
            if not metric_name:
                logger.warning(f"接口指标采集设置失败，未设置metric_name")
                return {
                    'status': 'warning',
                    'message': '配置中未提供接口指标',
                    'data': []
                }
            # 获取线程池大小配置，默认使用CPU核心数的2倍
            thread_pool_size = self.config.get('thread_pool_size', 8)

            if not ip_list:
                logger.warning("配置中未提供IP列表")
                return {
                    'status': 'warning',
                    'message': '配置中未提供IP列表',
                    'data': []
                }

            logger.info(f"开始采集{len(ip_list)}个设备的接口信息, 配置信息{str(self.config)}")
            # 使用线程池并行采集设备端口状态
            tasks = []
            for ip in ip_list:
                task = {"name": ip, "func": collect_interface_metric, "args": (ip,community,metric_name), "kwargs": {}}
                tasks.append(task)
            results = execAllFunctions(tasks, max_workers=thread_pool_size)

            # 统计采集结果
            success_count = sum(1 for r in results.values() if r.get('status') == 'ok')
            failed_count = len(results) - success_count

            logger.info(f"设备指标信息采集完成，成功: {success_count}, 失败: {failed_count}")

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
            logger.error(f"设备接口指标信息采集任务执行失败: {str(e)}")
            return {
                'status': 'failed',
                'error': str(e),
                'data': []
            }