"""
设备接口信息采集任务实现模块

负责采集设备的接口信息
"""
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, Any, List
from task_core.task_base import BaseTask
from collectors.device_interface_collector import DeviceInterfaceCollector

logger = logging.getLogger(__name__)


class DeviceInterfaceTask(BaseTask):
    """
    设备接口信息采集任务
    采集设备的接口信息，包括接口状态、流量等数据
    """

    TASK_ID = "device_interface"
    TASK_NAME = "设备接口信息采集"
    TASK_DESCRIPTION = "定时采集设备列表的接口信息，包含接口状态、流量、描述等信息"

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
            thread_pool_size = self.config.get('thread_pool_size', None)
            
            if not ip_list:
                logger.warning("配置中未提供IP列表")
                return {
                    'status': 'warning',
                    'message': '配置中未提供IP列表',
                    'data': []
                }
            
            logger.info(f"开始采集{len(ip_list)}个设备的接口信息")
            
            # 创建接口信息采集器实例
            collector = DeviceInterfaceCollector()
            
            # 设置接口相关OID的TTL值（默认300秒，可根据需要调整）
            collector.set_oid_ttl('1.3.6.1.2.1.2.2', 300)
            
            logger.info(f"设备接口采集配置 - 已设置接口信息OID的TTL值为300秒")
            
            # 使用线程池并行采集设备接口信息
            results: List[Dict[str, Any]] = []
            with ThreadPoolExecutor(max_workers=thread_pool_size) as executor:
                # 提交所有任务
                future_to_ip = {
                    executor.submit(self._collect_single_device, collector, ip, community): ip 
                    for ip in ip_list
                }
                
                # 收集结果
                for future in as_completed(future_to_ip):
                    ip = future_to_ip[future]
                    try:
                        result = future.result()
                        results.append(result)
                    except Exception as e:
                        logger.error(f"采集设备 {ip} 接口信息时发生异常: {str(e)}")
                        results.append({
                            'ip': ip,
                            'status': 'failed',
                            'error': str(e),
                            'data': {}
                        })
            
            # 统计采集结果
            success_count = sum(1 for r in results if r.get('status') == 'success')
            failed_count = len(results) - success_count
            
            logger.info(f"设备接口信息采集完成，成功: {success_count}, 失败: {failed_count}")
            
            # 返回采集结果
            return {
                'status': 'success',
                'total_devices': len(ip_list),
                'success_count': success_count,
                'failed_count': failed_count,
                'data': results
            }
            
        except Exception as e:
            logger.error(f"设备接口信息采集任务执行失败: {str(e)}")
            return {
                'status': 'failed',
                'error': str(e),
                'data': []
            }
    
    def _collect_single_device(self, collector: DeviceInterfaceCollector, ip: str, community: str) -> Dict[str, Any]:
        """
        采集单个设备的接口信息
        
        Args:
            collector: 设备接口信息采集器实例
            ip: 设备IP地址
            community: SNMP团体字符串
            
        Returns:
            Dict[str, Any]: 设备接口信息
        """
        logger.info(f"开始采集设备 {ip} 的接口信息")
        try:
            # 调用单个设备接口信息采集方法
            interface_info = collector.collect_data(ip, community)
            
            if interface_info:
                # 统计接口数量
                interface_count = len(interface_info.get('interfaces', {}))
                logger.info(f"设备 {ip} 接口信息采集完成，获取到 {interface_count} 个接口")
                return {
                    'ip': ip,
                    'status': 'success',
                    'data': interface_info
                }
            else:
                logger.warning(f"设备 {ip} 接口信息采集未获取到数据")
                return {
                    'ip': ip,
                    'status': 'failed',
                    'error': '未获取到接口数据',
                    'data': {}
                }
                
        except Exception as e:
            logger.error(f"采集设备 {ip} 接口信息失败: {str(e)}")
            return {
                'ip': ip,
                'status': 'failed',
                'error': str(e),
                'data': {}
            }