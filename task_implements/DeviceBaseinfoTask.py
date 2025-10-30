"""
具体任务实现模块

包含各种具体任务的实现类
"""
import logging
import socket
import requests
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, Any, List
from task_core.task_base import BaseTask
from config import Config
# 导入设备信息采集模块
from collectors.device_info_collector import DeviceBaseInfoCollector

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
            thread_pool_size = self.config.get('thread_pool_size', None)
            
            if not ip_list:
                logger.warning("配置中未提供IP列表")
                return {
                    'status': 'warning',
                    'message': '配置中未提供IP列表',
                    'data': []
                }
            
            logger.info(f"开始采集{len(ip_list)}个设备的基础信息")
            

            
            # 创建采集器实例
            collector = DeviceBaseInfoCollector(default_community=community)
            
            # 设置不同OID的TTL值
            # 系统基本信息设置较短的TTL（例如60秒）
            collector.set_oid_ttl('1.3.6.1.2.1.1', 60)
            # 接口信息设置较长的TTL（例如300秒）
            collector.set_oid_ttl('1.3.6.1.2.1.2.2', 300)
            
            logger.info(f"设备信息采集配置 - 已设置不同OID的TTL值")
            
            # 使用线程池并行采集设备信息
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
                        logger.error(f"采集设备 {ip} 时发生异常: {str(e)}")
                        results.append({
                            'ip': ip,
                            'status': 'failed',
                            'error': str(e),
                            'data': {}
                        })
            
            # 统计采集结果
            success_count = sum(1 for r in results if r['status'] == 'success')
            failed_count = len(results) - success_count
            
            logger.info(f"设备信息采集完成，成功: {success_count}, 失败: {failed_count}")
            
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
    
    def _collect_single_device(self, collector: DeviceBaseInfoCollector, ip: str, community: str) -> Dict[str, Any]:
        """
        采集单个设备的基础信息
        
        Args:
            collector: 设备信息采集器实例
            ip: 设备IP地址
            community: SNMP团体字符串
            
        Returns:
            Dict[str, Any]: 设备信息
        """
        logger.info(f"开始采集设备 {ip} 的基础信息")
        try:
            # 调用单个设备采集方法
            device_info = collector.collect_data(ip, community)
            logger.info(f"设备 {ip} 信息采集完成，状态: {device_info['status']}")
            return device_info
        except Exception as e:
            logger.error(f"采集设备 {ip} 失败: {str(e)}")
            return {
                'ip': ip,
                'status': 'failed',
                'error': str(e),
                'data': {}
            }
