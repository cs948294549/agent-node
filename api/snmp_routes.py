from flask import Blueprint, jsonify, request

# 创建SNMP相关的蓝图，前缀设置为/snmp
snmp_bp = Blueprint('snmp', __name__, url_prefix='/snmp')

# 导入SNMP相关模块
from function_snmp.snmp_collector import snmp_get, snmp_walk
from collectors.device_info_collector import global_collector
from config import Config


@snmp_bp.route('/snmpget', methods=['POST'])
def snmp_agent_get():
    """
    SNMP GET基础调用接口
    """
    try:
        data = request.json
        ip = data.get('ip')
        community = data.get('community', Config.common_community)
        oid = data.get('oid')
        coding = data.get('coding', 'utf-8')
        
        if not ip or not oid:
            return jsonify({'error': 'ip和oid参数不能为空'}), 400
        
        result = snmp_get(ip, community, oid)
        
        return jsonify({
            'success': True,
            'result': result,
            'ip': ip,
            'oid': oid
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@snmp_bp.route('/snmpwalk', methods=['POST'])
def snmp_agent_walk():
    """
    SNMP WALK基础调用接口
    """
    try:
        data = request.json
        ip = data.get('ip')
        community = data.get('community', Config.common_community)
        oids = data.get('oids')
        bulk_size = data.get('bulk_size', 10)
        coding = data.get('coding', 'utf-8')
        
        if not ip or not oids:
            return jsonify({'error': 'ip和oids参数不能为空'}), 400
        
        # 确保oids是列表格式
        if isinstance(oids, str):
            oids = [oids]
        
        result = snmp_walk(ip, community, oids)
        
        return jsonify({
            'success': True,
            'result': result,
            'ip': ip,
            'oids': oids
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@snmp_bp.route('/device-info', methods=['POST'])
def snmp_collector_device_info():
    """
    设备信息采集接口
    """
    try:
        data = request.json
        ip = data.get('ip')
        community = data.get('community', Config.common_community)
        if not ip:
            return jsonify({'error': 'ip参数不能为空'}), 400
        
        # 使用全局设备信息采集器
        device_info = global_collector.collect_data(ip, community)
        
        return jsonify({
            'success': True,
            'data': device_info
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

# 导出蓝图
__all__ = ['snmp_bp']