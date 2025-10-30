from flask import Blueprint, jsonify, request

# 创建任务相关的蓝图
tasks_bp = Blueprint('task_core', __name__, url_prefix='/api/task_core')

# 导入任务管理器
from task_core import task_manager
from task_core.task_factory import TaskFactory

@tasks_bp.route('/', methods=['GET'])
def get_all_tasks():
    """
    获取所有任务信息
    """
    tasks = task_manager.get_all_tasks()
    return jsonify(tasks)

@tasks_bp.route('/<task_id>', methods=['GET'])
def get_task(task_id):
    """
    获取指定任务的详细信息
    """
    task = task_manager.get_task_info(task_id)
    if task:
        return jsonify(task)
    return jsonify({'error': '任务不存在'}), 404

@tasks_bp.route('/', methods=['POST'])
def create_task():
    """
    创建新任务
    """
    try:
        data = request.json
        task_id = data.get('task_id')
        task_config = data.get('config', {})
        schedule_type = data.get('schedule_type', 'interval')
        schedule_config = data.get('schedule_config', {'seconds': 60})
        
        if not task_id:
            return jsonify({'error': 'task_id参数不能为空'}), 400
        
        task_id = task_manager.register_task(
            task_id=task_id,
            config=task_config,
            schedule_type=schedule_type,
            schedule_config=schedule_config
        )
        
        return jsonify({
            'success': True,
            'task_id': task_id,
            'message': '任务创建成功'
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@tasks_bp.route('/<task_id>', methods=['DELETE'])
def delete_task(task_id):
    """
    删除指定任务
    """
    try:
        success = task_manager.unregister_task(task_id)
        if success:
            return jsonify({
                'success': True,
                'message': '任务删除成功'
            })
        return jsonify({'error': '任务不存在'}), 404
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@tasks_bp.route('/<task_id>/execute', methods=['POST'])
def execute_task_now(task_id):
    """
    立即执行指定任务
    """
    try:
        result = task_manager.execute_task_now(task_id)
        return jsonify({
            'success': True,
            'result': result
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@tasks_bp.route('/<task_id>/schedule', methods=['PUT'])
def update_task_schedule(task_id):
    """
    更新任务调度配置
    """
    try:
        data = request.json
        schedule_type = data.get('schedule_type')
        schedule_config = data.get('schedule_config')
        
        if not schedule_type or not schedule_config:
            return jsonify({'error': 'schedule_type和schedule_config参数不能为空'}), 400
        
        success = task_manager.update_task_schedule(
            task_id=task_id,
            schedule_type=schedule_type,
            schedule_config=schedule_config
        )
        
        if success:
            return jsonify({
                'success': True,
                'message': '任务调度更新成功'
            })
        return jsonify({'error': '任务不存在'}), 404
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

# 导出蓝图
__all__ = ['tasks_bp']