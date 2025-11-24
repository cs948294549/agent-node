from flask import Blueprint, request
from api.api_response import APIResponse

# 创建任务相关的蓝图，前缀设置为/task/
tasks_bp = Blueprint('tasks', __name__, url_prefix='/task')

# 导入任务管理器
from task_core.task_manager import task_manager

@tasks_bp.route('/', methods=['GET'])
def get_all_tasks():
    """
    获取所有任务信息
    """
    tasks = task_manager.get_all_tasks()
    return APIResponse.success(data=tasks)


@tasks_bp.route('/<task_instance_id>', methods=['GET'])
def get_task(task_instance_id):
    """
    获取指定任务的详细信息
    """
    task = task_manager.get_task_info(task_instance_id)
    if task:
        return APIResponse.success(data=task)
    return APIResponse.not_found_error(message="任务不存在")

@tasks_bp.route('/', methods=['POST'])
def create_task():
    """
    创建新任务（任务发布功能）
    """
    try:
        data = request.json
        
        # 获取必要的参数
        task_instance_id = data.get('task_instance_id')  # 任务实例ID
        task_class_id = data.get('task_class_id')        # 任务类ID
        task_config = data.get('config', {})             # 任务配置
        schedule_type = data.get('schedule_type', 'interval')  # 调度类型
        schedule_config = data.get('schedule_config', {'seconds': 60})  # 调度配置
        
        # 参数验证
        if not task_instance_id:
            return APIResponse.param_error(message="task_instance_id参数不能为空")
        
        if not task_class_id:
            return APIResponse.param_error(message="task_class_id参数不能为空")
        
        # 发布任务
        registered_task_id = task_manager.register_task(
            task_instance_id=task_instance_id,
            task_class_id=task_class_id,
            config=task_config,
            schedule_type=schedule_type,
            schedule_config=schedule_config
        )

        return APIResponse.success(message="任务发布成功",data=registered_task_id)
    except Exception as e:
        return APIResponse.server_error(message=str(e))

@tasks_bp.route('/<task_instance_id>', methods=['POST'])
def delete_task(task_instance_id):
    """
    删除指定任务
    """
    try:
        success = task_manager.unregister_task(task_instance_id)
        if success:
            return APIResponse.success(message="任务删除成功")
        return APIResponse.not_found_error(message="任务不存在")
    except Exception as e:
        return APIResponse.server_error(message=str(e))

@tasks_bp.route('/<task_instance_id>/execute', methods=['POST'])
def execute_task_now(task_instance_id):
    """
    立即执行指定任务
    """
    try:
        result = task_manager.execute_task_now(task_instance_id)
        if result is None:
            return APIResponse.not_found_error(message="任务不存在")
        return APIResponse.success(data=result)
    except Exception as e:
        return APIResponse.server_error(message=str(e))

@tasks_bp.route('/<task_instance_id>/schedule', methods=['POST'])
def update_task_schedule(task_instance_id):
    """
    更新任务调度配置
    """
    try:
        data = request.json
        schedule_type = data.get('schedule_type')
        schedule_config = data.get('schedule_config')
        
        if not schedule_type or not schedule_config:
            return APIResponse.param_error(message="schedule_type和schedule_config参数不能为空")
        
        success = task_manager.update_task_schedule(
            task_instance_id=task_instance_id,
            schedule_type=schedule_type,
            schedule_config=schedule_config
        )
        
        if success:
            return APIResponse.success(message="任务调度更新成功", data={
                'success': True,
                'message': '任务调度更新成功'
            })
        return APIResponse.not_found_error(message="任务不存在")
    except Exception as e:
        return APIResponse.server_error(message=str(e))

@tasks_bp.route('/available', methods=['GET'])
def get_available_tasks():
    """
    获取所有可用的任务类型
    """
    try:
        available_tasks = task_manager.get_all_available_tasks()
        return APIResponse.success(data=available_tasks)
    except Exception as e:
        return APIResponse.server_error(message=str(e))

@tasks_bp.route('/<task_instance_id>/config', methods=['POST'])
def update_task_config(task_instance_id):
    """
    更新任务配置
    """
    try:
        data = request.json
        config = data.get('config', {})
        
        success = task_manager.update_task_config(task_instance_id, config)
        if success:
            return APIResponse.success(message="任务配置更新成功", data=True)
        return APIResponse.not_found_error(message="任务不存在")
    except Exception as e:
        return APIResponse.server_error(message=str(e))

# 导出蓝图
__all__ = ['tasks_bp']