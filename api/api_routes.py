from flask import jsonify, Blueprint

# 导入必要的模块
from core.scheduler import scheduler

# 创建蓝图
api_bp = Blueprint('api', __name__, url_prefix='/api')

@api_bp.route('/scheduler/jobs', methods=['GET'])
def get_scheduler_jobs():
    """获取调度器任务信息"""
    if scheduler.running:
        jobs = []
        for job in scheduler.get_jobs():
            jobs.append({
                'id': job.id,
                'name': job.name,
                'next_run_time': str(job.next_run_time) if job.next_run_time else None
            })
        return jsonify(jobs)
    return jsonify({'error': '调度器未运行'}), 503

# 导出蓝图和设置函数
__all__ = ['api_bp']