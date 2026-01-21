import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from celery import Celery, signals
from shared.config import settings
from prometheus_client import Counter, Histogram, Gauge
import time

# Prometheus metrics for Celery
celery_task_total = Counter(
    'celery_task_total',
    'Total Celery tasks',
    ['task_name', 'status']
)

celery_task_duration_seconds = Histogram(
    'celery_task_duration_seconds',
    'Celery task duration in seconds',
    ['task_name'],
    buckets=(10, 30, 60, 120, 300, 600, 900, 1200)  # Up to 20 minutes
)

celery_worker_alive = Gauge(
    'celery_worker_alive',
    'Number of alive Celery workers'
)

celery_worker_heartbeat_timestamp = Gauge(
    'celery_worker_heartbeat_timestamp',
    'Timestamp of last worker heartbeat',
    ['worker_name']
)

celery_app = Celery(
    "video_service",
    broker=settings.rabbitmq_url,
    backend="rpc://"
)

celery_app.conf.update(
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    timezone='UTC',
    enable_utc=True,
)

# Signal handlers for Prometheus metrics
@signals.task_prerun.connect
def task_prerun_handler(sender=None, task_id=None, task=None, **kwargs):
    """Track task start time"""
    kwargs['start_time'] = time.time()

@signals.task_postrun.connect
def task_postrun_handler(sender=None, task_id=None, task=None, **kwargs):
    """Record successful task completion"""
    celery_task_total.labels(task_name=task.name, status='success').inc()
    if 'start_time' in kwargs:
        duration = time.time() - kwargs['start_time']
        celery_task_duration_seconds.labels(task_name=task.name).observe(duration)

@signals.task_failure.connect
def task_failure_handler(sender=None, task_id=None, exception=None, **kwargs):
    """Record task failure"""
    celery_task_total.labels(task_name=sender.name, status='failed').inc()

@signals.worker_ready.connect
def worker_ready_handler(sender=None, **kwargs):
    """Record worker ready"""
    celery_worker_alive.set(1)
    celery_worker_heartbeat_timestamp.labels(worker_name=sender.hostname).set(time.time())

@signals.worker_shutdown.connect
def worker_shutdown_handler(sender=None, **kwargs):
    """Reset metrics when worker stops"""
    celery_worker_alive.set(0)

@celery_app.task(bind=True)
def process_video_task(self, video_id: int):
    """Async task for video processing"""
    from .processor import process_video
    try:
        process_video(video_id)
        return {"status": "completed", "video_id": video_id}
    except Exception as e:
        self.retry(exc=e, countdown=60)
