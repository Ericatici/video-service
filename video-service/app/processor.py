import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import ffmpeg
import os
from shared.models import Video
from shared.database import SessionLocal
import pika
import json
from shared.config import settings
from shared.redis_client import get_redis_client

UPLOAD_DIR = "uploads"
PROCESSED_DIR = "processed"

os.makedirs(UPLOAD_DIR, exist_ok=True)
os.makedirs(PROCESSED_DIR, exist_ok=True)

redis_client = get_redis_client()


def _invalidate_status_cache(video):
    try:
        username = video.user.username if video and video.user else None
        if username:
            redis_client.delete(f"videos:status:{username}")
    except Exception as e:
        print(f"Cache invalidate failed: {e}")

def process_video(video_id: int):
    """Process video and send event"""
    print(f"Starting processing for video {video_id}")
    db = SessionLocal()
    video = db.query(Video).filter(Video.id == video_id).first()
    if not video:
        print(f"Video {video_id} not found")
        db.close()
        return
    try:
        input_path = os.path.join(UPLOAD_DIR, video.filename)
        output_path = os.path.join(PROCESSED_DIR, f"{video_id}_converted.mp4")
        print(f"Converting {input_path} to {output_path}")
        ffmpeg.input(input_path).output(output_path).run()
        video.status = "completed"
        db.commit()
        _invalidate_status_cache(video)
        print(f"Video {video_id} completed")
        # Publish event to notification service
        publish_video_event("video.completed", {"video_id": video_id, "user_id": video.user_id})
    except Exception as e:
        video.status = "error"
        print(f"Error processing video {video_id}: {e}")
        db.commit()
        _invalidate_status_cache(video)
        # Publish error event
        publish_video_event("video.error", {"video_id": video_id, "user_id": video.user_id, "error": str(e)})
    finally:
        db.close()

def publish_video_event(event_type: str, data: dict):
    """Publish event to RabbitMQ"""
    try:
        connection = pika.BlockingConnection(pika.URLParameters(settings.rabbitmq_url))
        channel = connection.channel()
        channel.exchange_declare(exchange='video_events', exchange_type='topic', durable=True)
        channel.basic_publish(
            exchange='video_events',
            routing_key=event_type,
            body=json.dumps(data),
            properties=pika.BasicProperties(delivery_mode=2)
        )
        connection.close()
        print(f"Event published: {event_type}")
    except Exception as e:
        print(f"Error publishing event: {e}")
