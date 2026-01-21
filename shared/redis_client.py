import redis
from functools import lru_cache
from .config import settings


@lru_cache(maxsize=1)
def get_redis_client() -> redis.Redis:
    """Get a singleton Redis client using URL from settings."""
    return redis.Redis.from_url(settings.redis_url, decode_responses=True)
