import asyncio
import logging
from redis.asyncio import Redis
from redis.asyncio.connection import ConnectionPool

from app.config import settings

logger = logging.getLogger(__name__)

_redis_client: Redis | None = None
_connection_pool: ConnectionPool | None = None


async def get_redis() -> Redis:
    global _redis_client, _connection_pool

    if _redis_client is None:
        try:
            # Create connection pool with retry logic
            _connection_pool = ConnectionPool(
                host=settings.redis_host,
                port=settings.redis_port,
                password=settings.redis_password or None,
                # Keep raw bytes so binary cache values (e.g., JPEG previews) are safe.
                decode_responses=False,
                socket_timeout=5,
                socket_connect_timeout=5,
                retry_on_timeout=True,
                socket_keepalive=True,
                socket_keepalive_options={
                    1: 1,  # TCP_KEEPIDLE
                    2: 1,  # TCP_KEEPINTVL
                    3: 3,  # TCP_KEEPCNT
                } if hasattr(asyncio, 'socket') else {},
                max_connections=10,
            )
            
            _redis_client = Redis(connection_pool=_connection_pool)
            
            # Test connection
            await _redis_client.ping()
            logger.info(f"✓ Redis connected: {settings.redis_host}:{settings.redis_port}")
        except Exception as e:
            logger.error(f"✗ Redis connection failed: {e}")
            _redis_client = None
            _connection_pool = None
            raise

    return _redis_client


async def close_redis():
    global _redis_client, _connection_pool
    
    if _redis_client:
        try:
            await _redis_client.close()
            logger.info("Redis connection closed")
        except Exception as e:
            logger.error(f"Error closing Redis: {e}")
    
    _redis_client = None
    _connection_pool = None
