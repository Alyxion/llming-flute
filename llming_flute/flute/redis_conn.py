"""Redis connection helper with cluster support.

Convention: URLs ending with a trailing comma are treated as Redis Cluster
connections (e.g. ``rediss://:pass@host:10000,``).
"""

import redis.asyncio
import redis.asyncio.cluster


async def connect_redis(
    url: str, **kwargs
) -> redis.asyncio.Redis | redis.asyncio.cluster.RedisCluster:
    """Create an async Redis or RedisCluster client from a URL.

    If the URL ends with a comma, it is treated as a cluster URL.
    The trailing comma is stripped before connecting.
    """
    if url.rstrip().endswith(","):
        clean_url = url.rstrip().rstrip(",")
        return redis.asyncio.cluster.RedisCluster.from_url(clean_url, **kwargs)
    return redis.asyncio.Redis.from_url(url, **kwargs)
