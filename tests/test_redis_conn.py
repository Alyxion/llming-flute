"""Unit tests for flute.redis_conn."""

from unittest.mock import MagicMock, patch

import pytest
from flute.redis_conn import connect_redis


class TestConnectRedis:
    @pytest.mark.asyncio
    async def test_regular_url(self):
        mock_client = MagicMock()
        with patch("flute.redis_conn.redis.asyncio.Redis.from_url", return_value=mock_client):
            result = await connect_redis("redis://localhost:6379/0", decode_responses=True)
        assert result is mock_client

    @pytest.mark.asyncio
    async def test_cluster_url_trailing_comma(self):
        mock_client = MagicMock()
        with patch(
            "flute.redis_conn.redis.asyncio.cluster.RedisCluster.from_url",
            return_value=mock_client,
        ) as mock_from_url:
            result = await connect_redis(
                "rediss://:pass@host:10000,", decode_responses=True
            )
        assert result is mock_client
        mock_from_url.assert_called_once_with(
            "rediss://:pass@host:10000", decode_responses=True
        )

    @pytest.mark.asyncio
    async def test_cluster_url_trailing_comma_with_whitespace(self):
        mock_client = MagicMock()
        with patch(
            "flute.redis_conn.redis.asyncio.cluster.RedisCluster.from_url",
            return_value=mock_client,
        ) as mock_from_url:
            result = await connect_redis(
                "rediss://:pass@host:10000,  ", decode_responses=True
            )
        assert result is mock_client
        mock_from_url.assert_called_once_with(
            "rediss://:pass@host:10000", decode_responses=True
        )
