"""Shared fixtures for unit tests."""

import asyncio

import pytest
from flute.client import SessionClient


class FakePubSub:
    """Minimal async pub/sub mock for unit testing."""

    def __init__(self, fake_redis):
        self._redis = fake_redis
        self._channels = set()
        self._messages = []

    async def subscribe(self, channel):
        self._channels.add(channel)
        self._redis._subscribers.setdefault(channel, []).append(self)

    async def get_message(self, timeout=0):
        if self._messages:
            return self._messages.pop(0)
        await asyncio.sleep(0.01)
        return None

    async def unsubscribe(self, channel):
        self._channels.discard(channel)
        subs = self._redis._subscribers.get(channel, [])
        if self in subs:
            subs.remove(self)

    async def close(self):
        for ch in list(self._channels):
            await self.unsubscribe(ch)


class FakeRedis:
    """In-memory async Redis mock for unit testing."""

    def __init__(self):
        self.store: dict[str, str] = {}
        self.hashes: dict[str, dict[str, str]] = {}
        self.lists: dict[str, list[str]] = {}
        self.published: dict[str, list[str]] = {}
        self._subscribers: dict[str, list[FakePubSub]] = {}
        self._expires: dict[str, int] = {}

    async def set(self, key, val, ex=None):
        self.store[key] = val if isinstance(val, str) else str(val)

    async def get(self, key):
        return self.store.get(key)

    async def delete(self, key):
        self.store.pop(key, None)
        self.hashes.pop(key, None)
        self.lists.pop(key, None)

    async def rpush(self, key, val):
        self.lists.setdefault(key, []).append(val)

    async def ltrim(self, key, start, stop):
        if key in self.lists:
            lst = self.lists[key]
            length = len(lst)
            if start < 0:
                start = max(0, length + start)
            if stop < 0:
                stop = length + stop
            self.lists[key] = lst[start:stop + 1]

    async def lrange(self, key, start, stop):
        lst = self.lists.get(key, [])
        length = len(lst)
        if start < 0:
            start = max(0, length + start)
        if stop < 0:
            stop = length + stop
        return lst[start:stop + 1]

    async def blpop(self, key, timeout=0):
        lst = self.lists.get(key, [])
        if lst:
            return (key, lst.pop(0))
        await asyncio.sleep(0.01)
        return None

    async def hset(self, key, field, val):
        self.hashes.setdefault(key, {})[field] = val

    async def hget(self, key, field):
        return self.hashes.get(key, {}).get(field)

    async def hgetall(self, key):
        return dict(self.hashes.get(key, {}))

    async def hdel(self, key, field):
        if key in self.hashes:
            self.hashes[key].pop(field, None)

    async def incrbyfloat(self, key, amount):
        current = float(self.store.get(key, "0"))
        new_val = current + float(amount)
        self.store[key] = str(new_val)
        return new_val

    async def ttl(self, key):
        if key not in self.store and key not in self.hashes and key not in self.lists:
            return -2
        return self._expires.get(key, -1)

    async def expire(self, key, seconds):
        if key in self.store or key in self.hashes or key in self.lists:
            self._expires[key] = seconds

    async def ping(self):
        return True

    async def publish(self, channel, message):
        self.published.setdefault(channel, []).append(message)
        for sub in self._subscribers.get(channel, []):
            sub._messages.append({"type": "message", "channel": channel, "data": message})

    def pubsub(self):
        return FakePubSub(self)

    async def aclose(self):
        pass

    @classmethod
    def from_url(cls, url, **kwargs):
        return cls()


@pytest.fixture
def fake_redis():
    return FakeRedis()


@pytest.fixture
def client(fake_redis):
    """SessionClient backed by FakeRedis."""
    c = SessionClient.__new__(SessionClient)
    c.r = fake_redis
    c._redis_url = "redis://fake"
    return c
