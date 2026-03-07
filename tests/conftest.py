"""Shared fixtures for unit tests."""

import pytest

from flute.client import SessionClient


class FakeRedis:
    """In-memory Redis mock for unit testing."""

    def __init__(self):
        self.store: dict[str, str] = {}
        self.hashes: dict[str, dict[str, str]] = {}
        self.lists: dict[str, list[str]] = {}

    def set(self, key, val, ex=None):
        self.store[key] = val if isinstance(val, str) else str(val)

    def get(self, key):
        return self.store.get(key)

    def delete(self, key):
        self.store.pop(key, None)
        self.hashes.pop(key, None)
        self.lists.pop(key, None)

    def rpush(self, key, val):
        self.lists.setdefault(key, []).append(val)

    def blpop(self, key, timeout=0):
        lst = self.lists.get(key, [])
        if lst:
            return (key, lst.pop(0))
        return None

    def hset(self, key, field, val):
        self.hashes.setdefault(key, {})[field] = val

    def hgetall(self, key):
        return dict(self.hashes.get(key, {}))

    def expire(self, key, seconds):
        pass

    def ping(self):
        return True

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
    return c
