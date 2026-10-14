"""JSON, pydantic and memoize through a Client (sync and async), plus NotImplemented paths."""

from __future__ import annotations

import asyncio
from collections.abc import Iterator
from dataclasses import dataclass
from typing import Optional

import pytest

from tinycache import Cache, Client, Server

pydantic = pytest.importorskip("pydantic")
BaseModel = pydantic.BaseModel
ValidationError = pydantic.ValidationError


class Address(BaseModel):
    city: str


class User(BaseModel):
    id: int
    name: str
    address: Optional[Address] = None


@dataclass
class Point:
    x: int
    y: int


@pytest.fixture
def server() -> Iterator[Server]:
    cache = Cache()
    srv = Server(cache, host="127.0.0.1", port=0)
    srv.start()
    try:
        yield srv
    finally:
        srv.stop()
        cache.close()


@pytest.fixture
def pc(server: Server) -> Iterator[Client]:
    client = Client(host="127.0.0.1", port=server.port, serializer="json")
    try:
        yield client
    finally:
        client.close()


def test_json_nested_round_trip(pc: Client) -> None:
    obj = {"a": [1, 2.5, None, True], "b": {"c": "héllo"}, "n": 2**70}
    pc.set_json("k", obj)
    assert pc.get_json("k") == obj


def test_json_hash_and_list_helpers(pc: Client) -> None:
    pc.hset_json("h", "f", {"x": 1})
    assert pc.hget_json("h", "f") == {"x": 1}
    assert pc.lpush_json("l", 1, 2) == 2
    assert pc.rpush_json("l", 3) == 3
    assert pc.lrange_json("l", 0, -1) == [2, 1, 3]


def test_json_nan_rejected_before_send(pc: Client) -> None:
    with pytest.raises(ValueError):
        pc.set_json("k", float("nan"))
    assert pc.get("k") is None


def test_corrupt_json_is_value_error(server: Server) -> None:
    # A raw (serializer="raw") client writes bytes unencoded, so the stored value is not JSON.
    with Client(host="127.0.0.1", port=server.port) as raw:
        raw.set("bad", b"{not json")
    with Client(host="127.0.0.1", port=server.port, serializer="json") as jc:
        with pytest.raises(ValueError):
            jc.get_json("bad")


def test_mget_json_and_mset_json(pc: Client) -> None:
    pc.mset_json({"a": {"v": 1}, "b": [1]})
    assert pc.mget_json(["a", "missing", "b"]) == [{"v": 1}, None, [1]]


def test_set_model_and_get_model(pc: Client) -> None:
    user = User(id=1, name="Ada", address=Address(city="London"))
    assert pc.set_model("u", user, ex=60) is True
    got = pc.get_model("u", User)
    assert isinstance(got, User)
    assert got == user
    assert pc.get_model("missing", User, default="d") == "d"


def test_list_of_models_round_trip(pc: Client) -> None:
    users = [User(id=1, name="a"), User(id=2, name="b")]
    pc.set_model("many", users)
    assert pc.get_model("many", list[User]) == users


def test_mget_model(pc: Client) -> None:
    pc.set_model("u1", User(id=1, name="a"))
    result = pc.mget_model(["u1", "nope"], User)
    assert result[0] == User(id=1, name="a")
    assert result[1] is None


def test_hash_model_fields(pc: Client) -> None:
    pc.hset_model("h", "f", Point(x=1, y=2))
    assert pc.hget_model("h", "f", Point) == Point(x=1, y=2)


def test_validation_error_on_mismatched_model(pc: Client) -> None:
    pc.set_json("u", {"id": "not-an-int"})
    with pytest.raises(ValidationError):
        pc.get_model("u", User)


def test_memoize_sync_returns_model(pc: Client) -> None:
    calls: list[int] = []

    @pc.memoize(ttl=60)
    def load(uid: int) -> User:
        calls.append(uid)
        return User(id=uid, name="n")

    first = load(5)
    second = load(5)
    assert isinstance(first, User) and isinstance(second, User)
    assert second == first
    assert calls == [5]


def test_memoize_async_returns_model(pc: Client) -> None:
    calls: list[int] = []

    @pc.memoize(ttl=60)
    async def load(uid: int) -> User:
        calls.append(uid)
        await asyncio.sleep(0)
        return User(id=uid, name="async")

    async def run() -> tuple[User, User]:
        return await load(9), await load(9)

    first, second = asyncio.run(run())
    assert isinstance(second, User)
    assert second == first
    assert calls == [9]


def test_memoize_caches_none_distinct_from_miss(pc: Client) -> None:
    calls: list[int] = []

    @pc.memoize(ttl=60)
    def maybe(n: int) -> Optional[User]:
        calls.append(n)
        return None

    assert maybe(1) is None
    assert maybe(1) is None
    assert calls == [1]


def test_memoize_unique_args_do_not_collide(pc: Client) -> None:
    @pc.memoize(ttl=60)
    def ident(n: int) -> dict:
        return {"n": n}

    assert [ident(i)["n"] for i in range(20)] == list(range(20))


def test_memoize_model_argument_key(pc: Client) -> None:
    calls: list[str] = []

    @pc.memoize(ttl=60)
    def show(u: User) -> str:
        calls.append(u.name)
        return u.name

    assert show(User(id=1, name="x")) == "x"
    assert show(User(id=1, name="x")) == "x"
    assert calls == ["x"]


def test_memoize_ttl_expires_through_client(pc: Client) -> None:
    import time

    calls: list[int] = []

    @pc.memoize(ttl=0.05)
    def ticking(n: int) -> int:
        calls.append(n)
        return n

    ticking(1)
    time.sleep(0.15)
    ticking(1)
    assert calls == [1, 1]


def test_used_memory_raises_not_implemented_when_server_lacks_info(pc: Client) -> None:
    # The server does not implement INFO yet, so used_memory degrades gracefully.
    with pytest.raises(NotImplementedError, match="INFO"):
        _ = pc.used_memory


def test_len_and_flush_still_work_without_info(pc: Client) -> None:
    pc.set("a", 1)
    assert len(pc) == 1
    pc.flush()
    assert len(pc) == 0


def test_pydantic_serializer_on_client(server: Server) -> None:
    from tinycache import PydanticSerializer

    with Client(host="127.0.0.1", port=server.port, serializer=PydanticSerializer(User)) as c:
        c.set("u", User(id=3, name="p"))
        assert c.get("u") == User(id=3, name="p")
