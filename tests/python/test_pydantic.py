"""Pydantic support (PLAN section 4b). Skipped where pydantic is not installed."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Optional

import pytest

from tinycache import Cache

from helpers import SHORT_PX, wait_for_expiry

pydantic = pytest.importorskip("pydantic")
BaseModel = pydantic.BaseModel
ValidationError = pydantic.ValidationError


class Address(BaseModel):
    city: str
    zip_code: str


class User(BaseModel):
    id: int
    name: str
    address: Optional[Address] = None


@dataclass
class Point:
    x: int
    y: int


@pytest.fixture
def pc() -> Cache:
    c = Cache(serializer="json")
    try:
        yield c
    finally:
        c.close()


def test_model_round_trip(cache: Cache) -> None:
    u = User(id=1, name="Ada")
    cache.set_model("u", u)
    got = cache.get_model("u", User)
    assert isinstance(got, User)
    assert got == u


def test_nested_model_round_trip(cache: Cache) -> None:
    u = User(id=2, name="Bob", address=Address(city="Paris", zip_code="75001"))
    cache.set_model("u", u)
    got = cache.get_model("u", User)
    assert isinstance(got.address, Address)
    assert got.address.city == "Paris"


def test_list_of_models(cache: Cache) -> None:
    users = [User(id=1, name="a"), User(id=2, name="b")]
    cache.set_model("many", users)
    got = cache.get_model("many", list[User])
    assert all(isinstance(u, User) for u in got)
    assert [u.id for u in got] == [1, 2]


def test_dict_of_models(cache: Cache) -> None:
    data = {"a": User(id=1, name="a")}
    cache.set_model("dict", data)
    got = cache.get_model("dict", dict[str, User])
    assert isinstance(got["a"], User)


def test_optional_model_none_and_value(cache: Cache) -> None:
    cache.set_model("present", User(id=3, name="c"))
    assert isinstance(cache.get_model("present", Optional[User]), User)
    cache.set_model("nothing", None)
    assert cache.get_model("nothing", Optional[User]) is None


def test_dataclass_round_trip(cache: Cache) -> None:
    cache.set_model("p", Point(x=1, y=2))
    assert cache.get_model("p", Point) == Point(x=1, y=2)


def test_get_model_missing_returns_default(cache: Cache) -> None:
    assert cache.get_model("missing", User) is None
    assert cache.get_model("missing", User, default="dflt") == "dflt"


def test_validation_error_on_mismatched_data(cache: Cache) -> None:
    cache.set_model("u", {"id": "not-an-int", "name": 5})
    with pytest.raises(ValidationError):
        cache.get_model("u", User)


def test_validation_error_is_value_error(cache: Cache) -> None:
    cache.set("corrupt", b"{garbage")
    with pytest.raises(ValueError):
        cache.get_model("corrupt", User)


def test_model_ttl_expires(cache: Cache) -> None:
    cache.set_model("t", User(id=1, name="x"), px=SHORT_PX)
    assert cache.get_model("t", User) is not None
    wait_for_expiry()
    assert cache.get_model("t", User, default="gone") == "gone"


def test_set_model_nx(cache: Cache) -> None:
    assert cache.set_model("k", User(id=1, name="a"), nx=True) is True
    assert cache.set_model("k", User(id=2, name="b"), nx=True) is False


def test_mget_model(cache: Cache) -> None:
    cache.set_model("a", User(id=1, name="a"))
    cache.set_model("b", User(id=2, name="b"))
    got = cache.mget_model(["a", "missing", "b"], User)
    assert [u.id if u else None for u in got] == [1, None, 2]


def test_hash_model_fields(cache: Cache) -> None:
    cache.hset_model("users", "1", User(id=1, name="Ada"))
    got = cache.hget_model("users", "1", User)
    assert got == User(id=1, name="Ada")
    assert cache.hget_model("users", "2", User, default="none") == "none"


def test_hash_model_validation_error(cache: Cache) -> None:
    cache.hset("users", "bad", b"{}")
    with pytest.raises(ValidationError):
        cache.hget_model("users", "bad", User)


def test_memoize_annotation_returns_model_instance(pc: Cache) -> None:
    calls: list[int] = []

    @pc.memoize(ttl=60)
    def load(uid: int) -> User:
        calls.append(uid)
        return User(id=uid, name="n")

    first = load(7)
    second = load(7)
    assert isinstance(first, User)
    assert isinstance(second, User)
    assert second == first
    assert calls == [7]


def test_memoize_model_override(pc: Cache) -> None:
    @pc.memoize(ttl=60, model=User)
    def load(uid: int):  # no annotation on purpose
        return User(id=uid, name="override")

    load(1)
    hit = load(1)
    assert isinstance(hit, User)
    assert hit.name == "override"


def test_memoize_list_of_models(pc: Cache) -> None:
    @pc.memoize(ttl=60)
    def batch(n: int) -> list[User]:
        return [User(id=i, name=str(i)) for i in range(n)]

    batch(2)
    hit = batch(2)
    assert all(isinstance(u, User) for u in hit)
    assert [u.id for u in hit] == [0, 1]


def test_memoize_optional_model(pc: Cache) -> None:
    @pc.memoize(ttl=60)
    def find(uid: int) -> Optional[User]:
        return User(id=uid, name="x") if uid else None

    assert isinstance(find(5), User)
    assert isinstance(find(5), User)


def test_memoize_with_model_argument_uses_dump_json_key(pc: Cache) -> None:
    calls: list[object] = []

    @pc.memoize(ttl=60)
    def handle(u: User) -> dict:
        calls.append(u)
        return {"id": u.id}

    handle(User(id=1, name="a"))
    handle(User(id=1, name="a"))
    handle(User(id=1, name="b"))
    assert len(calls) == 2


def test_memoize_async_function(pc: Cache) -> None:
    calls: list[int] = []

    @pc.memoize(ttl=60)
    async def fetch(uid: int) -> User:
        calls.append(uid)
        await asyncio.sleep(0)
        return User(id=uid, name="async")

    async def run() -> tuple[User, User]:
        return await fetch(9), await fetch(9)

    first, second = asyncio.run(run())
    assert isinstance(first, User)
    assert isinstance(second, User)
    assert calls == [9]


def test_memoize_caches_none_distinct_from_miss(pc: Cache) -> None:
    calls: list[int] = []

    @pc.memoize(ttl=60)
    def maybe(x: int) -> Optional[User]:
        calls.append(x)
        return None

    assert maybe(1) is None
    assert maybe(1) is None
    assert calls == [1]


def test_memoize_ttl_expiry_recomputes(pc: Cache) -> None:
    calls: list[int] = []

    @pc.memoize(ttl=0.05)
    def load(x: int) -> User:
        calls.append(x)
        return User(id=x, name="t")

    load(1)
    wait_for_expiry()
    load(1)
    assert calls == [1, 1]


def test_pydantic_serializer_round_trip(cache: Cache) -> None:
    from tinycache import PydanticSerializer

    ser = PydanticSerializer(User)
    data = ser.dumps(User(id=4, name="ser"))
    assert ser.loads(data) == User(id=4, name="ser")
