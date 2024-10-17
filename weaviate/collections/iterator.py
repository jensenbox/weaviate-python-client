import time
from dataclasses import dataclass
from typing import Any, AsyncIterable, AsyncIterator, Generic, Iterable, Iterator, List, Optional
from uuid import UUID

from weaviate.collections.classes.grpc import METADATA
from weaviate.collections.classes.internal import (
    TReferences,
    TProperties,
    ReturnProperties,
    ReturnReferences,
    Object,
)
from weaviate.collections.queries.fetch_objects import _FetchObjectsQueryAsync
from weaviate.event_loop import _EventLoop
from weaviate.types import UUID as UUIDorStr

ITERATOR_CACHE_SIZE = 100


@dataclass
class _IteratorInputs(Generic[TProperties, TReferences]):
    include_vector: bool
    return_metadata: Optional[METADATA]
    return_properties: Optional[ReturnProperties[TProperties]]
    return_references: Optional[ReturnReferences[TReferences]]
    after: Optional[UUIDorStr]


def _parse_after(after: Optional[UUIDorStr]) -> Optional[UUID]:
    return after if after is None or isinstance(after, UUID) else UUID(after)


class _ObjectIterator(
    Generic[TProperties, TReferences],
    Iterable[Object[TProperties, TReferences]],
):
    def __init__(
        self,
        query: _FetchObjectsQueryAsync[Any, Any],
        inputs: _IteratorInputs[TProperties, TReferences],
        loop: _EventLoop,
        cache_size: Optional[int] = None,
    ) -> None:
        self.__query = query
        self.__inputs = inputs
        self.__loop = loop

        self.__iter_object_cache: List[Object[TProperties, TReferences]] = []
        self.__iter_object_last_uuid: Optional[UUID] = _parse_after(self.__inputs.after)
        self.__iter_cache_size = cache_size or ITERATOR_CACHE_SIZE

        self.__object_future = self.__loop.schedule(self.__fetch_objects)

        self.__time = 0.0

    def __iter__(
        self,
    ) -> Iterator[Object[TProperties, TReferences]]:
        self.__iter_object_cache = []
        self.__iter_object_last_uuid = _parse_after(self.__inputs.after)
        return self

    async def __fetch_objects(self) -> Any:
        return await self.__query.fetch_objects(
            limit=self.__iter_cache_size,
            after=self.__iter_object_last_uuid,
            include_vector=self.__inputs.include_vector,
            return_metadata=self.__inputs.return_metadata,
            return_properties=self.__inputs.return_properties,
            return_references=self.__inputs.return_references,
        )

    def __next__(self) -> Object[TProperties, TReferences]:
        if len(self.__iter_object_cache) == 0:
            start = time.time()
            res = self.__object_future.result()
            self.__time += time.time() - start
            self.__iter_object_cache = res.objects
            if len(res.objects) > 0:
                self.__iter_object_last_uuid = res.objects[-1].uuid

            self.__object_future = self.__loop.schedule(self.__fetch_objects)

            if len(self.__iter_object_cache) == 0:
                print(f"Time: {self.__time}")
                raise StopIteration

            assert (
                self.__iter_object_last_uuid is not None
            )  # if this is None the iterator will never stop

        ret_object = self.__iter_object_cache.pop(0)
        return ret_object  # pyright: ignore


class _ObjectAIterator(
    Generic[TProperties, TReferences],
    AsyncIterable[Object[TProperties, TReferences]],
):
    def __init__(
        self,
        query: _FetchObjectsQueryAsync[Any, Any],
        inputs: _IteratorInputs[TProperties, TReferences],
        cache_size: Optional[int] = None,
    ) -> None:
        self.__query = query
        self.__inputs = inputs

        self.__iter_object_cache: List[Object[TProperties, TReferences]] = []
        self.__iter_object_last_uuid: Optional[UUID] = _parse_after(self.__inputs.after)
        self.__iter_cache_size = cache_size or ITERATOR_CACHE_SIZE

    def __aiter__(
        self,
    ) -> AsyncIterator[Object[TProperties, TReferences]]:
        self.__iter_object_cache = []
        self.__iter_object_last_uuid = _parse_after(self.__inputs.after)
        return self

    async def __anext__(
        self,
    ) -> Object[TProperties, TReferences]:
        if len(self.__iter_object_cache) == 0:
            res = await self.__query.fetch_objects(
                limit=self.__iter_cache_size,
                after=self.__iter_object_last_uuid,
                include_vector=self.__inputs.include_vector,
                return_metadata=self.__inputs.return_metadata,
                return_properties=self.__inputs.return_properties,
                return_references=self.__inputs.return_references,
            )
            self.__iter_object_cache = res.objects  # type: ignore
            if len(self.__iter_object_cache) == 0:
                raise StopAsyncIteration

        ret_object = self.__iter_object_cache.pop(0)
        self.__iter_object_last_uuid = ret_object.uuid
        assert (
            self.__iter_object_last_uuid is not None
        )  # if this is None the iterator will never stop
        return ret_object  # pyright: ignore
