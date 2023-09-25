from typing import (
    List,
    Optional,
    Union,
    Type,
)

from weaviate.collection.classes.filters import (
    _Filters,
)
from weaviate.collection.classes.grpc import (
    GroupBy,
    MetadataQuery,
    PROPERTIES,
    Move,
)
from weaviate.collection.classes.internal import (
    _Generative,
    _GenerativeReturn,
    _GroupBy,
    _GroupByReturn,
    _Object,
)
from weaviate.collection.classes.types import (
    Properties,
)
from weaviate.collection.queries.base import _Grpc


class _NearTextQuery(_Grpc):
    def near_text(
        self,
        query: Union[List[str], str],
        certainty: Optional[float] = None,
        distance: Optional[float] = None,
        move_to: Optional[Move] = None,
        move_away: Optional[Move] = None,
        limit: Optional[int] = None,
        auto_limit: Optional[int] = None,
        filters: Optional[_Filters] = None,
        return_metadata: Optional[MetadataQuery] = None,
        return_properties: Optional[Union[PROPERTIES, Type[Properties]]] = None,
    ) -> List[_Object[Properties]]:
        ret_properties, ret_type = self._parse_return_properties(return_properties)
        res = self._query().near_text(
            near_text=query,
            certainty=certainty,
            distance=distance,
            move_to=move_to,
            move_away=move_away,
            limit=limit,
            autocut=auto_limit,
            filters=filters,
            return_metadata=return_metadata,
            return_properties=ret_properties,
        )
        return self._result_to_query_return(res, ret_type)


class _NearTextGenerate(_Grpc):
    def near_text(
        self,
        query: Union[List[str], str],
        single_prompt: Optional[str] = None,
        grouped_task: Optional[str] = None,
        grouped_properties: Optional[List[str]] = None,
        certainty: Optional[float] = None,
        distance: Optional[float] = None,
        move_to: Optional[Move] = None,
        move_away: Optional[Move] = None,
        limit: Optional[int] = None,
        auto_limit: Optional[int] = None,
        filters: Optional[_Filters] = None,
        return_metadata: Optional[MetadataQuery] = None,
        return_properties: Optional[Union[PROPERTIES, Type[Properties]]] = None,
    ) -> _GenerativeReturn[Properties]:
        ret_properties, ret_type = self._parse_return_properties(return_properties)
        res = self._query().near_text(
            near_text=query,
            certainty=certainty,
            distance=distance,
            move_to=move_to,
            move_away=move_away,
            limit=limit,
            autocut=auto_limit,
            filters=filters,
            generative=_Generative(
                single=single_prompt,
                grouped=grouped_task,
                grouped_properties=grouped_properties,
            ),
            return_metadata=return_metadata,
            return_properties=ret_properties,
        )
        return self._result_to_generative_return(res, ret_type)


class _NearTextGroupBy(_Grpc):
    def near_text(
        self,
        query: Union[List[str], str],
        group_by: GroupBy,
        certainty: Optional[float] = None,
        distance: Optional[float] = None,
        move_to: Optional[Move] = None,
        move_away: Optional[Move] = None,
        limit: Optional[int] = None,
        auto_limit: Optional[int] = None,
        filters: Optional[_Filters] = None,
        return_metadata: Optional[MetadataQuery] = None,
        return_properties: Optional[Union[PROPERTIES, Type[Properties]]] = None,
    ) -> _GroupByReturn[Properties]:
        ret_properties, ret_type = self._parse_return_properties(return_properties)
        res = self._query().near_text(
            near_text=query,
            certainty=certainty,
            distance=distance,
            move_to=move_to,
            move_away=move_away,
            limit=limit,
            autocut=auto_limit,
            filters=filters,
            group_by=_GroupBy.from_input(group_by),
            return_metadata=return_metadata,
            return_properties=ret_properties,
        )
        return self._result_to_groupby_return(res, ret_type)
