from dataclasses import dataclass
from typing import Optional, List, Dict, Any, Union, Set, TypeVar, Generic

import grpc

from weaviate.connect import Connection
from weaviate.util import BaseEnum
from weaviate.weaviate_classes import MetadataReturn
from weaviate.weaviate_types import UUID
from weaviate_grpc import weaviate_pb2


class HybridFusion(str, BaseEnum):
    RANKED = "rankedFusion"
    RELATIVE_SCORE = "relativeScoreFusion"


@dataclass
class Metadata:
    uuid: bool = False
    vector: bool = False
    creationTimeUnix: bool = False
    lastUpdateTimeUnix: bool = False
    distance: bool = False
    certainty: bool = False
    score: bool = False
    explainScore: bool = False


@dataclass
class LinkTo:
    link_on: str
    linked_class: str
    properties: "PROPERTIES"
    metadata: Metadata

    def __hash__(self):  # for set
        return hash(str(self))


PROPERTIES = Union[Set[Union[str, LinkTo]], str]


@dataclass
class RefProps:
    meta: Metadata
    refs: Dict[str, "RefProps"]


class GrpcBuilderBase:
    def __init__(
        self, connection: Connection, name: str, default_properties: Optional[Set[str]] = None
    ):
        self._connection: Connection = connection
        self._name: str = name

        if default_properties is not None:
            self._default_props: Set[str] = default_properties
        else:
            self._default_props = set()
        self._metadata: Optional[Metadata] = None

        self._limit: Optional[int] = None
        self._offset: Optional[int] = None
        self._autocut: Optional[int] = None
        self._after: Optional[UUID] = None

        self._hybrid_query: Optional[str] = None
        self._hybrid_alpha: Optional[float] = None
        self._hybrid_vector: Optional[List[float]] = None
        self._hybrid_properties: Optional[List[str]] = None
        self._hybrid_fusion_type: Optional[weaviate_pb2.HybridSearchParams.FusionType] = None

        self._bm25_query: Optional[str] = None
        self._bm25_properties: Optional[List[str]] = None

        self._near_vector_vec: Optional[List[float]] = None
        self._near_object_obj: Optional[UUID] = None
        self._near_certainty: Optional[float] = None
        self._near_distance: Optional[float] = None

    def add_return_values(self, props: Optional[PROPERTIES], metadata: Optional[Metadata]):
        if props is not None:
            if isinstance(props, set):
                self._default_props = self._default_props.union(props)

            else:
                self._default_props.add(props)
        self._metadata = metadata

    def _get(
        self,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        after: Optional[UUID] = None,
    ):
        self._limit = limit
        self._offset = offset
        self._after = after
        return self.__call()

    def _hybrid(
        self,
        query: str,
        alpha: Optional[float] = None,
        vector: Optional[List[float]] = None,
        properties: Optional[List[str]] = None,
        fusion_type: Optional[HybridFusion] = None,
        limit: Optional[int] = None,
        autocut: Optional[int] = None,
    ):
        self._hybrid_query = query
        self._hybrid_alpha = alpha
        self._hybrid_vector = vector
        self._hybrid_properties = properties
        self._hybrid_fusion_type = (
            weaviate_pb2.HybridSearchParams.FusionType.Value(fusion_type.name)
            if fusion_type is not None
            else None
        )
        self._limit = limit
        self._autocut = autocut

        return self.__call()

    def _bm25(
        self,
        query: str,
        properties: Optional[List[str]] = None,
        limit: Optional[int] = None,
        autocut: Optional[int] = None,
    ):
        self._bm25_query = query
        self._bm25_properties = properties
        self._limit = limit
        self._autocut = autocut

        return self.__call()

    def _near_vector(
        self,
        vector: List[float],
        certainty: Optional[float] = None,
        distance: Optional[float] = None,
        autocut: Optional[int] = None,
    ):
        self._near_vector_vec = vector
        self._near_certainty = certainty
        self._near_distance = distance
        self._autocut = autocut

        return self.__call()

    def _near_object(
        self,
        near_object: UUID,
        certainty: Optional[float] = None,
        distance: Optional[float] = None,
        autocut: Optional[int] = None,
    ):
        self._near_object_obj = near_object
        self._near_certainty = certainty
        self._near_distance = distance
        self._autocut = autocut
        return self.__call()

    def __call(self):
        metadata = ()
        access_token = self._connection.get_current_bearer_token()
        if len(access_token) > 0:
            metadata = (("authorization", access_token),)
        try:
            res, _ = self._connection.grpc_stub.Search.with_call(
                weaviate_pb2.SearchRequest(
                    class_name=self._name,
                    limit=self._limit,
                    offset=self._offset,
                    after=str(self._after) if self._after is not None else "",
                    autocut=self._autocut,
                    near_vector=weaviate_pb2.NearVectorParams(
                        vector=self._near_vector_vec,
                        certainty=self._near_certainty,
                        distance=self._near_distance,
                    )
                    if self._near_vector_vec is not None
                    else None,
                    near_object=weaviate_pb2.NearObjectParams(
                        id=str(self._near_object_obj),
                        certainty=self._near_certainty,
                        distance=self._near_distance,
                    )
                    if self._near_object_obj is not None
                    else None,
                    properties=self._convert_references_to_grpc(self._default_props),
                    additional_properties=self._metadata_to_grpc(self._metadata)
                    if self._metadata is not None
                    else None,
                    bm25_search=weaviate_pb2.BM25SearchParams(
                        properties=self._bm25_properties, query=self._bm25_query
                    )
                    if self._bm25_query is not None
                    else None,
                    hybrid_search=weaviate_pb2.HybridSearchParams(
                        properties=self._hybrid_properties,
                        query=self._hybrid_query,
                        alpha=self._hybrid_alpha,
                        vector=self._hybrid_vector,
                        fusion_type=self._hybrid_fusion_type,
                    )
                    if self._hybrid_query is not None
                    else None,
                ),
                metadata=metadata,
            )

            ref_props_meta = self._ref_props_return_meta(self._default_props)

            objects = []
            for result in res.results:
                obj = self._convert_references_to_grpc_result(result.properties, ref_props_meta)
                metadata = self._extract_metadata(result.additional_properties, self._metadata)
                objects.append((obj, metadata))

            return objects

        except grpc.RpcError as e:
            results = {"errors": [e.details()]}
            return results

    def _ref_props_return_meta(self, props: PROPERTIES) -> Dict[str, RefProps]:
        ref_props = {}
        for prop in props:
            if isinstance(prop, LinkTo):
                ref_props[prop.link_on] = RefProps(
                    meta=prop.metadata, refs=self._ref_props_return_meta(prop.properties)
                )
        return ref_props

    def _metadata_to_grpc(self, metadata: Metadata) -> weaviate_pb2.AdditionalProperties:
        return weaviate_pb2.AdditionalProperties(
            uuid=metadata.uuid,
            vector=metadata.vector,
            creationTimeUnix=metadata.creationTimeUnix,
            lastUpdateTimeUnix=metadata.lastUpdateTimeUnix,
            distance=metadata.distance,
            certainty=metadata.certainty,
            explainScore=metadata.explainScore,
            score=metadata.score,
        )

    def _convert_references_to_grpc_result(
        self, properties: "weaviate_pb2.ResultProperties", props: Dict[str, RefProps]
    ) -> Dict[str, Any]:
        result = {}
        for name, non_ref_prop in properties.non_ref_properties.items():
            result[name] = non_ref_prop

        for ref_prop in properties.ref_props:
            result[ref_prop.prop_name] = [
                (
                    self._convert_references_to_grpc_result(prop, props[ref_prop.prop_name].refs),
                    self._extract_metadata(prop.metadata, props[ref_prop.prop_name].meta),
                )
                for prop in ref_prop.properties
            ]

        return result

    def _convert_references_to_grpc(
        self, properties: Set[Union[LinkTo, str]]
    ) -> "weaviate_pb2.Properties":
        return weaviate_pb2.Properties(
            non_ref_properties=[prop for prop in properties if isinstance(prop, str)],
            ref_properties=[
                weaviate_pb2.RefProperties(
                    linked_class=prop.linked_class,
                    reference_property=prop.link_on,
                    linked_properties=self._convert_references_to_grpc(set(prop.properties)),
                    metadata=self._metadata_to_grpc(prop.metadata),
                )
                for prop in properties
                if isinstance(prop, LinkTo)
            ],
        )

    def _extract_metadata(
        self, props: "weaviate_pb2.ResultAdditionalProps", meta: Metadata
    ) -> MetadataReturn:
        if meta is None:
            return MetadataReturn()

        additional_props: Dict[str, Any] = {}
        if meta.uuid:
            additional_props["id"] = props.id
        if meta.vector:
            additional_props["vector"] = (
                [float(num) for num in props.vector] if len(props.vector) > 0 else None
            )
        if meta.distance:
            additional_props["distance"] = props.distance if props.distance_present else None
        if meta.certainty:
            additional_props["certainty"] = props.certainty if props.certainty_present else None
        if meta.creationTimeUnix:
            additional_props["creationTimeUnix"] = (
                str(props.creation_time_unix) if props.creation_time_unix_present else None
            )
        if meta.lastUpdateTimeUnix:
            additional_props["lastUpdateTimeUnix"] = (
                str(props.last_update_time_unix) if props.last_update_time_unix_present else None
            )
        if meta.score:
            additional_props["score"] = props.score if props.score_present else None
        if meta.explainScore:
            additional_props["explainScore"] = (
                props.explain_score if props.explain_score_present else None
            )
        return MetadataReturn(**additional_props)


GrpcBuilder = TypeVar("GrpcBuilder", bound=GrpcBuilderBase)


@dataclass
class ReturnValues(Generic[GrpcBuilder]):
    next_stage: GrpcBuilder

    def with_return_values(
        self,
        properties: Optional[PROPERTIES] = None,
        uuid: bool = False,
        vector: bool = False,
        creation_time_unix: bool = False,
        last_update_time_unix: bool = False,
        distance: bool = False,
        certainty: bool = False,
        score: bool = False,
        explain_score: bool = False,
    ) -> GrpcBuilder:
        additional_props = Metadata(
            uuid=uuid,
            vector=vector,
            creationTimeUnix=creation_time_unix,
            lastUpdateTimeUnix=last_update_time_unix,
            distance=distance,
            certainty=certainty,
            score=score,
            explainScore=explain_score,
        )
        self.next_stage.add_return_values(properties, additional_props)
        return self.next_stage
