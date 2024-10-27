# Copyright (c) 2024 Microsoft Corporation.
# Licensed under the MIT License

"""A module containing 'FilePipelineCache' model."""

import json
from typing import Any, cast

from graphrag.index.storage import PipelineStorage, CosmosDBStorage

from .pipeline_cache import PipelineCache


class CosmosPipelineCache(PipelineCache):
    """File pipeline cache class definition."""

    _storage: CosmosDBStorage
    _encoding: str
    _memory_cache: dict[str, Any]

    def __init__(self, storage: PipelineStorage, encoding="utf-8"):
        """Init method definition."""
        self._storage = cast(CosmosDBStorage, storage)
        self._encoding = encoding
        self._cache_client = self._storage.get_client('_cache')
        self._memory_cache = {}

    async def get(self, key: str) -> Any:
        if key in self._memory_cache:
            return self._memory_cache[key]
        
        items = self._cache_client.query_items(f"SELECT * FROM c where c.id = '{key}'", partition_key=key, enable_cross_partition_query=False)
        if items is not None:
            item = next(iter(items), None)
            if item is None:
                return None
        
            value = item['value']
            self._memory_cache[key] = value
            return value
        return None

    async def set(self, key: str, value: Any, debug_data: dict | None = None) -> None:
        self._cache_client.upsert_item({'id': key, 'value': value})

    async def has(self, key: str) -> bool:
        return self.get(key) != None

    async def delete(self, key: str) -> None:
        self._cache_client.delete_item(key, key)

    async def clear(self) -> None:
        if self._storage._db is not None:
            self._storage._db.delete_container('_cache')

    def child(self, name: str) -> PipelineCache:
        return self