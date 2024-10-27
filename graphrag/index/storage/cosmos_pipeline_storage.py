

import os
from time import time
from base64 import b64encode, b64decode
from re import Pattern
from collections.abc import Iterator
from typing import Any, Coroutine


from azure.cosmos import DatabaseProxy, PartitionKey, ContainerProxy
from azure.cosmos.cosmos_client import CosmosClient
from azure.identity import DefaultAzureCredential
import pandas as pd

from graphrag.logging.types import ProgressReporter

from .pipeline_storage import PipelineStorage

MAX_ITEM_LENGTH = 1024 * 1024 * 1.8

class CosmosDBStorage(PipelineStorage):
    _db: DatabaseProxy|None
    _container_clients: dict[str, ContainerProxy|None]
    _raw_client: ContainerProxy
    _memory_cache: dict[str, Any]    


    def __init__(self, database_name:str|None = None, connection_string:str|None = None, account_name:str|None = None, account_Key:str|None = None) -> None:
        self._db = None
        self._container_clients = dict[str, ContainerProxy|None]()
        self._connect(database_name, connection_string, account_name, account_Key)
        self._raw_client = self.get_client('_raw', True) # type: ignore
        self._memory_cache = dict[str, Any]()
    

    async def get(self, key: str, as_bytes: bool | None = None, encoding: str | None = None) -> dict[str, str | bytes | None] | None:
        if key in self._memory_cache:
            cache_item = self._memory_cache[key]
            if cache_item['expiry'] < time():
                del self._memory_cache[key]
            else:
                return cache_item['value']
        
        items = self._raw_client.query_items(f"SELECT * FROM c where c.id = '{key}'", partition_key=key, enable_cross_partition_query=False)
        if items is not None:
            item = next(iter(items), None)
            if item is None:
                return None
            
            value = item['value']
            if item['encoded']:
                if as_bytes:
                    value = b64decode(value)
                else:
                    value = b64decode(value).decode(encoding) if encoding is not None else b64decode(value).decode()

            cache_item = {'value': value, 'expiry': time() + 60}
            self._memory_cache[key] = cache_item
            return value # type: ignore

    async def set(self, key: str, value: str | bytes | None, encoding: str | None = None) -> None:
        is_encoded = False
        if value is not None:
            if isinstance(value, bytes):
                value = b64encode(value).decode(encoding) if encoding is not None else b64encode(value).decode()
                is_encoded = True

        self._raw_client.upsert_item({'id': key, 'value': value, 'encoded': is_encoded})
        cache_item = {'value': value, 'expiry': time() + 60}
        self._memory_cache[key] = cache_item

    async def has(self, key: str) -> bool:
        item = await self.get(key)
        return item is not None

    async def delete(self, key: str) -> None:
        self._raw_client.delete_item(key, key)
        if key in self._memory_cache:
            del self._memory_cache[key]

    async def clear(self) -> None:
        if self._db is not None:
            self._db.delete_container('_raw')
        self._memory_cache = dict[str, Any]()

    def keys(self) -> list[str]:
        keys = []
        items = self._raw_client.query_items("SELECT * FROM c", enable_cross_partition_query=True)
        for item in items:
            keys.append(item['id'])
        return keys

    def find(self, file_pattern: Pattern[str], base_dir: str | None = None, progress: ProgressReporter | None = None, file_filter: dict[str, Any] | None = None, max_count=-1) -> Iterator[tuple[str, dict[str, Any]]]:
        ## Iterate each key returning the key and the value if it matches the pattern
        items = self._raw_client.query_items(f"SELECT * FROM c where id like '{file_pattern}'", enable_cross_partition_query=True)
        return iter([(item['id'], item) for item in items])

    def child(self, name: str | None) -> "PipelineStorage":
        return self
    
    async def load_table(self, name: str) -> pd.DataFrame:
        dot_loc = name.find('.')
        table_name = '_' + name[0 : dot_loc] if dot_loc > 0 else '_' + name
        table_client = self.get_client(table_name, False)
        if table_client is None:
            return await super().load_table(name)    ## Assume it's saved in the _raw table
        
        items = table_client.query_items("SELECT * FROM c", enable_cross_partition_query=True)
        return pd.DataFrame(data=items)

    def get_client(self, container_name:str, create_if_not_exists:bool = True) -> ContainerProxy | None:
        client = None
        if self._db is None:
            raise ValueError("CosmosDBStorage not connected to a database")
        
        if container_name in self._container_clients:
            client = self._container_clients[container_name]
            if client is None: 
                client = self._db.get_container_client(container_name)
                self._container_clients[container_name] = client
        elif create_if_not_exists: 
            client = self._db.create_container(container_name, partition_key=PartitionKey(path='/id'))
            self._container_clients[container_name] = client

        return client

    def _connect(self, database_name:str|None = None, connection_string:str|None = None, account_name:str|None = None, account_Key:str|None = None) -> None:
        if self._db is not None:
            return
        
        ## Load CosmosDB Client
        cosmos_database = database_name or os.environ.get("GRAPH_DATABASE_ID") or os.environ.get("COSMOS_DATABASE_ID")
        if cosmos_database is None or len(cosmos_database) == 0:
            raise ValueError("Cosmos Database Name is required")
        
        cosmos_connection_str = connection_string or os.environ.get("COSMOS_CONNECTION_STRING")
        client = None
        if cosmos_connection_str is not None and len(cosmos_connection_str) > 0:
            client = CosmosClient.from_connection_string(cosmos_connection_str)
        else: 
            # .documents.azure.com:443/
            cosmos_account_host = account_name or os.environ.get("COSMOS_ACCOUNT_NAME") or os.environ.get("COSMOS_ACCOUNT_HOST")
            if cosmos_account_host is None or len(cosmos_account_host) == 0:
                raise ValueError("Cosmos Account Host is required (when not using a connection string)")
            if not cosmos_account_host.startswith("https://"):
                cosmos_account_host = f"https://{cosmos_account_host}"
            if not '.' in cosmos_account_host:
                cosmos_account_host += ".documents.azure.com:443/"

            cosmos_account_key = account_Key or os.environ.get("COSMOS_ACCOUNT_KEY")
            if cosmos_account_key is not None and len(cosmos_account_key) > 0:
                client = CosmosClient(cosmos_account_host, {'masterKey': cosmos_account_key})
            else: 
                client = CosmosClient(url=cosmos_account_host, credential=DefaultAzureCredential())

        self._db = client.get_database_client(cosmos_database)

        containers = self._db.list_containers()
        container_map = dict[str, ContainerProxy|None ]()
        for container_props in containers:
            if 'id' in container_props:
                container_map[container_props['id']] = None
        self._container_clients = container_map

def create_cosmos_storage(database_name:str|None = None, connection_string:str|None = None, account_name:str|None = None, account_Key:str|None = None) -> CosmosDBStorage:
    return CosmosDBStorage(database_name, connection_string, account_name, account_Key)