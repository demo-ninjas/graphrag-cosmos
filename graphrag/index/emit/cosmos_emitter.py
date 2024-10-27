
from typing import cast
import pandas as pd
import json
import numpy
from graphrag.index.storage import PipelineStorage, CosmosDBStorage
from graphrag.index.typing import ErrorHandlerFn

from azure.cosmos import ContainerProxy

from .table_emitter import TableEmitter

class CosmosEmitter(TableEmitter):
    """CosmosEmitter protocol for emitting tables to a destination."""

    def __init__(self, storage: PipelineStorage, on_error: ErrorHandlerFn) -> None:
        self.storage = storage
        self.on_error = on_error
        self._storage = cast(CosmosDBStorage, storage)

        
    async def emit(self, name: str, data: pd.DataFrame) -> None:
        """Emit a dataframe to CosmosDB."""
        client:ContainerProxy = self._storage.get_client('_' + name, True)  # type: ignore
        ## Emit the data
        for i, row in data.iterrows():
            if 'id' not in row:
                row['id'] = str(i)
            row_data = row.to_dict()
            for key, value in row_data.items():
                if isinstance(value, pd.Timestamp):
                    row_data[key] = value.to_pydatetime()
                elif isinstance(value, pd.Series):
                    row_data[key] = value.to_list()
                elif isinstance(value, numpy.ndarray):
                    row_data[key] = value.tolist()
                
            print(f"[{name}] Emitting Row: {row_data['id']}: {len(json.dumps(row_data))} bytes")
            client.upsert_item(row_data)
