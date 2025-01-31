# Copyright (c) 2024 Microsoft Corporation.
# Licensed under the MIT License

"""Table Emitter Factories."""

from graphrag.index.storage import PipelineStorage
from graphrag.index.typing import ErrorHandlerFn

from .csv_table_emitter import CSVTableEmitter
from .json_table_emitter import JsonTableEmitter
from .parquet_table_emitter import ParquetTableEmitter
from .table_emitter import TableEmitter
from .cosmos_emitter import CosmosEmitter
from .types import TableEmitterType


def create_table_emitter(
    emitter_type: TableEmitterType, storage: PipelineStorage, on_error: ErrorHandlerFn
) -> TableEmitter:
    """Create a table emitter based on the specified type."""
    match emitter_type:
        case TableEmitterType.Json:
            return JsonTableEmitter(storage)
        case TableEmitterType.Parquet:
            return ParquetTableEmitter(storage, on_error)
        case TableEmitterType.CSV:
            return CSVTableEmitter(storage)
        case TableEmitterType.Cosmos:
            return CosmosEmitter(storage, on_error)
        case _:
            msg = f"Unsupported table emitter type: {emitter_type}"
            raise ValueError(msg)


def create_table_emitters(
    emitter_types: list[TableEmitterType],
    storage: PipelineStorage,
    on_error: ErrorHandlerFn,
) -> list[TableEmitter]:
    """Create a list of table emitters based on the specified types."""
    return [
        create_table_emitter(emitter_type, storage, on_error)
        for emitter_type in emitter_types
    ]
