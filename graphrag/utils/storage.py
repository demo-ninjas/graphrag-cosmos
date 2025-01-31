# Copyright (c) 2024 Microsoft Corporation.
# Licensed under the MIT License

"""Storage functions for the GraphRAG run module."""

import logging
from io import BytesIO
from pathlib import Path

import pandas as pd

from graphrag.index.config.storage import (
    PipelineFileStorageConfig,
    PipelineStorageConfigTypes,
)
from graphrag.index.storage import load_storage
from graphrag.index.storage.pipeline_storage import PipelineStorage

log = logging.getLogger(__name__)


def _create_storage(
    config: PipelineStorageConfigTypes | None, root_dir: Path
) -> PipelineStorage:
    """Create the storage for the pipeline.

    Parameters
    ----------
    config : PipelineStorageConfigTypes
        The storage configuration.
    root_dir : str
        The root directory.

    Returns
    -------
    PipelineStorage
        The pipeline storage.
    """
    return load_storage(
        config or PipelineFileStorageConfig(base_dir=str(root_dir / "output"))
    )


async def _load_table_from_storage(name: str, storage: PipelineStorage) -> pd.DataFrame:
    try:
        log.info("read table from storage: %s", name)
        return await storage.load_table(name)
    except Exception:
        log.exception("error loading table from storage: %s", name)
        raise
