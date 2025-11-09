from dataclasses import dataclass
from sqlalchemy import MetaData, Table


@dataclass
class ProcessorConfig:
    """
    Configuration for a generic data processor.

    This dataclass encapsulates all entity-specific details needed to process
    different types of data using a single generic processor implementation.

    Attributes:
        entity_name: Name of the entity used for logging and query naming.
        tmp_table: Temporary staging table for incoming data.
        manifest_table: Table tracking processed files and their metadata.
        target_table: Main target table where data is ultimately stored.
        primary_key_column: Name of the primary key column for upsert operations.
        tmp_metadata: Metadata object for the temporary table.
        manifest_metadata: Metadata object for the manifest table.
    """
    entity_name: str
    tmp_table: Table
    manifest_table: Table
    target_table: Table
    primary_key_column: str
    tmp_metadata: MetaData
    manifest_metadata: MetaData
