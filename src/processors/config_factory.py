from processors.processor_config import ProcessorConfig
from models.customer import tmp_customer, customer_manifest, customer, tmp_metadata as customer_tmp_metadata, manifest_metadata as customer_manifest_metadata
from models.order import tmp_order, order_manifest, order, tmp_metadata as order_tmp_metadata, manifest_metadata as order_manifest_metadata


def create_customer_config() -> ProcessorConfig:
    """
    Creates a ProcessorConfig for customer data processing.

    Returns:
        ProcessorConfig configured for customer entity with appropriate tables,
        metadata, and primary key column name.
    """
    return ProcessorConfig(
        entity_name="customer",
        tmp_table=tmp_customer,
        manifest_table=customer_manifest,
        target_table=customer,
        primary_key_column="customer_id",
        tmp_metadata=customer_tmp_metadata,
        manifest_metadata=customer_manifest_metadata
    )


def create_order_config() -> ProcessorConfig:
    """
    Creates a ProcessorConfig for order data processing.

    Returns:
        ProcessorConfig configured for order entity with appropriate tables,
        metadata, and primary key column name.
    """
    return ProcessorConfig(
        entity_name="order",
        tmp_table=tmp_order,
        manifest_table=order_manifest,
        target_table=order,
        primary_key_column="order_id",
        tmp_metadata=order_tmp_metadata,
        manifest_metadata=order_manifest_metadata
    )
