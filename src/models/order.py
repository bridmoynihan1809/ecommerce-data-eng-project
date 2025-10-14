
from sqlalchemy import UUID, BigInteger, Boolean, Column, Date, DateTime, Float, Integer, MetaData, Numeric, String, Table, Text, func


tmp_metadata = MetaData()
manifest_metadata = MetaData()
order_metadata = MetaData()

tmp_order = Table(
    "tmp_order",
    tmp_metadata,
    Column("order_id", UUID(as_uuid=True), primary_key=True),
    Column("order_ts", DateTime),
    Column("customer_id", Text),
    Column("product_id", Text),
    Column("quantity", Integer),
    Column("price_per_unit", Numeric(10, 2)),
    Column("status", Text),
    Column("processed_at", DateTime, server_default=func.now()),
    schema="raw"
)

order = Table(
    "order",
    order_metadata,
    Column("order_id", UUID(as_uuid=True), primary_key=True),
    Column("order_ts", DateTime),
    Column("customer_id", Text),
    Column("product_id", Text),
    Column("quantity", Integer),
    Column("price_per_unit", Numeric(10, 2)),
    Column("status", Text),
    Column("processed_at", DateTime),
    schema="raw"
)

order_manifest = Table(
    "order_manifest",
    manifest_metadata,
    Column("file_name", String),
    Column("digest", String, primary_key=True),
    Column("file_size", BigInteger),
    Column("processed_at", BigInteger),
    schema="raw"
)