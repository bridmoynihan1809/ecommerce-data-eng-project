
from sqlalchemy import UUID, BigInteger, Column, DateTime, MetaData, String, Table, Text, func

tmp_metadata = MetaData()
manifest_metadata = MetaData()
customer_metadata = MetaData()

tmp_customer = Table(
    "tmp_customer",
    tmp_metadata,
    Column("customer_id", UUID(as_uuid=True), primary_key=True),
    Column("first_name", Text),
    Column("last_name", Text),
    Column("email", Text),
    Column("processed_at", DateTime, server_default=func.now()),
    schema="raw"
)

customer = Table(
    "customer",
    customer_metadata,
    Column("customer_id", UUID(as_uuid=True), primary_key=True),
    Column("first_name", Text),
    Column("last_name", Text),
    Column("email", Text),
    Column("processed_at", DateTime),
    schema="raw"
)

customer_manifest = Table(
    "customer_manifest",
    manifest_metadata,
    Column("file_name", String),
    Column("digest", String, primary_key=True),
    Column("file_size", BigInteger),
    Column("processed_at", BigInteger),
    schema="raw"
)
