from sqlalchemy import BigInteger, Column, Double, Index, Integer, MetaData, PrimaryKeyConstraint, Table

metadata = MetaData()

history_table = Table(
    "history",
    metadata,
    Column("itemid", Integer, nullable=False),
    Column("clock", BigInteger, nullable=False),
    Column("value", Double, nullable=False),
    Column("ns", Integer, nullable=False),
    PrimaryKeyConstraint("itemid", "clock", "ns", name="pk_history"),
    Index("ix_history_itemid_clock", "itemid", "clock"),
)
