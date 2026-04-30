from sqlalchemy import BigInteger, Column, Double, Index, Integer, MetaData, PrimaryKeyConstraint, Table, Text

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

seuils_api_meteo_table = Table(
    "seuils_api_meteo",
    metadata,
    Column("id", Text, nullable=False),
    Column("timestamp_debut_validite", BigInteger, nullable=False),
    Column("seuil_neg2", Double, nullable=False),
    Column("seuil_neg1", Double, nullable=False),
    Column("seuil_pos1", Double, nullable=False),
    Column("seuil_pos2", Double, nullable=False),
    PrimaryKeyConstraint("id", "timestamp_debut_validite", name="seuils_api_meteo_pkey"),
)
