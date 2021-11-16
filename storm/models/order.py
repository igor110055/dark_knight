from .base import Base
from sqlalchemy import Column, Integer, String, Numeric, DateTime


class Order(Base):
    __tablename__ = 'orders'

    id = Column(Integer, primary_key=True)
    symbol = Column(String)
    type = Column(String)  # TODO: enum
    side = Column(String)  # TODO: enum
    external_id = Column(Integer)
    price = Column(Numeric)
    quantity = Column(Numeric)
    executed_quantity = Column(Numeric)
    cumulative_quote_quantity = Column(Numeric)
    status = Column(String)  # TODO: enum
    transacted_at = Column(Integer)
