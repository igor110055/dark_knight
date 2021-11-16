from .base import Base
from sqlalchemy import Column, Integer, String, Numeric, ForeignKey


class Fill(Base):
    __tablename__ = 'fills'

    id = Column(Integer, primary_key=True)
    asset = Column(String)  # TODO: foreign key
    price = Column(Numeric)
    quantity = Column(Numeric)
    commission = Column(Numeric)
    commission_asset = Column(Numeric)
    order_id = Column(Integer, ForeignKey('orders.id'))
    # trade_id = Column(ForeignKey('trades'))
