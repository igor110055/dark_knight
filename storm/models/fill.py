from .base import Base
from sqlalchemy import Column, Integer, String, Numeric, ForeignKey
from sqlalchemy.orm import relationship


class Fill(Base):
    __tablename__ = 'fills'

    id = Column(Integer, primary_key=True)
    symbol = Column(String)  # TODO: foreign key
    price = Column(Numeric)
    quantity = Column(Numeric)
    commission = Column(Numeric)
    commission_asset = Column(String)
    order_id = Column(Integer, ForeignKey('orders.id'))
    order = relationship('Order', back_populates='fills')
    trade_id = Column(Integer)
