from typing import TYPE_CHECKING
from sqlalchemy import Column, ForeignKey, Integer, Numeric, String
from sqlalchemy.orm import relationship

from .base import Base

if TYPE_CHECKING:
    from .order import Order


class Fill(Base):
    __tablename__ = "fills"

    id = Column(Integer, primary_key=True)
    symbol = Column(String)  # TODO: foreign key
    price = Column(Numeric)
    quantity = Column(Numeric)
    commission = Column(Numeric)
    commission_asset = Column(String)
    order_id = Column(Integer, ForeignKey("orders.id"))
    order: "Order" = relationship("Order", back_populates="fills", uselist=False)
    trade_id = Column(Integer)
