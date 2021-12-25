from typing import TYPE_CHECKING
from sqlalchemy import Column, Integer, Numeric, String
from sqlalchemy.orm import relationship

from .base import Base

if TYPE_CHECKING:
    from .order import Order


class Arbitrage(Base):
    __tablename__ = "arbitrages"

    id = Column(Integer, primary_key=True)
    # buy natural sell synthetic / buy synthetic sell natural
    type = Column(String)
    group_id = Column(String, index=True)
    cost_asset = Column(String)
    cost_quantity = Column(Numeric)
    proceed_asset = Column(String)
    proceed_quantity = Column(Numeric)
    fee_asset = Column(String)
    fee_quantity = Column(Numeric)

    orders: "Order" = relationship(
        "Order", primaryjoin="foreign(Arbitrage.group_id)==Order.group_id", uselist=True
    )
