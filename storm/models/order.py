from typing import TYPE_CHECKING
from sqlalchemy import Column, Integer, Numeric, String
from sqlalchemy.orm import relationship

from .base import Base

if TYPE_CHECKING:
    from .fill import Fill


class Order(Base):
    __tablename__ = "orders"

    id = Column(Integer, primary_key=True)
    group_id = Column(String, index=True)
    symbol = Column(String)
    type = Column(String)  # TODO: enum
    side = Column(String)  # TODO: enum
    external_id = Column(Integer, index=True)
    price = Column(Numeric)
    quantity = Column(Numeric)
    executed_quantity = Column(Numeric)
    cumulative_quote_quantity = Column(Numeric)
    status = Column(String)  # TODO: enum
    transacted_at = Column(Integer)

    fills: "Fill" = relationship("Fill", back_populates="order", uselist=True)
