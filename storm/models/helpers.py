def create_db():
    from .fill import Fill
    from .order import Order
    from .arbitrage import Arbitrage
    from .base import Base
    from .engine import engine

    Base.metadata.create_all(engine)
