from uuid import uuid4

from ..models.fill import Fill
from ..models.order import Order


class SaveArbitrageOrdersService:
    def __init__(self, session):
        self.session = session

    def execute(self, natural_order, first_synthetic_order, second_synthetic_order):
        group_id = str(uuid4())
        for order in [natural_order, first_synthetic_order, second_synthetic_order]:
            order_record = Order(
                group_id=group_id,
                symbol=order["symbol"],
                external_id=order["orderId"],
                transacted_at=order["transactTime"],
                price=order["price"],
                quantity=order["origQty"],
                executed_quantity=order["executedQty"],
                cumulative_quote_quantity=order["cummulativeQuoteQty"],
                status=order["status"],
                type=order["type"],
                side=order["side"],
            )
            self.session.add(order_record)

            for fill in order["fills"]:
                fill_record = Fill(
                    symbol=order["symbol"],
                    price=fill["price"],
                    quantity=fill["qty"],
                    commission=fill["commission"],
                    commission_asset=fill["commissionAsset"],
                    trade_id=fill["tradeId"],
                    order=order_record,
                )
                self.session.add(fill_record)

        self.session.commit()
