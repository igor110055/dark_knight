from ..models.order import Order

class SaveArbitrageOrdersService:
    def __init__(self, session):
        self.session = session

    def execute(self, natural_order, first_synthetic_order, second_synthetic_order):
        for order in [natural_order, first_synthetic_order, second_synthetic_order]:
            order_record = Order(
                symbol=order['symbol'],
                external_id=order['orderId'],
                transacted_at=order['transactTime'],
                price=order['price'],
                quantity=order['origQty'],
                executed_quantity=order['executedQty'],
                cumulative_quote_quantity=order['cummulativeQuoteQty'],
                status=order['status'],
                type=order['type'],
                side=order['side']
            )
            self.session.add(order_record)
        
        self.session.commit()
        
        