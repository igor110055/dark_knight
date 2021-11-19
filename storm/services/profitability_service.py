from ..models import Order, Arbitrage
from ..models.session import get_session
from ..utils import get_assets
from collections import defaultdict
from decimal import Decimal
from sqlalchemy.orm import joinedload

session = get_session()

ZERO = Decimal('0')


def record_arbitrages():
    existing_arbitrage_group_ids = session.query(Arbitrage.group_id)
    pending_order_group_ids = session.query(Order.group_id).distinct(
        Order.group_id).filter(Order.group_id.not_in(existing_arbitrage_group_ids))

    for group_id, in pending_order_group_ids:
        arbitrage_data = calculate_return(group_id)
        arbitrage = Arbitrage(
            group_id=group_id,
            cost_asset=arbitrage_data['cost']['asset'],
            cost_quantity=arbitrage_data['cost']['quantity'],
            proceed_asset=arbitrage_data['proceed']['asset'],
            proceed_quantity=arbitrage_data['proceed']['quantity'],
            fee_asset=arbitrage_data['fee']['asset'],
            fee_quantity=arbitrage_data['fee']['quantity'],
        )
        session.add(arbitrage)

    session.commit()


def calculate_return(order_group_id):
    orders = session.query(Order).options(joinedload(
        Order.fills)).filter_by(group_id=order_group_id).all()

    asset_quantities = defaultdict(Decimal)
    prices = {}

    cost = ZERO

    for order in orders:
        base, quote = get_assets(order.symbol)

        if order.side == 'BUY':
            if quote == 'USDT':  # TODO: add other stable coins, consider in base
                cost = max(cost, order.cumulative_quote_quantity)
            asset_quantities[base] += order.quantity
            asset_quantities[quote] -= order.cumulative_quote_quantity
        else:
            asset_quantities[base] -= order.quantity
            asset_quantities[quote] += order.cumulative_quote_quantity

        for fill in order.fills:
            asset_quantities[fill.commission_asset] -= fill.commission
            prices[fill.symbol] = fill.price

    if 'USDT' in asset_quantities:  # TODO: or other stable coins
        ret = {
            'cost': dict(asset='USDT', quantity=cost),
            'proceed': dict(asset='USDT', quantity=ZERO),
            'fee': dict(asset=None, quantity=ZERO)
        }
        ret['proceed']['quantity'] = asset_quantities.pop('USDT')

        for asset, quantity in asset_quantities.items():
            if (symbol := f"{asset}USDT") in prices:
                ret['proceed']['quantity'] += quantity * prices[symbol]
            elif (symbol := f"USDT{asset}") in prices:
                ret['proceed']['quantity'] += quantity / prices[symbol]
            else:
                ret['fee']['asset'] = asset
                ret['fee']['quantity'] += quantity

        return ret

    else:
        raise Exception('Stable coin not supported')
