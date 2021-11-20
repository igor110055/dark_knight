#%%
import os
from ccxt import binance
from dotenv import load_dotenv
from order_engine import OrderEngine

binance_client = binance(
    {"apiKey": os.getenv("API_KEY"), "secret": os.getenv("SECRET_KEY")}
)

engine = OrderEngine(binance_client)
# %%
from storm.models import Order
from storm.models.session import get_session
from storm.services.profitability_service import calculate_return, record_arbitrages


session = get_session()

# %%
groups = session.query(Order.group_id).distinct(Order.group_id).all()

from pprint import pprint

for (group,) in groups:
    pprint(calculate_return(group))

# %%
