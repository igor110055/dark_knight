#%%
import os
from ccxt import binance
from dotenv import load_dotenv
from order_engine import OrderEngine

binance_client = binance({'apiKey': os.getenv('API_KEY'), 'secret': os.getenv('SECRET_KEY')})

engine = OrderEngine(binance_client)
# %%
