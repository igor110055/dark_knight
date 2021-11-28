import requests

adapter = requests.adapters.HTTPAdapter(pool_connections=50, pool_maxsize=100)

def get_session(url):
    session = requests.Session()
    session.mount(url, adapter)
    return session
