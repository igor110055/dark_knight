import redis

redis_pool = redis.ConnectionPool(host='localhost', port=6379, db=0, decode_responses=True)

def get_client():
    return redis.Redis(connection_pool=redis_pool)
