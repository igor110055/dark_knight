import os

import redis

redis_host = os.getenv("REDIS_HOST", "localhost")
# redis_pool = redis.ConnectionPool(
#     host=redis_host, port=6379, db=0, decode_responses=True
# )

# redis_pool = redis.ConnectionPool.from_url('unix:///var/run/redis/redis.sock', decode_responses=True)


def get_client(db=0, decode_responses=True):
    return redis.Redis(
        # host=redis_host,
        unix_socket_path="/var/run/redis/redis-server.sock",
        db=db,
        decode_responses=decode_responses,
        health_check_interval=20,
    )
    # return FRedis(unix_socket_path='/run/redis/redis.sock', db=db, decode_responses=True)
    # return FRedis(connection_pool=redis_pool)
