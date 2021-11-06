import redis
import aioredis

# redis_pool = redis.ConnectionPool(host='localhost', port=6379, db=0, decode_responses=True)

# redis_pool = redis.ConnectionPool.from_url('unix:///var/run/redis/redis.sock', decode_responses=True)

class FRedis(redis.Redis):
    def rpop(self, name, count=None):
        if count is None:
            return super().rpop(name)
        else:
            return self.execute_command('RPOP', name, count)

def get_client(db=0, a_sync=False):
    if a_sync:
        return aioredis.from_url('unix:///run/redis/redis.sock', decode_responses=True)
    # return redis.Redis(connection_pool=redis_pool)
    return FRedis(unix_socket_path='/run/redis/redis.sock', db=db, decode_responses=True)
    # return FRedis(connection_pool=redis_pool)
