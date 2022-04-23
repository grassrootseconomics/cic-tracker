from stateness.redis import RedisMonitor


class SyncTimeRedisCache:

    def __init__(self, host='localhost', port=6379, db=9999):
        self.state = RedisMonitor('cic-eth-tracker', host=host, port=port, db=db)
        self.state.register('lastseen', persist=True)

    def cache_time(self, block, tx):
        v = self.state.get('lastseen')
        if v != None:
            v = int(v.decode('utf-8'))
            if v > block.timestamp:
                return
        self.state.set('lastseen', block.timestamp)


