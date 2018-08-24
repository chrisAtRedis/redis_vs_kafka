from confluent_kafka import Producer, Consumer, KafkaError
from redis import Redis
from time import time
import sys

# Simple synchronous reader / writer
class RedisStream(object):
    def __init__(self, redis, stream):
        self.redis = redis
        self.stream = stream
        self.last_id = None

    def put(self, msg):
        self.redis.xadd(self.stream, {'msg':msg})

    def get(self):
        if not self.last_id:
            self.last_id = '0'
        msgs = self.redis.xread([self.stream], [self.last_id], count=1)
        id = msgs[0][1][0][0]
        self.last_id = id
        self.redis.xdel(self.stream, [id])
        return msgs[0][1][0][1]

    def __len__(self):
        return self.redis.xlen(self.stream)

class BenchBase(object):
    def __init__(self, no_msg, msg_len):
        self.no_msg = no_msg
        self.msg_len = msg_len

    def construct_msg(self, pref, val):
        return pref + 'Value #{0}: '.format(val) + 'a' * self.msg_len

    def clock_produce(self):
        self.start0 = time()
        start = time()
        self.produce()
        self.prod_time_el = round(time() - start, 3)

    def clock_consume(self):
        start = time()
        self.consume()
        self.cons_time_el = round(time() - start, 3)

    def run_benchmark(self):
        self.clock_produce()
        self.clock_consume()

    def get_result(self):
        vals = [self.no_msg, self.msg_len, self.prod_time_el, self.cons_time_el, round(self.prod_time_el
                                                                                       + self.cons_time_el, 3)]
        return '\t\t'.join([str(i) for i in vals])

class KafkaBench(BenchBase):
    def produce(self):
        p = Producer(
            {'bootstrap.servers': 'localhost:9092', 'broker.version.fallback': '0.9.0.0',
             'message.max.bytes':2000000,
             #'queue.buffering.max.messages': 10000000,
             'api.version.request': False})
        # try:
        for val in range(0, self.no_msg):
            p.produce('bench_kafka', self.construct_msg('Kafka', val))
        # except KeyboardInterrupt:
        # pass
        p.flush(30)

    def consume(self):
        settings = {
            'bootstrap.servers': 'localhost:9092',
            'group.id': 'mygroup',
            'client.id': 'client-1',
            'enable.auto.commit': True,
            'session.timeout.ms': 6000,
            'broker.version.fallback': '0.9.0.0',
            'api.version.request': False,
            'fetch.max.bytes': 10000000,
            'default.topic.config': {'auto.offset.reset': 'smallest'}
        }

        c = Consumer(settings)

        c.subscribe(['bench_kafka'])
        msgcount = 0
        try:
            while msgcount < self.no_msg:
                msg = c.poll(0.1)
                if msg is None:
                    continue
                if msg.error():
                    # print('Error occured: {0}'.format(msg.error().str()))
                    continue
                msgcount += 1
        except KeyboardInterrupt:
            pass
        finally:
            c.close()
        return msgcount, msg

class RedisBench(BenchBase):
    def create_stream(self, redis,  stream='mystream'):
       self.redis_stream = RedisStream(redis, stream)

    def produce(self):
        for val in range(0, self.no_msg):
            self.redis_stream.put(self.construct_msg('Redis', val))

    def consume(self):
        for val in range(0, self.no_msg):
            msg = self.redis_stream.get()

def main():
    msgs = [100, 1000]
    lens = [10, 1000, 1000000]
    k, r = False
    for i in sys.argv[1:]:
        fl = sys.argv[i][0].lower()
        if  fl == 'k':
            k = True
        elif fl == 'r':
            r = True

    if not (k or r):
        print('Please state either k[afka] or [r]edis as benchmark targets')
        exit(0)

    if k:
        print('=== Kafka benchmark ===')
        for i in msgs:
            for j in lens:
                k = KafkaBench(i, j)
                k.run_benchmark()
                print(k.get_result())

    if r:
        print('=== Redis benchmark ===')
        redis = Redis()
        for i in msgs:
            for j in lens:
                r = RedisBench(i, j)
                r.create_stream(redis)
                r.run_benchmark()
                print(r.get_result())

if __name__ == '__main__':
    exit(main())
