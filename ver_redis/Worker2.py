import time

import redis

# Set up logging
worker = redis.Redis(host='localhost', port=6379, db=2)

# Set up client to master
master = redis.from_url('redis://localhost:6379', db=0)

master.rpush("workers", 'redis://localhost:6379/2')

try:
    print("Use Ctrl+c to exit")
    while True:
        time.sleep(86400)
except KeyboardInterrupt:
    print("Exiting")
    worker.flushdb()
    exit()
