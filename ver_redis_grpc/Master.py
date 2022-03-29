import time
import redis

# Set up logging
master = redis.Redis(host='localhost', port=6379, db=0)

try:
    print("Use Ctrl+c to exit")
    while True:
        time.sleep(86400)
except KeyboardInterrupt:
    print("Exiting")
    master.flushdb()
    exit()
