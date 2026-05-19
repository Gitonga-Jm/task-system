# Replace your Redis connection code in main.py with this:
import ssl

REDIS_HOST = os.getenv('REDIS_HOST', 'localhost')
REDIS_PORT = int(os.getenv('REDIS_PORT', 6379))
REDIS_PASSWORD = os.getenv('REDIS_PASSWORD', None)
REDIS_TLS = os.getenv('REDIS_TLS', 'False').lower() == 'true'

if REDIS_PASSWORD:
    if REDIS_TLS:
        redis_client = redis.Redis(
            host=REDIS_HOST, 
            port=REDIS_PORT, 
            password=REDIS_PASSWORD,
            ssl=True,
            ssl_cert_reqs=None,
            decode_responses=True
        )
    else:
        redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, password=REDIS_PASSWORD, decode_responses=True)
else:
    redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True)
