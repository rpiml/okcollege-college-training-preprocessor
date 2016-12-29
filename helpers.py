import redis
import time
import os
import pika

def wait_for_redis(host=None):
    '''
    Block until the redis service is running
    '''

    if host is None:
        host = (os.getenv('REDIS_HOST') or 'localhost')

    print('Confirming redis service is running...')
    r = redis.StrictRedis(host=(os.getenv('REDIS_HOST') or 'localhost'))
    while True:
        try:
            r.ping()
            break
        except (redis.exceptions.ConnectionError, redis.exceptions.BusyLoadingError):
            print('Could not connect to redis. Retrying...')
            time.sleep(1)

    print('Redis service confirmed running.')

def rabbitmq_connect(user='rabbitmq', password='rabbitmq', host=None):
    '''
    Connect to a rabbitmq server and wait for the connection to be established
    '''
    if host is None:
        host = os.getenv('RABBITMQ_HOST') or 'localhost'
    credentials = pika.PlainCredentials(user, password)
    parameters = pika.ConnectionParameters(
        host=host,
        credentials=credentials
    )
    print('Attempting RabbitMQ connection...')
    while True:
        try:
            connection = pika.BlockingConnection(parameters)
            break
        except Exception as e:
            print('Could not connect to RabbitMQ. Retrying...')
            time.sleep(1)

    print('RabbitMQ connection established')
    return connection
