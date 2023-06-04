import time
from pslx.micro_service.pubsub.publisher import Publisher
from pslx.util.timezone_util import TimeSleepObj
from pslx.schema.rpc_pb2 import HealthCheckerRequest

if __name__ == "__main__":
    publisher1 = Publisher(
        exchange_name='test_exchange_1',
        topic_name='test1',
        connection_str='amqp://guest:guest@localhost:5672'
    )

    message1 = HealthCheckerRequest()
    message1.server_url = 'test1'

    publisher2 = Publisher(
        exchange_name='test_exchange_2',
        topic_name='test2',
        connection_str='amqp://guest:guest@localhost:5672'
    )

    message2 = HealthCheckerRequest()
    message2.server_url = 'test2'

    while True:
        print("Publish message...")
        publisher1.publish(message=message1)
        publisher2.publish(message=message2)
        time.sleep(TimeSleepObj.ONE_SECOND)
