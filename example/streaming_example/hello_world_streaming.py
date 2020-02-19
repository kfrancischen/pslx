from pslx.streaming.operator import StreamingOperator
from pslx.streaming.container import DefaultStreamingContainer
from pslx.util.dummy_util import DummyUtil


class HelloWorldOp(StreamingOperator):

    def __init__(self):
        super().__init__(operator_name='hello_world_op')

    def _execute(self):
        print('hello world')
        return True


class HelloWorldContainer(DefaultStreamingContainer):
    def __init__(self):
        super().__init__(container_name='hello_world_container')


if __name__ == "__main__":
    op = HelloWorldOp()
    container = HelloWorldContainer()
    container.add_operator_edge(from_operator=op, to_operator=DummyUtil.dummy_streaming_operator())
    container.add_operator_edge(
        from_operator=op,
        to_operator=DummyUtil.dummy_streaming_operator(operator_name='dummy_streaming_operator2')
    )

    container.initialize()
    container.execute()
