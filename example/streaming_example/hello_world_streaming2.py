from pslx.streaming.operator import StreamingOperator
from pslx.streaming.container import DefaultStreamingContainer
from pslx.util.dummy_util import DummyUtil


class HelloWorldOp(StreamingOperator):

    def __init__(self, operator_name='hello_world_op'):
        super().__init__(operator_name=operator_name)

    def execute_impl(self):
        print(self.get_node_name())


class HelloWorldContainer(DefaultStreamingContainer):
    def __init__(self, container_name='hello_world_container', ttl=7):
        super().__init__(container_name=container_name, ttl=ttl)


if __name__ == "__main__":
    op1 = HelloWorldOp(operator_name='hello_world_op1')
    op2 = HelloWorldOp(operator_name='hello_world_op2')
    op3 = HelloWorldOp(operator_name='hello_world_op3')
    op4 = HelloWorldOp(operator_name='hello_world_op4')
    container1 = HelloWorldContainer()
    container1.bind_backend(
        server_url="localhost:11443"
    )
    container1.add_operator_edge(from_operator=op1, to_operator=op3)
    container1.add_operator_edge(from_operator=op1, to_operator=op4)
    container1.add_operator_edge(from_operator=op2, to_operator=op3)
    container1.add_operator_edge(from_operator=op2, to_operator=op4)
    container1.initialize()
    container1.execute()

    op1 = HelloWorldOp(operator_name='hello_world_op1')
    op2 = HelloWorldOp(operator_name='hello_world_op2')
    op3 = HelloWorldOp(operator_name='hello_world_op3')
    op4 = HelloWorldOp(operator_name='hello_world_op4')
    container2 = HelloWorldContainer(container_name='hello_world_container_2', ttl=1)
    container2.bind_backend(
        server_url="localhost:11443"
    )
    dummy_op = DummyUtil.dummy_streaming_operator(operator_name='dummy')
    container2.add_operator_edge(from_operator=op1,
                                 to_operator=dummy_op)
    container2.add_operator_edge(from_operator=op2,
                                 to_operator=dummy_op)
    container2.add_operator_edge(from_operator=op3,
                                 to_operator=dummy_op)
    container2.add_operator_edge(from_operator=op4,
                                 to_operator=dummy_op)
    container2.initialize()
    container2.execute()
