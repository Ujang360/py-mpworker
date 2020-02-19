from ctypes import c_uint32
from multiprocessing import Pipe, Value
from multiprocessing.connection import Connection as PipeConnection

from py_mpworker import MPWorker

TEST_INTERVAL = 1


def process_event(
    state: Value, desired_call_count: int, finished_signaler: PipeConnection,
):
    with state.get_lock():
        state.value += 1
        if state.value == desired_call_count:
            finished_signaler.send(None)


def test_mpworker_callback_should_be_called_multiple_times():
    desired_call_count = 5
    (signal_receiver, finished_signaler) = Pipe(duplex=False)
    shared_state = Value(c_uint32)
    shared_state.value = 0

    def callback(state: Value):
        process_event(state, desired_call_count, finished_signaler)

    worker = MPWorker("test_worker", TEST_INTERVAL, callback, shared_state)
    worker.start()
    signal_receiver.poll(3600)
    worker.stop()
    with shared_state.get_lock():
        assert shared_state.value == desired_call_count
