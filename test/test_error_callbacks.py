from multiprocessing import Pipe, Value, Manager
from multiprocessing.connection import Connection as PipeConnection
from py_mpworker import MPWorker

TEST_INTERVAL = 1


def test_mpworker_error_callback_got_called():
    expected_error_message = "Hello! This exception got handled..."
    (signal_receiver, finished_signaler) = Pipe(duplex=False)
    error_message = Manager().dict()
    error_message["the_message"] = ""

    def callback_func(_: Value):
        raise ValueError(expected_error_message)

    def error_callback_func(error: Exception):
        error_message["the_message"] = str(error)
        finished_signaler.send(None)

    worker = MPWorker(
        "test_worker",
        TEST_INTERVAL,
        callback_func,
        None,
        error_callback=error_callback_func,
    )
    worker.start()
    signal_receiver.poll(3600)
    worker.stop()
    assert error_message.get("the_message") == expected_error_message
