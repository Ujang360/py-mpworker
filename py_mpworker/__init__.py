from logging import Logger, getLogger
from multiprocessing import Pipe, Process, Value
from multiprocessing.connection import Connection as PipeConnection
from signal import SIGINT, SIGTERM, signal
from time import sleep
from typing import Callable


class MPWorker(object):
    _name: str
    _signal_tx: PipeConnection
    _first_run_immediate: bool
    _interval_seconds_seconds: float
    _do_work: Callable[[Value], None]
    _error_callback: Callable[[Exception], None]
    _shared_state: Value
    _logger: Logger

    def __init__(
        self,
        name: str,
        interval_seconds: float,
        do_work: Callable[[Value], None],
        shared_state: Value,
        logger: Logger = None,
        error_callback: Callable[[Exception], None] = None,
        first_run_immediate: bool = True,
    ):
        self._name = name
        self._first_run_immediate = first_run_immediate
        self._interval_seconds_seconds = interval_seconds
        self._do_work = do_work
        self._shared_state = shared_state
        self._error_callback = error_callback
        if logger is None:
            self._logger = getLogger(self._name)
        else:
            self._logger = logger

    @staticmethod
    def watcher_loop(
        signal_rx: PipeConnection,
        interval_seconds: float,
        first_run_immediate: bool,
        do_work: Callable[[Value], None],
        error_callback: Callable[[Exception], None],
        shared_state: Value,
    ):
        signal(SIGINT, MPWorker.empty_signal_handler)
        signal(SIGTERM, MPWorker.empty_signal_handler)
        configured_interval = interval_seconds
        if first_run_immediate:
            interval_seconds = 0
        while not signal_rx.poll(timeout=interval_seconds):
            interval_seconds = configured_interval
            try:
                do_work(shared_state)
            except Exception as captured_exception:
                if error_callback is not None:
                    error_callback(captured_exception)
                else:
                    raise captured_exception

    @staticmethod
    def empty_signal_handler(sig, frame):
        pass

    def start(self):
        self._logger.info(f"Starting {self._name}...")
        (signal_rx, signal_tx) = Pipe(duplex=False)
        self._signal_tx = signal_tx
        self._process = Process(
            target=MPWorker.watcher_loop,
            name=self._name,
            args=[
                signal_rx,
                self._interval_seconds_seconds,
                self._first_run_immediate,
                self._do_work,
                self._error_callback,
                self._shared_state,
            ],
        )
        self._process.start()
        self._logger.info(f"{self._name} started")

    def stop(self):
        self._logger.info(f"Stopping {self._name}...")
        try:
            self._signal_tx.send(None)
            # Since the caller of this stop method is the OS, then we can't call JOIN
            while self._process.is_alive():
                sleep(0.1)
        except BrokenPipeError as bpe:
            self._logger.error(bpe)
        self._logger.info(f"{self._name} stopped")
