from logging import Logger, getLogger
from multiprocessing import Pipe, Process, Value
from multiprocessing.connection import Connection as PipeConnection
from signal import SIGINT, SIGTERM, signal
from time import sleep
from typing import Callable


class MPWorker(object):
    _process: Process
    _signal_tx: PipeConnection
    _name: str
    _interval: float
    _do_work: Callable[[Value], None]
    _shared_state: Value
    _logger: Logger

    def __init__(
        self,
        name: str,
        interval: float,
        do_work: Callable[[Value], None],
        shared_state: Value,
        logger: Logger = None,
    ):
        self._name = name
        self._interval = interval
        self._do_work = do_work
        self._shared_state = shared_state
        if logger is None:
            self._logger = getLogger(self._name)
        else:
            self._logger = logger

    @staticmethod
    def watcher_loop(
        signal_rx: PipeConnection,
        interval: float,
        do_work: Callable[[Value], None],
        shared_state: Value,
    ):
        signal(SIGINT, MPWorker.empty_signal_handler)
        signal(SIGTERM, MPWorker.empty_signal_handler)
        do_work(shared_state)
        while not signal_rx.poll(timeout=interval):
            do_work(shared_state)

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
            args=[signal_rx, self._interval, self._do_work, self._shared_state],
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
