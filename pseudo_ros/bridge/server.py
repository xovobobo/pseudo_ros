import zmq
import time

from pseudo_ros.tools.logger import logger
from urllib.parse import SplitResult
from threading import Thread, Event, Lock

class BridgeServerTCP:
    __active = False
    stop_event = Event()
    lock = Lock()

    def __init__(self, host: SplitResult, cb):
        self.cb = cb
        self.__host = host
        self.__socket = None
        self.__context = None
        self.__th = Thread(target=self.__receive, daemon=True, name="FromRosReceiver")

    def __init_socket(self):
        self.__context = zmq.Context()
        self.__socket = self.__context.socket(zmq.PULL)
        self.__socket.setsockopt(zmq.LINGER, 0)
        self.__socket.bind(f"tcp://{self.__host.hostname}:{self.__host.port}")
        self.__socket.setsockopt(zmq.RCVTIMEO, 500)

    def active(self):
        return self.__active

    def start(self):
        if self.__active:
            return
        self.__init_socket()
        self.__th.start()

    def stop(self, max_timeout = 10) -> bool:
        if not self.__active:
            return True

        logger.debug(f"BridgeServer({self.__host.netloc}) stopping...")
        self.stop_event.set()

        timeout = 0
        while self.__active and timeout < max_timeout:
            time.sleep(0.1)
            timeout += 0.1
            
        if timeout >= max_timeout:
            raise Exception("Failed to stop BridgeServerTCP thread")

        with self.lock:
            try:
                if self.__socket is not None:
                    self.__socket.close()

                if self.__context:
                    self.__context.destroy()
            except Exception as e:
                logger.error(e)
        
        logger.debug(f"BridgeServer({self.__host.netloc}) stopped")

        return True

    def __receive(self):
        self.__active = True
        while not self.stop_event.is_set():
            with self.lock:
                try:
                    message = self.__socket.recv()
                except:
                    continue
            try:
                self.cb(message)
            except:
                continue

        self.__active = False
        self.stop_event.clear()