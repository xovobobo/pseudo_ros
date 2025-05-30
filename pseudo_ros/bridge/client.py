import zmq
import time

from pseudo_ros.tools.logger import logger
from urllib.parse import SplitResult
from threading import Thread, Lock, Event
from zmq.utils.monitor import recv_monitor_message


ZMQ_INT_VERSION = int(zmq.__version__.split('.')[0])
if ZMQ_INT_VERSION <= 22:
    ZMQ_EVENT_HANDSHAKE_SUCCEEDED = zmq.EVENT_HANDSHAKE_SUCCEEDED
    ZMQ_EVENT_DISCONNECTED = zmq.EVENT_DISCONNECTED
else:
    ZMQ_EVENT_HANDSHAKE_SUCCEEDED = zmq.Event.HANDSHAKE_SUCCEEDED
    ZMQ_EVENT_DISCONNECTED = zmq.Event.DISCONNECTED

class BridgeClientTCP:
    __connected = False
    __active = False
    lock = Lock()
    stop_event = Event()

    def __init__(self, host: SplitResult, cb: list= []):
        self.cb = []
        self.__hostname = host.hostname
        self.__port = host.port

        self.__socket = None
        self.__context = None

    def init_socket(self):
        self.__context = zmq.Context()
        self.__socket = self.__context.socket(zmq.PUSH)
        self.__socket.setsockopt(zmq.LINGER, 0)
        self.__socket.connect(f"tcp://{self.__hostname}:{self.__port}")

    def active(self):
        return self.__active

    def start(self):
        if self.__active:
            return
        
        with self.lock:
            self.init_socket()
            self.__monitor = self.__socket.get_monitor_socket()
            self.__monitor_th = Thread(target=self.__monitor_socket, daemon=True)
            self.__monitor_th.start()

    def stop(self, max_timeout=10):
        if not self.__active:
            return True
        
        logger.debug(f"BridgeClient({self.__hostname}:{self.__port}) stopping...")
        self.stop_event.set()

        timeout = 0
        while self.__active and timeout < max_timeout:
            time.sleep(0.1)
            timeout += 0.1

        if timeout >= max_timeout:
            raise Exception("Failed to stop BridgeClientTCP thread")
        self.stop_event.clear()

        with self.lock:
            try:
                if self.__socket is not None:
                    self.__socket.close()
                
                if self.__context:
                    self.__context.destroy()
            except Exception as e:
                print(e)
                pass      
      
        logger.debug(f"BridgeClient({self.__hostname}:{self.__port}) stopped")
        return True

    def __monitor_socket(self):
        self.__active = True
        while not self.stop_event.is_set():
            try:
                while self.__monitor.poll(timeout=0.1):
                    event = recv_monitor_message(self.__monitor)

                    value = event.get("event")
                    with self.lock:
                        if value == ZMQ_EVENT_HANDSHAKE_SUCCEEDED:
                            self.__connected = True
                            try:
                                for cb in self.cb:
                                    cb(self)
                            except:
                                continue
                        elif value == ZMQ_EVENT_DISCONNECTED:
                            self.__connected = False
            except:
                continue

        self.__connected = False
        self.__active = False

    def is_connected(self) -> bool:
        return self.__connected

    def send(self, data):
        with self.lock:
            if self.__connected:
                try:
                    self.__socket.send(data)
                    return True
                except Exception as e:
                    self.__connected = False
                    return False
            else:
                return False
