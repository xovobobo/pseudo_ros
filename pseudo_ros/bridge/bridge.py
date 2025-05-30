import argparse
import urllib.parse
import json
import gzip

from typing import Union, Optional

from .client import BridgeClientTCP
from .server import BridgeServerTCP

from rosbags.serde import deserialize_cdr, serialize_cdr
from rosbags.typesys import Stores, get_typestore

class Bridge():
    def __init__(self, config_path: str, cb_on_ros_msg):
        cfg = self.read_config(config_path)
        self.cb_on_ros_msg = cb_on_ros_msg

        self.host = urllib.parse.urlsplit('//' + cfg['host'])
        

        self.ts1 = get_typestore(Stores.ROS1_NOETIC)
        self.ts2 = get_typestore(Stores.ROS2_HUMBLE)

        self.tcp_clients = []
        self.msg_data = {}

        for p in cfg['published']:
            clients = []
            for h in p['hosts']:
                clients.append(urllib.parse.urlsplit('//' + h))

            for client in clients:
                tcp_client = BridgeClientTCP(client)
                self.tcp_clients.append(tcp_client)

            for t in p['topics']:
                n = t['name']
                t.pop('name')
                self.msg_data.update( {n: t})  

        self.from_ros_subscriber = BridgeServerTCP(self.host, self.on_ros_binary_msg)

    def on_ros_binary_msg(self, msg:bytes):
        try:
            json_length = int.from_bytes(msg[:2], byteorder="little")
            json_data: dict = json.loads(gzip.decompress(msg[2 : 2 + json_length]))
        except Exception as e:
            print("Invalid received message. Failed to parse: {}".format(str(e)))
            return
        
        topic_name = json_data.get("n", "")

        compression_level = json_data.get("c")
        if (compression_level is None) or (not isinstance(compression_level, int)):
            print("Compression level field was not filled for the topic {}. Added to ignore list".format(topic_name))
            return
        if compression_level > 0:
            binary_packet = gzip.decompress(msg[2+json_length:])
        else:
            binary_packet = msg[2+json_length:]
        
        try:
            json_data['t'] = json_data['t'].replace('.', '/')
        except:
            print('failed to determine topic name')
            return
        
        if str(json_data.get('v')) == '2':
            deserializer = self.ts2.deserialize_cdr
        elif str(json_data.get('v')) == '1':
            deserializer = self.ts1.deserialize_cdr
        else:
            print("Unknown ros version")
            return

        try:
            ros_msg = deserializer(rawdata=binary_packet, typename=json_data['t'])
        except Exception as e:
            print(f'Failed to deserialize ros message. {e}')
            return

        try:
            self.cb_on_ros_msg(ros_msg, json_data)
        except:
            return

    
    def publish_message(self, msg, topic: str, version=2, compress_level: Optional[int] = None, q: Optional[Union[str, dict, int]] = None):
        d = {
            'v': version,
            't': msg.__msgtype__.replace('/', '.'),
            'n': topic,
        }

        if version == 2:
            serializer = self.ts2.serialize_cdr

            if q is None:
                q = self.msg_data[topic]["qos"]
            d.update({'q': q})
        elif version == 1:
            serializer = self.ts1.serialize_cdr
        else:
            print('unknown version')
            return
        
        if compress_level is None:
            compress_level = self.msg_data[topic].get('compression_level', 0)
        
        d.update({'c': compress_level})

        serialized_message = serializer(message=msg, typename=msg.__msgtype__)
        
        if compress_level > 0:
            serialized_message = gzip.compress(serialized_message, compresslevel=compress_level)
        
        json_compressed = gzip.compress(
            json.dumps(d).encode("utf-8"), compresslevel=1
        )

        json_length = len(json_compressed).to_bytes(length=2, byteorder='little')
        binary_msg = json_length + json_compressed + serialized_message

        for client in self.tcp_clients:
            client.send(binary_msg)


    @staticmethod
    def read_config(config_path: str) -> dict:
        with open(config_path, 'r') as json_file:
            cfg = json.load(json_file)
        return cfg


if __name__ == "__main__":
    import numpy as np
    import time

    bridge = Bridge('/home/bobo/Projects/hitl_sitl/cfg.json', None)
    Joy = bridge.ts2.types['sensor_msgs/msg/Joy']
    Header = bridge.ts2.types['std_msgs/msg/Header']
    Time = bridge.ts2.types['builtin_interfaces/msg/Time']

    now = time.time()
    sec = int(now)
    nanosec = int((now - sec) * 1e9)
    joy_msg = Joy(
        header=Header(stamp=Time(sec=sec, nanosec=nanosec), frame_id="qqqq"),
        axes=np.array([0.0, 0.1, 0.0, 0.1], dtype=np.float32),
        buttons=np.array([], dtype=np.int32)
    )
    bridge.publish_message(joy_msg, '/raw_cmd')
    