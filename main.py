#!/usr/bin/env python3


import urllib.parse
import sys
import json
import signal
import time
import multiprocessing
import selectors
import socket
import ctypes
import queue


class Data:
    def __init__(self, A1: int, A2: int, A3: int):
        self.A1_sum = A1
        self.A2_max = A2
        self.A3_min = A3

    def accumulate(self, data: 'Data'):
        self.A1_sum +=                 data.A1_sum
        self.A2_max = max(self.A2_max, data.A2_max)
        self.A3_min = min(self.A3_min, data.A3_min)


class ServerData(Data):
    def __init__(self, worker_data: Data):
        self.__dict__   = worker_data.__dict__.copy()
        self.timestamp  = int(time.time())
        self.count_type = 10


class Process(multiprocessing.Process):
    def __init__(self) -> None:
        super().__init__(target=self._run)
        self.__is_running   = multiprocessing.Value(ctypes.c_bool)
        self._timeout       = 1

    def start(self):
        self.__is_running.value = True
        super().start()

    def stop(self):
        self.__is_running.value = False
        super().join(self._timeout)

    def is_running(self) -> bool:
        return self.__is_running.value

    def _run(self):
        pass


class MessageHandler(Process):
    def __init__(self, messages) -> None:
        super().__init__()
        self.__messages     = messages
        self.__statistics   = multiprocessing.Queue()
        self.__flag         = multiprocessing.Event()

    def _run(self):
        print(multiprocessing.current_process().ident, "handler process begin")

        data = None

        while self.is_running():
            try:
                # print(multiprocessing.current_process().ident, "handler wait message ...")
                try:
                    message = self.__messages.get(timeout=1)
                except queue.Empty:
                    message = None
                    pass
                # print(multiprocessing.current_process().ident, "handler wait message OK,", message)

                if message is not None:
                    try:
                        json_data = json.loads(message)
                        json_data = Data(int(json_data["A1"]), int(json_data["A2"]), int(json_data["A3"]))
                        
                        if data is None:
                            data = json_data
                            # print(multiprocessing.current_process().ident, "new data")
                        else:
                            data.accumulate(json_data)
                            # print(multiprocessing.current_process().ident, "acc data")
                    except Exception as e:
                        print(multiprocessing.current_process().ident, e)

                # print(multiprocessing.current_process().ident, "handler flag   :", self.__flag.is_set())
                if self.__flag.is_set():
                    print(multiprocessing.current_process().ident, "dump data", data)
                    self.__statistics.put_nowait(data)
                    data = None
                    self.__flag.clear()

            except Exception as e:
                print(multiprocessing.current_process().ident, e)
                break

            except KeyboardInterrupt:
                pass

        print(multiprocessing.current_process().ident, "handler process end")

    def get_statistics(self):
        if not self.is_running():
            return None
        # print("get statistics ...")
        try:
            self.__flag.set()
            s = self.__statistics.get(self._timeout)
            # print("get statistics OK, ", s)
            return s
        except queue.Empty:
            # print("get statistics OK, timeout")
            return None
        except Exception as e:
            print("get statistics error:", e)


class UDPSocketAcceptor(Process):
    def __init__(self, messages, url: str) -> None:
        super().__init__()

        self.__messages = messages
        self.__url      = urllib.parse.urlparse(url)

        assert self.__url.hostname is not None and self.__url.port is not None

    def _run(self):
        print(multiprocessing.current_process().ident, "server  process begin")
        selector    = selectors.DefaultSelector()
        socket_     = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

        socket_.bind((self.__url.hostname, self.__url.port))
        socket_.setblocking(False)
        selector.register(socket_, selectors.EVENT_READ)

        while self.is_running():
            try:
                for key, mask in selector.select(timeout=self._timeout):
                    if key.fileobj == socket_:
                        self.__messages.put_nowait(socket_.recv(1024))
            except KeyboardInterrupt:
                pass

        selector.unregister(socket_)
        socket_.close()
        print(multiprocessing.current_process().ident, "server  process end")


def stop(signum, frame):
    state.is_running = False
    print()


def log_statistics(handlers: list[MessageHandler], delay_sec: int, file_path: str):
    statistics = None

    for handler in handlers:
        statistic = handler.get_statistics()

        if statistic is None:
            continue

        if statistics is None:
            statistics = statistic
        else:
            statistics.accumulate(statistic)

    if statistics is not None:
        statistics.delay_sec = delay_sec
        with open(file_path, 'a') as file:
            file.write(f"{json.dumps(statistics.__dict__)}\n")
        print(file_path, " log ", json.dumps(statistics.__dict__))


if __name__ == "__main__":
    if len(sys.argv) < 5:
        print(sys.argv[0], "url", "thread_count", "delay_sec", "output_log_path")
        exit(1)

    messages    = multiprocessing.Queue()
    statistics  = multiprocessing.Queue()
    acceptor    = UDPSocketAcceptor(messages, sys.argv[1])
    handlers    = []
    delay_sec   = int(sys.argv[3])

    print("start ...")

    for i in range(int(sys.argv[2])):
        handler = MessageHandler(messages)
        handler.start()
        handlers.append(handler)

    acceptor.start()

    print("start OK")

    state = type('', (), {'is_running': True})
    signal.signal(signal.SIGINT, stop)

    print("press 'ctrl+c' to stop server")

    time_step = 0.1
    while state.is_running:
        for i in range(int(delay_sec / time_step)):
            if state.is_running:
                time.sleep(time_step)
            else:
                break

        log_statistics(handlers, delay_sec, sys.argv[4])

    print("stop ...")

    acceptor.stop()

    for handler in handlers:
        handler.stop()

    print("stop OK")
