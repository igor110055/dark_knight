from multiprocessing import Manager, Process, current_process
from threading import Thread, current_thread
from time import sleep

from storm.clients.redis_client import get_client
from storm.utils import redis_lock


def test_redis_lock__multiprocessing():
    manager = Manager()
    executed = manager.list()
    exited = manager.list()

    processes = []
    for _ in range(10):
        process = Process(target=racing_condition, args=(executed, exited, lambda: current_process().pid))
        processes.append(process)
        process.start()

    for process in processes:
        process.join()

    # random fire
    assert len(executed) == 1
    assert len(exited) == 9


def test_redis_lock__threading():
    executed = []
    exited = []

    threads = []
    for _ in range(10):
        thread = Thread(target=racing_condition, args=(executed, exited, current_thread))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()

    # random fire
    assert len(executed) == 1
    assert len(exited) == 9


def racing_condition(executed, exited, func):
    redis = get_client()
    with redis_lock(redis, 'test_lock') as lock:
        if lock.lock_acquired:
            sleep(1)  # workload
            executed.append(func())
        else:
            exited.append(func())
