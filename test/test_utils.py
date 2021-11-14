from multiprocessing import Manager, Process, current_process
from threading import Thread
from time import sleep

from storm.clients.redis_client import get_client
from storm.utils import redis_lock


def test_redis_lock__multiprocessing():
    manager = Manager()
    fired = manager.list()
    waiting = manager.list()

    processes = []
    for _ in range(10):
        process = Process(target=racing_condition, args=(fired, waiting))
        processes.append(process)
        process.start()

    for process in processes:
        process.join()

    # random fire
    assert len(fired) == 1
    assert len(waiting) == 9


def test_redis_lock__threading():
    manager = Manager()
    fired = manager.list()
    waiting = manager.list()

    threads = []
    for _ in range(10):
        thread = Thread(target=racing_condition, args=(fired, waiting))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()

    # random fire
    assert len(fired) == 1
    assert len(waiting) == 9


def racing_condition(fired, waiting):
    redis = get_client()
    with redis_lock(redis, 'test_lock') as lock_acquired:
        if lock_acquired:
            sleep(0.1)  # workload
            fired.append(current_process().pid)
        else:
            waiting.append(current_process().pid)
