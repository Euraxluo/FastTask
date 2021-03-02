# coding:utf8

import uuid
import asyncio
import threading
from _queue import Empty
from functools import wraps
from functools import partial
from concurrent import futures
from asyncio import AbstractEventLoop
from abc import abstractmethod
from typing import Callable, Collection, TypeVar, Any

Executor = TypeVar('Executor', bound=Any)


class Channel(Collection):
    """Channel"""
    @abstractmethod
    def put(self):
        pass

    @abstractmethod
    def get(self):
        pass

    @classmethod
    def __subclasshook__(cls, C):
        for method in ('put', 'get'):
            for B in C.__mro__:
                if method in B.__dict__:
                    if B.__dict__[method] is None:
                        return NotImplemented
                    break
            else:
                return NotImplemented
        return True


class Broker(Collection):
    """Broker"""
    @abstractmethod
    def put(self):
        pass

    @abstractmethod
    def get(self):
        pass

    @classmethod
    def __subclasshook__(cls, C):
        for method in ('put', 'get'):
            for B in C.__mro__:
                if method in B.__dict__:
                    if B.__dict__[method] is None:
                        return NotImplemented
                    break
            else:
                return NotImplemented
        return True


class Backend(Collection):
    """Backend"""
    @abstractmethod
    def put(self):
        pass

    @abstractmethod
    def get(self):
        pass

    @classmethod
    def __subclasshook__(cls, C):
        for method in ('put', 'get'):
            for B in C.__mro__:
                if method in B.__dict__:
                    if B.__dict__[method] is None:
                        return NotImplemented
                    break
            else:
                return NotImplemented
        return True


class FastTask(object):
    def __init__(self):
        """初始化task控制器"""
        self.brokers = {}
        """具名后端map"""
        self.backends = {}
        """具名通道map"""
        self.channels = {}  # 通道
        """task"""
        self.tasks = {}
        """总的期望worker数"""
        self.expect_processes = 0
        """是否停止worker的运行"""
        self.terminated = False
        """运行所有worker的线程对象"""
        self.threading_workers = None

    def addChannel(self, channelName: str, channel: Channel):
        """
        设置channel，用于主进程和子进程通信
        :param channelName:
        :param channel:
        :return:
        """
        self.channels[channelName] = channel

    def addBroker(self, brokerName: str, broker: Broker):
        """
        设置中间人
        :param self:
        :param brokerName:
        :param broker:
        :return:
        """
        self.brokers[brokerName] = broker

    def addBackend(self, backendName: str, backend: Backend):
        """
        设置任务结果存储器
        :param self:
        :param backendName:
        :param backend:
        :return:
        """
        self.backends[backendName] = backend

    def task(self, broker: str, backend: str, max_worker: int = 1):
        """
        task装饰器，可以将普通函数装饰为一个异步任务,启动时，会自动创建进程来执行task
        :param self:
        :param broker:中间人，woker需要从任务关注的中间人那里获取任务信息
        :param backend:结果收集者，woker将任务执行的结果存储在backend那里
        :param max_worker:woker的最大数量
        :return:
        """

        def decorator(func):
            @wraps(func)
            def wrapper_task(*args, **kwargs):
                task_result = func(*args, **kwargs)
                backend_obj = self.backends[backend]
                backend_obj.put(task_result)
                return task_result

            self.tasks[uuid.uuid1()] = [wrapper_task, self.brokers[broker], self.backends[backend], max_worker]
            return wrapper_task

        self.expect_processes += max_worker
        return decorator

    def __call__(self, max_workers: int = 12, thread_name_prefix: str = '', loop=None, executor=None, join=False):
        """
        调用器，开启一个线程来异步拉起所有的worker
        :param max_workers:
        :param thread_name_prefix:
        :param loop:
        :param executor:
        :return:
        """
        self.threading_workers = threading.Thread(target=self.start_all_worker,
                                                  args=(max_workers, thread_name_prefix, loop, executor))
        self.threading_workers.start()
        if join:
            self.threading_workers.join()

    def join(self):
        """
        使所有worker的运行阻塞
        :return:
        """
        if self.threading_workers:
            self.threading_workers.join()
        else:
            raise Exception("workers has None")

    def terminate(self):
        """
        使worker的运行终止
        :return:
        """
        self.terminated = True

    def start_all_worker(self, max_workers: int = 12, thread_name_prefix: str = '', loop: AbstractEventLoop = None,
                         executor: Executor = None):
        """
        拉起所有的worker
        1.为所有的task分配合理的worker数量
        2.将所有的同步worker包装为异步函数（future）并且收集起来
        3.启动所有的异步worker并阻塞等待
        :param max_workers:
        :param thread_name_prefix:
        :param loop:
        :param executor:
        :return:
        """
        """1.为所有的task分配合理的worker数量"""
        assert max_workers >= len(self.tasks)
        surplus_expects = 1 if self.expect_processes - len(self.tasks) == 0 else self.expect_processes - len(self.tasks)
        surplus_workers = max_workers - len(self.tasks)
        worker_executors = executor if executor else futures.ThreadPoolExecutor(len(self.tasks))

        all_worker = []
        for taskid, task in self.tasks.items():
            """worker分配算法"""
            if surplus_workers > 0:
                more_worker_num = int(round((task[-1] - 1) * (surplus_workers / surplus_expects), 0))
                surplus_workers -= more_worker_num
                surplus_expects -= (task[-1] - 1)
                task[-1] = 1 + more_worker_num
            else:
                task[-1] = 1
            """2.将所有的同步worker包装为异步函数（future）并且收集起来"""
            all_worker.append(
                FastTask.work_in_async(worker_executors, self.worker, task, thread_name_prefix, async_loop=loop))

        """3.启动所有的异步worker并阻塞等待"""
        loop.run_until_complete(asyncio.wait(all_worker))

    def worker(self, task: Callable, thread_name_prefix: str):
        """
        worker
        :param task:
        :param broker:
        :param backend:
        :return:
        """
        [worker_nums, task, broker, backend] = task[-1], task[0], task[1], task[2]
        """申请一个线程池用于运行worker，数量根据worker_nums指定"""
        all_tasks = []
        with futures.ThreadPoolExecutor(worker_nums, thread_name_prefix=thread_name_prefix) as executor:
            """若没有发出worker停止指令,就一直阻塞等待中间人分发任务"""
            while not self.terminated:
                if broker.qsize() > 0:
                    try:
                        """中间人那里有任务,获取任务，并且提交为一个运行态的worker"""
                        distribut = broker.get(block=False)
                        if isinstance(distribut, tuple):
                            all_tasks.append(executor.submit(task, *distribut))
                        elif isinstance(distribut, dict):
                            all_tasks.append(executor.submit(task, **distribut))
                        else:
                            all_tasks.append(executor.submit(task,distribut))
                    except Empty as e:
                        # print("broker have not task")
                        continue
                else:
                    continue

    @staticmethod
    async def work_in_async(executor, func: Callable, *args, async_loop: AbstractEventLoop = None, **kwargs):
        """
        将同步函数封装为异步函数
        :param executor:future,支持线程池实例
        :param func:
        :param args:
        :param async_loop:
        :param kwargs:
        :return:
        """
        async_loop = async_loop or asyncio.get_event_loop()
        async_worker = await async_loop.run_in_executor(executor, partial(func, *args, **kwargs))
        return async_worker
