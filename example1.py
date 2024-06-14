import asyncio
import time
from asyncio import Queue

from concurrent.futures import ThreadPoolExecutor
from typing import Optional, Any

import aiohttp

MAX_TASKS = 64
MAX_WORKERS = 3
CHUNK_SIZE = 1

requests_samples = [
    ('http://127.0.0.1:8000/items/get_items/', {'user_id': 100}),  # вернет {'item_ids': [1, 2, 3]}
    ('http://127.0.0.1:8000/items/get_items/', {'user_id': 101}),
    # ...
]

service_0 = "http://127.0.0.1:8000/items/get_items/"

services = [
    'http://127.0.0.1:8000/someservice1/',
    'http://127.0.0.1:8000/someservice2/',
    'http://127.0.0.1:8000/someservice3/',
]


def retry(max_attempts: int = 3, initial_delay: float = 1.0, max_delay: float = 60.0, exceptions=(Exception,)):
    def decorator(func):
        async def wrapper(*args, **kwargs):
            delay = initial_delay
            for attempt in range(1, max_attempts + 1):
                try:
                    return await func(*args, **kwargs)
                except exceptions as e:
                    if attempt == max_attempts:
                        raise
                    await asyncio.sleep(delay)
                    delay = min(delay * 2, max_delay)

        return wrapper

    return decorator


def generate_requests():
    for i in range(2):
        requests_samples.append(('http://127.0.0.1:8000/items/get_items/', {'user_id': i}))


def business_logic(service1_response, service2_response, service3_response) -> dict:
    result = service1_response['item_id'] + service2_response['item_id'] + service3_response['item_id']
    return result


@retry(max_attempts=3)
async def fetch_json(session: aiohttp.ClientSession, url: str, params: Optional[dict[str, Any]] = None) -> dict:
    async with session.post(url, json=params) as response:
        return await response.json()


@retry(max_attempts=3)
async def fetch_json_get(session: aiohttp.ClientSession, url: str, item_id) -> dict:
    async with session.get(f'{url}{item_id}') as response:
        return await response.json()


class Executor:

    def __init__(self, urls, max_workers=10):
        self._urls = urls
        self._queue = Queue(max_workers)
        self._max_workers = max_workers
        self._results = []
        self._session = aiohttp.ClientSession()

    async def _put_queue(self):
        for url in self._urls:
            await self._queue.put(url)
        await self._queue.join()

    async def execute(self):
        tasks = [asyncio.create_task(self._work()) for _ in range(self._max_workers)]
        tasks.append(asyncio.create_task(self._put_queue()))

        done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)

        for task in pending:
            task.cancel()

        await self._session.close()
        return self._results

    async def _work(self):
        while True:
            task = await self._queue.get()
            url = task[0]
            user_id = task[1]

            ids = await fetch_json(self._session, url, user_id)
            ids = ids['item_ids']

            async with asyncio.TaskGroup() as tg:
                srv1 = tg.create_task(fetch_json_get(self._session, services[0], ids[0]))
                srv2 = tg.create_task(fetch_json_get(self._session, services[1], ids[1]))
                srv3 = tg.create_task(fetch_json_get(self._session, services[2], ids[2]))

            self._results.append(business_logic(srv1.result(), srv2.result(), srv3.result()))
            self._queue.task_done()


async def main():
    executor = Executor(requests_samples, 1)
    results = await executor.execute()
    print(results)


if __name__ == '__main__':
    generate_requests()
    start_time = time.perf_counter_ns()
    asyncio.run(main())
    print(f'Время выполнения: {time.perf_counter_ns() - start_time} сек.')
