import asyncio
import time

from concurrent.futures import ProcessPoolExecutor

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
service_1 = 'http://127.0.0.1:8000/someservice1/'
service_2 = 'http://127.0.0.1:8000/someservice2/'
service_3 = 'http://127.0.0.1:8000/someservice3/'


def retry(max_attempts: int = 3, initial_delay: float = 1.0, max_delay: float = 60.0, exceptions=(Exception,)):
    def decorator(func):
        async def wrapper(*args, **kwargs):
            delay = initial_delay
            for attempt in range(1, max_attempts + 1):
                print(attempt)
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
    for i in range(1):
        requests_samples.append(('http://127.0.0.1:8000/items/get_items/', {'user_id': i}))


def business_logic(service1_response, service2_response, service3_response) -> dict:
    result = service1_response['item_id'] + service2_response['item_id'] + service3_response['item_id']
    return result


@retry(max_attempts=3)
async def fetch_json(session: aiohttp.ClientSession, url: str, params: dict = None) -> dict:
    async with session.post(url, json=params) as response:
        return await response.json()


@retry(max_attempts=3)
async def fetch_json_get(session: aiohttp.ClientSession, url: str, item_id) -> dict:
    async with session.get(f'{url}{item_id}') as response:
        return await response.json()


async def process_url(session, semaphore, executor, request_data):
    async with semaphore:
        items_response = await fetch_json(session, service_0, request_data[1])
        item_ids = items_response['item_ids']

        service1_task = fetch_json_get(session, service_1, item_ids[0])
        service2_task = fetch_json_get(session, service_2, item_ids[1])
        service3_task = fetch_json_get(session, service_3, item_ids[2])

        service1_response, service2_response, service3_response = await asyncio.gather(
            service1_task, service2_task, service3_task
        )

        loop = asyncio.get_running_loop()
        result = await loop.run_in_executor(
            executor, business_logic, service1_response, service2_response, service3_response
        )

        return result


async def gather_data():
    results = []
    semaphore = asyncio.Semaphore(MAX_TASKS)

    with ProcessPoolExecutor(MAX_WORKERS) as executor:
        async with aiohttp.ClientSession() as session:
            for i in range(0, len(requests_samples), CHUNK_SIZE):
                chunk = requests_samples[i:i + CHUNK_SIZE]
                tasks = [process_url(session, semaphore, executor, request_data) for request_data in chunk]
                chunk_results = await asyncio.gather(*tasks)
                results.extend(chunk_results)


if __name__ == '__main__':
    generate_requests()
    start_time = time.perf_counter_ns()
    asyncio.run(gather_data())
    print(f'Время выполнения: {time.perf_counter_ns() - start_time} сек.')
