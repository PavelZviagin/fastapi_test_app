import asyncio
import time

import aiohttp
from typing import List

requests_samples = [
    ('http://127.0.0.1:8000/items/get_items/', {'user_id': 100}),  # вернет {'item_ids': [1, 2, 3]}
    ('http://127.0.0.1:8000/items/get_items/', {'user_id': 101}),
    # ...
]

service_1 = 'http://127.0.0.1:8000/someservice1/'
service_2 = 'http://127.0.0.1:8000/someservice2/'
service_3 = 'http://127.0.0.1:8000/someservice3/'


def generate_requests():
    for i in range(10_000):
        requests_samples.append(('http://127.0.0.1:8000/items/get_items/', {'user_id': i}))


def business_logic(service1_response, service2_response, service3_response) -> dict:
    result = service1_response['item_id'] + service2_response['item_id'] + service3_response['item_id']
    return result


async def fetch_json(session: aiohttp.ClientSession, url: str, params: dict = None) -> dict:
    async with session.post(url, json=params) as response:
        return await response.json()


async def fetch_json_get(session: aiohttp.ClientSession, url: str, item_id) -> dict:
    async with session.get(f'{url}{item_id}') as response:
        return await response.json()


async def process_item(item_id: int, session: aiohttp.ClientSession) -> dict:
    response1 = await fetch_json_get(session, service_1, item_id)
    response2 = await fetch_json_get(session, service_2, item_id)
    response3 = await fetch_json_get(session, service_3, item_id)

    return business_logic(response1, response2, response3)


async def gather_data(requests: List[tuple]) -> List[dict]:
    results = []
    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit=50)) as session:
        for url, params in requests:
            item_ids_response = await fetch_json(session, url, params)
            item_ids = item_ids_response.get('item_ids', [])

            tasks = [process_item(item_id, session) for item_id in item_ids]
            results.extend(await asyncio.gather(*tasks))

    return results


if __name__ == '__main__':
    generate_requests()
    start_time = time.time()
    asyncio.run(gather_data(requests_samples))
    print(f'Время выполнения: {time.time() - start_time} сек.')
