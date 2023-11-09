import asyncio
import random
from typing import Union

from fastapi import FastAPI
from pydantic import BaseModel

app = FastAPI()


class Payload(BaseModel):
    user_id: int

@app.post("/items/get_items")
async def read_item(data: Payload) -> dict:
    return {'item_ids': [1, 2, 3]}


@app.get('/someservice1/{item_id}')
async def some_service1(item_id: int) -> dict:
    return {'item_id': item_id + random.Random().randint(1, 1000)}


@app.get('/someservice2/{item_id}')
async def some_service1(item_id: int) -> dict:
    return {'item_id': item_id + random.Random().randint(1, 1000)}


@app.get('/someservice3/{item_id}')
async def some_service1(item_id: int) -> dict:
    return {'item_id': item_id + random.Random().randint(1, 1000)}
