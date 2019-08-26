#! /usr/bin/env python3

"""Testset of serveral asynchronous POST requests"""

import json, requests, time, asyncio, aiohttp

def order_json(obj):
    if isinstance(obj, dict):
        return sorted((k, order_json(v)) for k, v in obj.items())
    if isinstance(obj, list):
        return sorted(order_json(x) for x in obj)
    else:
        return obj

async def post_get(serv_addr, s):

    # POST
    db_orig = json.load(open('data/post_sequential/writers_orig.json'))
    r = await s.post(f'{serv_addr}', json=db_orig)
    assert r.status == 201
    test_response = await r.json()

    # read sample response, save import_id
    sample_response = json.load(open('data/post_sequential/post_resp.json'))
    import_id = test_response['data']['import_id']
    sample_response['data']['import_id'] = import_id
    assert test_response == sample_response

    # GET
    r = await s.get(f'{serv_addr}/{import_id}/citizens')
    assert r.status == 200
    test_response = (await r.json())['data']
    assert order_json(test_response) == order_json(db_orig['citizens'])

async def batch_post_get(serv_addr):
    async with aiohttp.ClientSession() as s:
        tasks = [asyncio.ensure_future(post_get(serv_addr, s)) for _ in range(10)]
        await asyncio.gather(*tasks)

def test_f():

    s = requests.Session()
    serv_addr = 'http://0.0.0.0:8080/imports'

    # send PATCH requests and GET for check data correctness
    t = time.time()

    loop = asyncio.new_event_loop()
    loop.run_until_complete(batch_post_get(serv_addr))
    loop.close()

    print('post+get:', round(((time.time() - t)*1000)/10, 3), 'ms ', end='')


if __name__ == '__main__':
    test_f()

