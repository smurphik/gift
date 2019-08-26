#! /usr/bin/env python3

"""Testset of many asynchronous PATCH requests"""

import json, requests, time, asyncio, aiohttp

def order_json(obj):
    if isinstance(obj, dict):
        return sorted((k, order_json(v)) for k, v in obj.items())
    if isinstance(obj, list):
        return sorted(order_json(x) for x in obj)
    else:
        return obj

def check_rels(citizens):
    rels = set()
    for cit in citizens:
        for r in cit['relatives']:
            rels.add((cit['citizen_id'], r))
    while rels:
        pair = rels.pop()
        inv_pair = pair[::-1]
        if inv_pair not in rels:
            return False
        rels.remove(inv_pair)
    return True

async def patch_get(cit, rels, addr, s):

    # PATCH
    r = await s.patch(f'{addr}/citizens/{cit}', json={'relatives': rels})
    assert r.status == 200

    # GET
    r = await s.get(f'{addr}/citizens')
    assert r.status == 200
    test_response = (await r.json())['data']
    assert check_rels(test_response)

async def batch_patch_get(new_rels, addr):
    async with aiohttp.ClientSession() as s:
        tasks = [asyncio.ensure_future(patch_get(i, r, addr, s)) for i, r in enumerate(new_rels, 1)]
        await asyncio.gather(*tasks)

def test_f():

    s = requests.Session()
    serv_addr = 'http://0.0.0.0:8080/imports'

    # POST
    db_orig = json.load(open('data/patch_sequential/writers_orig.json'))
    r = s.post(f'{serv_addr}', json=db_orig)
    assert r.status_code == 201
    test_response = r.json()

    # read sample response, save import_id
    sample_response = json.load(open('data/patch_sequential/post_resp.json'))
    import_id = test_response['data']['import_id']
    sample_response['data']['import_id'] = import_id

    assert test_response == sample_response

    # GET
    r = s.get(f'{serv_addr}/{import_id}/citizens')
    assert r.status_code == 200
    test_response = r.json()['data']

    assert order_json(test_response) == order_json(db_orig['citizens'])

    # check relations correctness
    assert check_rels(db_orig['citizens'])

    # decide new relations (pseudorandom changes)
    new_rels = []
    N = len(db_orig['citizens'])
    for i in range(1, N+1):
        if i % 3 == 0:
            inv_mask = (True, False, True, True)
        elif i % 3 == 1:
            inv_mask = (True, True, False, True)
        elif i % 2:
            inv_mask = (False, True, False, True)
        else:
            inv_mask = (True, False, True, False)
        rels = list(db_orig['citizens'][i-1]['relatives'])
        for j, f in zip([i-2, i-1, i+1, i+2], inv_mask):
            # check borders
            if j < 1 or j > 21:
                continue
            # invert relation
            if f:
                if j in rels:
                    rels.remove(j)
                else:
                    rels.append(j)
        new_rels.append(rels)

    # send PATCH requests and GET for check data correctness
    t = time.time()
    addr = f'{serv_addr}/{import_id}'

    loop = asyncio.new_event_loop()
    loop.run_until_complete(batch_patch_get(new_rels, addr))
    loop.close()

    print('patch+get:', round(((time.time() - t)*1000)/len(new_rels), 3),
          'ms ', end='')


if __name__ == '__main__':
    test_f()

