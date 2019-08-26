#! /usr/bin/env python3

"""Testset of one big POST request (10500 rows)"""

import json, requests
from time import time

def order_json(obj):
    if isinstance(obj, dict):
        return sorted((k, order_json(v)) for k, v in obj.items())
    if isinstance(obj, list):
        return sorted(order_json(x) for x in obj)
    else:
        return obj

def test_f():

    s = requests.Session()
    serv_addr = 'http://0.0.0.0:8080/imports'

    # prepare big import
    db_orig = json.load(open('data/post_sequential/writers_orig.json'))
    db_orig = db_orig['citizens']
    N = len(db_orig)
    db_big = []
    for i in range(500):
        new_batch = []
        for cit in db_orig:
            new_cit = dict(cit)
            new_cit['citizen_id'] += N*i
            new_cit['relatives'] = [(j + N*i) for j in new_cit['relatives']]
            new_batch.append(new_cit)
        db_big += new_batch

    t_post = time()

    # POST
    r = s.post(f'{serv_addr}', json={'citizens': db_big})
    assert r.status_code == 201
    test_response = r.json()['data']

    # read sample response, save import_id
    import_id = test_response['import_id']
    assert test_response == {'import_id': import_id}

    t_post = time() - t_post
    t_get = time()

    # GET
    r = s.get(f'{serv_addr}/{import_id}/citizens')
    assert r.status_code == 200
    test_response = r.json()['data']

    # read sample response
    assert order_json(test_response) == order_json(db_big)

    t_get = time() - t_get
    print('post: {} sec, get: {} sec '.format(
        round(t_post, 3), round(t_get, 3)), end='')


if __name__ == '__main__':
    test_f()

