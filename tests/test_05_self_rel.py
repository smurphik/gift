#! /usr/bin/env python3

"""Tests with single citizen (U-Yanus and A-Yanus in one person)"""

import json, requests

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


    # POST
    db_orig = json.load(open('data/self_rel/post.json'))
    r = s.post(f'{serv_addr}', json=db_orig)
    assert r.status_code == 201
    test_response = r.json()

    # read sample response, save import_id
    sample_response = json.load(open('data/self_rel/post_response.json'))
    import_id = test_response['data']['import_id']
    sample_response['data']['import_id'] = import_id
    assert test_response == sample_response


    # GET percentile
    r = s.get(f'{serv_addr}/{import_id}/towns/stat/percentile/age')
    assert r.status_code == 200
    test_response = r.json()

    # read sample response
    sample_response = json.load(open('data/self_rel/get_perc.json'))
    assert order_json(test_response) == order_json(sample_response)


    # GET donators distribution by months
    r = s.get(f'{serv_addr}/{import_id}/citizens/birthdays')
    assert r.status_code == 200
    test_response = r.json()['data']

    # read sample response
    sample_response = json.load(open('data/self_rel/get_donators0.json'))
    assert order_json(test_response) == order_json(sample_response)


    # PATCH destruction
    patch_request = json.load(open('data/self_rel/patch_destruction.json'))
    r = s.patch(f'{serv_addr}/{import_id}/citizens/1000', json=patch_request)
    assert r.status_code == 200
    test_response = r.json()['data']

    # read sample response
    sample_response = json.load(open('data/self_rel/yanus_destructed.json'))
    assert test_response == sample_response


    # GET
    r = s.get(f'{serv_addr}/{import_id}/citizens')
    assert r.status_code == 200
    test_response = r.json()['data']

    # read sample response
    assert order_json(test_response) == order_json([sample_response])


    # GET donators distribution by months
    r = s.get(f'{serv_addr}/{import_id}/citizens/birthdays')
    assert r.status_code == 200
    test_response = r.json()['data']

    # read sample response
    sample_response = json.load(open('data/self_rel/get_donators1.json'))
    assert order_json(test_response) == order_json(sample_response)


    # PATCH reconstruction
    patch_request = json.load(open('data/self_rel/patch_reconstruction.json'))
    r = s.patch(f'{serv_addr}/{import_id}/citizens/1000', json=patch_request)
    assert r.status_code == 200
    test_response = r.json()['data']

    # read sample response
    sample_response = db_orig['citizens'][0]
    assert test_response == sample_response


if __name__ == '__main__':
    test_f()

