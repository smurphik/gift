#! /usr/bin/env python3

"""Tests with empty import"""

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
    r = s.post(f'{serv_addr}', json={'citizens': []})
    assert r.status_code == 201
    test_response = r.json()['data']

    # read sample response, save import_id
    import_id = test_response['import_id']
    assert test_response == {'import_id': import_id}


    # GET
    r = s.get(f'{serv_addr}/{import_id}/citizens')
    assert r.status_code == 200
    test_response = r.json()['data']

    # read sample response
    assert test_response == []


    # GET donators distribution by months
    r = s.get(f'{serv_addr}/{import_id}/citizens/birthdays')
    assert r.status_code == 200
    test_response = r.json()['data']

    # read sample response
    sample_response = {str(i): [] for i in range(1, 13)}
    assert order_json(test_response) == order_json(sample_response)


    # GET percentile
    r = s.get(f'{serv_addr}/{import_id}/towns/stat/percentile/age')
    assert r.status_code == 200
    test_response = r.json()['data']

    # read sample response
    assert test_response == []


if __name__ == '__main__':
    test_f()

