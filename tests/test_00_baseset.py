#! /usr/bin/env python3

"""Testset of small elementary requests"""

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
    db_orig = json.load(open('data/baseset/db_orig.json'))
    r = s.post(f'{serv_addr}', json=db_orig)
    assert r.status_code == 201
    test_response = r.json()

    # read sample response, save import_id
    sample_response = json.load(open('data/baseset/post_response.json'))
    import_id = test_response['data']['import_id']
    sample_response['data']['import_id'] = import_id
    assert test_response == sample_response


    # GET percentile
    r = s.get(f'{serv_addr}/{import_id}/towns/stat/percentile/age')
    assert r.status_code == 200
    test_response = r.json()

    # read sample response
    sample_response = json.load(open('data/baseset/percentile.json'))
    assert order_json(test_response) == order_json(sample_response)


    # PATCH wedding
    patch_request = json.load(open('data/baseset/patch_wedding.json'))
    r = s.patch(f'{serv_addr}/{import_id}/citizens/3', json=patch_request)
    assert r.status_code == 200
    test_response = r.json()['data']

    # read sample response
    sample_response = json.load(open('data/baseset/mariya_married.json'))
    assert test_response == sample_response


    # GET
    r = s.get(f'{serv_addr}/{import_id}/citizens')
    assert r.status_code == 200
    test_response = r.json()['data']

    # read sample response
    sample_response = json.load(open('data/baseset/db_big_family.json'))
    assert order_json(test_response) == order_json(sample_response)


    # GET donators distribution by months
    r = s.get(f'{serv_addr}/{import_id}/citizens/birthdays')
    assert r.status_code == 200
    test_response = r.json()['data']

    # read sample response
    sample_response = json.load(open('data/baseset/months_distr.json'))
    assert test_response == sample_response


    # PATCH divorce
    patch_request = json.load(open('data/baseset/patch_divorce.json'))
    r = s.patch(f'{serv_addr}/{import_id}/citizens/3', json=patch_request)
    assert r.status_code == 200
    test_response = r.json()['data']

    # read sample response
    sample_response = json.load(open('data/baseset/mariya_divorced.json'))
    assert test_response == sample_response


    # GET all in Moscow, but divorced
    r = s.get(f'{serv_addr}/{import_id}/citizens')
    assert r.status_code == 200
    test_response = r.json()['data']

    # read sample response
    sample_response = json.load(open('data/baseset/db_finally.json'))
    assert order_json(test_response) == order_json(sample_response)


    # PATCH migration
    patch_request = json.load(open('data/baseset/patch_migration.json'))
    r = s.patch(f'{serv_addr}/{import_id}/citizens/3', json=patch_request)
    assert r.status_code == 200
    test_response = r.json()['data']

    # read sample response
    sample_response = json.load(open('data/baseset/mariya_migrated.json'))
    assert test_response == sample_response


    # PATCH empty
    r = s.patch(f'{serv_addr}/{import_id}/citizens/3', json={})
    assert r.status_code == 200
    test_response = r.json()['data']

    # read sample response
    sample_response = json.load(open('data/baseset/mariya_migrated.json'))
    assert test_response == sample_response


if __name__ == '__main__':
    test_f()

