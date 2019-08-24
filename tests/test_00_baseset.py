#! /usr/bin/env python3

"""Testset of small elementary requests"""

import json, requests

def test_f():

    s = requests.Session()

    # POST
    db_orig = json.load(open('data/baseset/db_orig.json'))
    r = s.post('http://0.0.0.0:8080/imports', json=db_orig)
    assert r.status_code == 201
    test_response = r.json()

    # read sample response, save import_id
    sample_response = json.load(open('data/baseset/post_response.json'))
    import_id = test_response['data']['import_id']
    sample_response['data']['import_id'] = import_id

    assert test_response == sample_response

    # GET percentile
    r = s.get(
        f'http://0.0.0.0:8080/imports/{import_id}/towns/stat/percentile/age')
    assert r.status_code == 200
    test_response = r.json()

    # read sample response
    sample_response = json.load(open('data/baseset/percentile.json'))

    assert test_response == sample_response

    # PATCH wedding
    patch_request = json.load(open('data/baseset/patch_wedding.json'))
    r = s.patch(f'http://0.0.0.0:8080/imports/{import_id}/citizens/3',
                json=patch_request)
    assert r.status_code == 200
    test_response = r.json()['data']

    # read sample response
    sample_response = json.load(open('data/baseset/mariya_married.json'))

    assert test_response == sample_response

    # GET
    r = s.get(f'http://0.0.0.0:8080/imports/{import_id}/citizens')
    assert r.status_code == 200
    test_response = r.json()['data']

    # read sample response
    sample_response = json.load(open('data/baseset/db_big_family.json'))

    assert test_response == sample_response

    # GET donators distribution by months
    r = s.get(f'http://0.0.0.0:8080/imports/{import_id}/citizens/birthdays')
    assert r.status_code == 200
    test_response = r.json()['data']

    # read sample response
    sample_response = json.load(open('data/baseset/months_distr.json'))

    assert test_response == sample_response

    # PATCH divorce
    patch_request = json.load(open('data/baseset/patch_divorce.json'))
    r = s.patch(f'http://0.0.0.0:8080/imports/{import_id}/citizens/3',
                json=patch_request)
    assert r.status_code == 200
    test_response = r.json()['data']

    # read sample response
    sample_response = json.load(open('data/baseset/mariya_divorced.json'))

    assert test_response == sample_response

    # GET all in Moscow, but divorced
    r = s.get(f'http://0.0.0.0:8080/imports/{import_id}/citizens')
    assert r.status_code == 200
    test_response = r.json()['data']

    # read sample response
    sample_response = json.load(open('data/baseset/db_finally.json'))

    assert test_response == sample_response

if __name__ == '__main__':
    test_f()

