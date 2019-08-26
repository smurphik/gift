#! /usr/bin/env python3

"""Testset of many sequential POST requests"""

import json, requests, time

def make_post(db_orig, session):

    # POST
    db_orig = json.load(open('data/patch_sequential/writers_orig.json'))
    r = session.post('http://0.0.0.0:8080/imports', json=db_orig)
    assert r.status_code == 201
    test_response = r.json()

    # read sample response, save import_id
    sample_response = json.load(open('data/patch_sequential/post_resp.json'))
    import_id = test_response['data']['import_id']
    sample_response['data']['import_id'] = import_id

    assert test_response == sample_response

def test_f():

    s = requests.Session()
    db_orig = json.load(open('data/patch_sequential/writers_orig.json'))

    # heat up
    for _ in range(5):
        make_post(db_orig, s)

    # send PATCH requests and GET for check data correctness
    t = time.time()
    for _ in range(10):
        make_post(db_orig, s)
    print('post:', round((time.time() - t)*100, 3), 'ms ', end='')

if __name__ == '__main__':
    test_f()

