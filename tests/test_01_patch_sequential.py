#! /usr/bin/env python3

"""Testset of many sequential PATCH requests"""

import json, requests, time

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

def test_f():

    s = requests.Session()

    # POST
    db_orig = json.load(open('data/patch_sequential/writers_orig.json'))
    r = s.post('http://0.0.0.0:8080/imports', json=db_orig)
    assert r.status_code == 201
    test_response = r.json()

    # read sample response, save import_id
    sample_response = json.load(open('data/patch_sequential/post_resp.json'))
    import_id = test_response['data']['import_id']
    sample_response['data']['import_id'] = import_id

    assert test_response == sample_response

    # GET
    r = s.get(f'http://0.0.0.0:8080/imports/{import_id}/citizens')
    assert r.status_code == 200
    test_response = r.json()['data']

    assert test_response == db_orig['citizens']

    # check relations correctness
    assert check_rels(db_orig['citizens'])

    # decide new relations
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

    t = time.time()
    # send PATCH requests and GET for check data correctness
    for i, rels in enumerate(new_rels, 1):
        # TODO: PATCH
        pass

        # TODO: GET
        #assert check_rels(db_orig['citizens'])
    print(time.time() - t)


# TODO: в baseset добавить patch без relatives

if __name__ == '__main__':
    test_f()

