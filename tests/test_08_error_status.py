#! /usr/bin/env python3

"""Testset of serveral erroneous requests"""

import json, requests

def order_json(obj):
    if isinstance(obj, dict):
        return sorted((k, order_json(v)) for k, v in obj.items())
    if isinstance(obj, list):
        return sorted(order_json(x) for x in obj)
    else:
        return obj

def post400(s, serv_addr, field, value, status=400):
    db_orig = json.load(open('data/error_status/post_correct.json'))
    db_orig['citizens'][0][field] = value
    r = s.post(f'{serv_addr}', json=db_orig)
    if r.status_code != status:
        print(r.status_code)
        print(r.json())
    assert r.status_code == status

def patch400(s, serv_addr, import_id, field, value, status=400):

    db_orig = json.load(open('data/error_status/post_correct.json'))
    cit = db_orig['citizens'][0]
    cit[field] = value
    citizen_id = cit['citizen_id']
    del cit['citizen_id']

    r = s.patch(f'{serv_addr}/{import_id}/citizens/1', json=cit)
    if r.status_code != status:
        print(r.status_code)
        print(r.json())
    assert r.status_code == status
    if r.status_code == 200:
        cit['citizen_id'] = citizen_id
        assert r.json()['data'] == cit

def test_f():

    s = requests.Session()
    serv_addr = 'http://0.0.0.0:8080/imports'


    post400(s, serv_addr, 'relatives', None)
    post400(s, serv_addr, 'relatives', '')
    post400(s, serv_addr, 'relatives', [4])
    post400(s, serv_addr, 'relatives', ['lizard'])
    post400(s, serv_addr, 'relatives', [-1])

    post400(s, serv_addr, 'birth_date', None)
    post400(s, serv_addr, 'birth_date', '')
    post400(s, serv_addr, 'birth_date', '.')
    post400(s, serv_addr, 'birth_date', '..')
    post400(s, serv_addr, 'birth_date', '1.01.2019')
    post400(s, serv_addr, 'birth_date', 13)
    post400(s, serv_addr, 'birth_date', '2019.01.01')
    post400(s, serv_addr, 'birth_date', '01.2019.01')
    post400(s, serv_addr, 'birth_date', ' 01.01.2019')
    post400(s, serv_addr, 'birth_date', '01.01.2019 ')

    post400(s, serv_addr, 'name', None)
    post400(s, serv_addr, 'name', '')
    post400(s, serv_addr, 'name', ' ')
    post400(s, serv_addr, 'name', '*', status=201)

    post400(s, serv_addr, 'town', None)
    post400(s, serv_addr, 'town', '')
    post400(s, serv_addr, 'town', ' ~.*-/')
    post400(s, serv_addr, 'town', ' ~.*-/7', status=201)

    post400(s, serv_addr, 'gender', None)
    post400(s, serv_addr, 'gender', '')
    post400(s, serv_addr, 'gender', 'male', status=201)
    post400(s, serv_addr, 'gender', 'female', status=201)

    post400(s, serv_addr, 'building', None)
    post400(s, serv_addr, 'building', '')
    post400(s, serv_addr, 'building', 15)
    post400(s, serv_addr, 'building', '15', status=201)
    post400(s, serv_addr, 'building', 'a', status=201)

    post400(s, serv_addr, 'apartment', None)
    post400(s, serv_addr, 'apartment', '')
    post400(s, serv_addr, 'apartment', 0, status=201)
    post400(s, serv_addr, 'apartment', -1)
    post400(s, serv_addr, 'apartment', '1')


    # POST correct
    db_orig = json.load(open('data/error_status/post_correct1.json'))
    r = s.post(f'{serv_addr}', json=db_orig)
    if r.status_code != 201:
        print(r.status_code)
        print(r.json())
    assert r.status_code == 201
    import_id = r.json()['data']['import_id']


    patch400(s, serv_addr, import_id, 'relatives', None)
    patch400(s, serv_addr, import_id, 'relatives', '')
    patch400(s, serv_addr, import_id, 'relatives', [4])
    patch400(s, serv_addr, import_id, 'relatives', ['lizard'])
    patch400(s, serv_addr, import_id, 'relatives', [-1])

    patch400(s, serv_addr, import_id, 'birth_date', None)
    patch400(s, serv_addr, import_id, 'birth_date', '')
    patch400(s, serv_addr, import_id, 'birth_date', '.')
    patch400(s, serv_addr, import_id, 'birth_date', '..')
    patch400(s, serv_addr, import_id, 'birth_date', '1.01.2019')
    patch400(s, serv_addr, import_id, 'birth_date', 13)
    patch400(s, serv_addr, import_id, 'birth_date', '2019.01.01')
    patch400(s, serv_addr, import_id, 'birth_date', '01.2019.01')
    patch400(s, serv_addr, import_id, 'birth_date', ' 01.01.2019')
    patch400(s, serv_addr, import_id, 'birth_date', '01.01.2019 ')

    patch400(s, serv_addr, import_id, 'name', None)
    patch400(s, serv_addr, import_id, 'name', '')
    patch400(s, serv_addr, import_id, 'name', ' ')
    patch400(s, serv_addr, import_id, 'name', '*', status=200)

    patch400(s, serv_addr, import_id, 'town', None)
    patch400(s, serv_addr, import_id, 'town', '')
    patch400(s, serv_addr, import_id, 'town', ' ~.*-/')
    patch400(s, serv_addr, import_id, 'town', ' ~.*-/7', status=200)

    patch400(s, serv_addr, import_id, 'gender', None)
    patch400(s, serv_addr, import_id, 'gender', '')
    patch400(s, serv_addr, import_id, 'gender', 'male', status=200)
    patch400(s, serv_addr, import_id, 'gender', 'female', status=200)

    patch400(s, serv_addr, import_id, 'building', None)
    patch400(s, serv_addr, import_id, 'building', '')
    patch400(s, serv_addr, import_id, 'building', 15)
    patch400(s, serv_addr, import_id, 'building', '15', status=200)
    patch400(s, serv_addr, import_id, 'building', 'a', status=200)

    patch400(s, serv_addr, import_id, 'apartment', None)
    patch400(s, serv_addr, import_id, 'apartment', '')
    patch400(s, serv_addr, import_id, 'apartment', 0, status=200)
    patch400(s, serv_addr, import_id, 'apartment', -1)
    patch400(s, serv_addr, import_id, 'apartment', '1')


    # PATCH wrong citizen_id
    db_orig = json.load(open('data/error_status/post_correct.json'))
    cit = db_orig['citizens'][0]
    del cit['citizen_id']
    r = s.patch(f'{serv_addr}/{import_id}/citizens/1000', json=cit)
    assert r.status_code == 404
    r = s.patch(f'{serv_addr}/{import_id}/citizens/0', json=cit)
    assert r.status_code == 404
    r = s.patch(f'{serv_addr}/{import_id}/citizens/-1', json=cit)
    assert r.status_code == 404


    # GET wrong citizen_id
    wrong_import_id = import_id + 1000
    r = s.get(f'{serv_addr}/{wrong_import_id}/citizens')
    assert r.status_code == 404
    r = s.get(f'{serv_addr}/{wrong_import_id}/citizens/birthdays')
    assert r.status_code == 404
    r = s.get(f'{serv_addr}/wrong_{import_id}/towns/stat/percentile/age')
    assert r.status_code == 404


if __name__ == '__main__':
    test_f()

