#! /usr/bin/env python3

"""Testset of small elementary requests"""

import pytest, json, requests, pymysql

def clean_tables(numeric_id):
    """Delete tables created by testing"""

    conn = pymysql.connect(host='localhost', port=3306, user='gift_server',
                           password='Qwerty!0', db='gift_db')

    with conn.cursor() as cursor:
        cursor.execute(f'DROP TABLE import_{numeric_id};')
        cursor.execute(f'DROP TABLE rel_{numeric_id};')
        cursor.execute(f'DELETE FROM unique_ids_table WHERE id = {numeric_id};')

    conn.commit()

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

    # PATCH
    patch_request = json.load(open('data/baseset/patch_request.json'))
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

    # clean database
    clean_tables(import_id)

if __name__ == '__main__':
    test_f()
