#! /usr/bin/env python3

"""
One-thread asynchronous server for storage and analysis data on citizens
"""

from aiohttp import web
import aiomysql, asyncio, sys, pymysql, traceback
import json

# A shareable counter for providing unique id for data tables
glob_id = 0

citizen_fields = ['citizen_id', 'town', 'street', 'building',
                  'apartment', 'name', 'birth_date', 'gender']

# This is a peculiar implementation of a shared counter using
# variable `glob_id`, datatable `unique_ids_table` and
# functions `init_global_id` and `update_global_id`.
#
# `init_global_id` - read last used id from last server run
#                    and write it to `glob_id`
# `update_global_id` - increment `glob_id`, save it for future
#                      `init_global_id` and retern it
#
# N.B.: Of course, table `unique_ids_table` is not necessary. At the server
# start, we can simply find the maximum index among the table names `import_X`.
# But I think it's not very conceptual. In the future, the customer may request
# the deletion of the data tables. Or the administrator can delete some tables
# for some reasons. This may cause duplicating indexes for different tables on
# the client-side. The table `unique_ids_table` helps to avoid this duplication.
def init_global_id():
    """Table global counter initialization"""

    try:
        conn = pymysql.connect(host='localhost', port=3306, user='gift_server',
                               password='Qwerty!0', db='gift_db',
                               autocommit=True)
    except Exception:
        print('FAIL: Make sure that you have configured the database '
              '"gift_db" according to the README')
        sys.exit()

    with conn.cursor() as cursor:
        try:
            # read last unique id
            cursor.execute('SELECT MAX(id) FROM unique_ids_table;')
            return int(cursor.fetchone()[0])
        except Exception:
            # create id-table and insert zero
            cursor.execute('CREATE TABLE unique_ids_table (id int);')
            cursor.execute('INSERT INTO unique_ids_table VALUES(0);')
            return 0

# See comment to `init_global_id()`
async def update_global_id(conn):
    """Getting unique id for new table"""

    global glob_id
    glob_id += 1
    async with conn.cursor() as cursor:
        await cursor.execute(f'INSERT INTO unique_ids_table VALUES({glob_id});')
    return glob_id

def check_value(key, value): # TODO
    pass

async def store_import(request):
    """Handle /imports POST-request"""

    try:
        post_obj = await request.json()

        # Check data
        # TODO: status 400

        pool = await aiomysql.create_pool(
            host='localhost', port=3306, user='gift_server',
            password='Qwerty!0', db='gift_db', loop=loop, charset='utf8')

        async with pool.acquire() as conn:

            numeric_id = await update_global_id(conn)
            import_id = 'import_' + str(numeric_id)
            rel_id = 'rel_' + str(numeric_id)

            async with conn.cursor() as cur:

                # Create table for citizen list
                await cur.execute(
                    f'CREATE TABLE {import_id} ('
                        'citizen_id int,'
                        'town       varchar(255),'
                        'street     varchar(255),'
                        'building   varchar(255),'
                        'apartment  int,'
                        'name       varchar(255),'
                        'birth_date varchar(255),'
                        'gender     varchar(255)'
                    ');'
                )

                # Fill table
                sql = ''
                for citizen_obj in post_obj['citizens']:
                    vals = []
                    for field in citizen_fields:
                        v = citizen_obj[field]
                        if isinstance(v, int):
                            vals.append(str(v))
                        else:
                            vals.append(f"'{v}'")
                    sql += 'INSERT INTO {} VALUES ({});'.format(import_id,
                                                                ', '.join(vals))
                if sql:
                    await cur.execute(sql)

                # Create table for citizens family relationship
                await cur.execute(f'CREATE TABLE {rel_id} (x int, y int);')

                # Fill table
                sql = ''
                for citizen_obj in post_obj['citizens']:
                    x = citizen_obj['citizen_id']
                    for y in citizen_obj['relatives']:
                        sql += f'INSERT INTO {rel_id} VALUES ({x}, {y});'
                if sql:
                    await cur.execute(sql)

            await conn.commit()

        pool.close()

        response_obj = {'data': {'import_id': numeric_id}}
        return web.json_response(response_obj, status=201)

    except Exception as e:
        traceback.print_exc()
        return web.json_response({'error': str(e)}, status=500)

async def alter_import(request):
    """Handle /imports/{numeric_id}/citizens/citizen_id PATCH-request"""

    try:
        patch_obj = await request.json()

        # Check data
        # TODO: status 400

        pool = await aiomysql.create_pool(
            host='localhost', port=3306, user='gift_server',
            password='Qwerty!0', db='gift_db', loop=loop, charset='utf8')

        async with pool.acquire() as conn:

            numeric_id = int(request.match_info['numeric_id'])
            import_id = 'import_' + str(numeric_id)
            rel_id = 'rel_' + str(numeric_id)
            citizen_id = int(request.match_info['citizen_id'])

            async with conn.cursor() as cur:

                # alter fields in table import_id
                vals = []
                for field, value in patch_obj.items():
                    if field == 'relatives':
                        continue
                    if isinstance(value, int):
                        vals.append(f'{field} = {value}')
                    else:
                        vals.append(f"{field} = '{value}'")
                await cur.execute(
                    f'UPDATE {import_id} ' +
                    'SET {} '.format(', '.join(vals)) +
                    f'WHERE citizen_id = {citizen_id};'
                )

                # alter relations in table import_id
                i = citizen_id
                sql = f'DELETE FROM {rel_id} WHERE x = {i} OR y = {i};'
                for j in patch_obj['relatives']:
                    sql += f'INSERT INTO {rel_id} VALUES ({i}, {j});'
                    sql += f'INSERT INTO {rel_id} VALUES ({j}, {i});'
                await cur.execute(sql)

                # control reading for response to client
                try:
                    await cur.execute(f'SELECT * FROM {import_id} ' +
                                      f'WHERE citizen_id = {citizen_id};')
                    response = await cur.fetchone()
                    citizen_obj = dict(zip(citizen_fields, response))
                    await cur.execute(f'SELECT y FROM {rel_id} ' +
                                      f'WHERE x = {citizen_id};')
                    rels = await cur.fetchone()
                    citizen_obj['relatives'] = list(rels) if rels else list()
                except Exception as e:
                    return web.json_response({'error': str(e)}, status=404)

            await conn.commit()

        pool.close()

        response_obj = {'data': citizen_obj}
        return web.json_response(response_obj, status=200)

    except Exception as e:
        traceback.print_exc()
        return web.json_response({'error': str(e)}, status=500)

async def load_import(request):
    """Handle /imports/{numeric_id}/citizens GET-request"""

    try:

        pool = await aiomysql.create_pool(
            host='localhost', port=3306, user='gift_server',
            password='Qwerty!0', db='gift_db', loop=loop, charset='utf8')

        async with pool.acquire() as conn:

            numeric_id = int(request.match_info['numeric_id'])
            import_id = 'import_' + str(numeric_id)
            rel_id = 'rel_' + str(numeric_id)

            async with conn.cursor() as cur:

                # read data from import_id
                try:
                    await cur.execute(f'SELECT * FROM {import_id};')
                except Exception as e:
                    return web.json_response({'error': str(e)}, status=404)
                response = await cur.fetchall()
                citizens_obj_list = []
                for r in response:
                    citizen_obj = dict(zip(citizen_fields, r))
                    citizens_obj_list.append(citizen_obj)
                response_obj = {'data': citizens_obj_list}

                # read data from rel_id
                try:
                    await cur.execute(f'SELECT * FROM {rel_id};')
                except Exception as e:
                    return web.json_response({'error': str(e)}, status=404)
                response = await cur.fetchall()
                rels = dict()
                for r in response:
                    i, j = r[0], r[1]
                    if i not in rels:
                        rels[i] = list()
                    rels[i].append(j)
                for citizen in response_obj['data']:
                    i = citizen['citizen_id']
                    if i in rels:
                        citizen['relatives'] = rels[i]
                    else:
                        citizen['relatives'] = []

        pool.close()

        return web.json_response(response_obj, status=200)

    except Exception as e:
        traceback.print_exc()
        return web.json_response({'error': str(e)}, status=500)


def main():

    global loop, glob_id

    loop = asyncio.get_event_loop()
    glob_id = init_global_id()

    app = web.Application()
    app.router.add_post('/imports', store_import)
    app.router.add_patch('/imports/{numeric_id:[0-9]+}/citizens/{citizen_id:[0-9]+}', alter_import)
    app.router.add_get('/imports/{numeric_id:[0-9]+}/citizens', load_import)

    web.run_app(app)

if __name__ == "__main__":
    main()
