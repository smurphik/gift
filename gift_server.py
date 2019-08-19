#! /usr/bin/env python3

"""
One-thread asynchronous server for storage and analysis data on citizens
"""

from aiohttp import web
import aiomysql, asyncio, sys, pymysql
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
def init_global_id():
    """Table global counter initialization"""

    try:
        conn = pymysql.connect(host='localhost', port=3306, user='gift_server',
                               password='qwertyqwer', db='gift_db',
                               autocommit=True)
    except:
        print('FAIL: Make sure that you have configured the database '
              '"gift_db" according to the README')
        sys.exit()

    with conn.cursor() as cursor:
        try:
            # read last unique id
            cursor.execute('SELECT MAX(id) FROM unique_ids_table;')
            return int(cursor.fetchone()[0])
        except:
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
        obj = await request.json()

        # Check data
        # TODO: status 400

        pool = await aiomysql.create_pool(
            host='localhost', port=3306, user='gift_server',
            password='qwertyqwer', db='gift_db', loop=loop, charset='utf8')

        async with pool.acquire() as conn:

            unique_id = await update_global_id(conn)
            import_id = 'import_' + str(unique_id)
            rel_id = 'rel_' + str(unique_id)

            async with conn.cursor() as cur:

                # Create table for citizen list
                sql = (
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
                await cur.execute(sql)

                # Fill table
                for citizen in obj['citizens']:
                    vals = []
                    for field in citizen_fields:
                        v = citizen[field]
                        if isinstance(v, int):
                            vals.append(str(v))
                        else:
                            vals.append(f"'{v}'")
                    sql = 'INSERT INTO {} VALUES ({});'.format(import_id,
                                                               ', '.join(vals))
                    await cur.execute(sql)

                # Create table for citizens family relationship
                sql = (
                    f'CREATE TABLE {rel_id} ('
                        'citizen_a int,'
                        'citizen_b int'
                    ');'
                )
                await cur.execute(sql)

                # Fill table
                for cit_item in obj['citizens']:
                    citizen_a = cit_item['citizen_id']
                    for citizen_b in cit_item['relatives']:
                        sql = 'INSERT INTO {} VALUES ({}, {});'.format(
                            rel_id, citizen_a, citizen_b
                        )
                        await cur.execute(sql)

                # It seems settings of autocommit depends on aiohttp version.
                # So, let it be
                await conn.commit()

        pool.close()

        response_obj = {'data': {'import_id': unique_id}}
        return web.Response(text=json.dumps(response_obj), status=201)

    except Exception as e:
        response_obj = {'error': str(e)}
        return web.Response(text=json.dumps(response_obj), status=500)


def main():

    global loop, glob_id

    loop = asyncio.get_event_loop()
    glob_id = init_global_id()

    app = web.Application()
    app.router.add_post('/imports', store_import)

    web.run_app(app)

if __name__ == "__main__":
    main()
