#! /usr/bin/env python3

"""
One-thread asynchronous server for storage and analysis data on citizens
"""

from aiohttp import web
import aiomysql, asyncio, sys, pymysql, traceback, json, datetime, numpy

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
            max_ = int(cursor.fetchone()[0])
            cursor.execute(f'DELETE FROM unique_ids_table WHERE id < {max_};')
            return max_
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

# datetime.date(*(int(i) for i in '2016.02.29'.split('.')))
def check_citizen_data(citizen_obj):
    pass #TODO

def invert_data(citizen_obj):
    """Convert field \"date\" 'DD.MM.YYYY' -> 'YYYY.MM.DD' or back"""
    citizen_obj['birth_date'] = \
        '.'.join(map(str.strip, citizen_obj['birth_date'].split('.')[::-1]))

async def store_import(request):
    """Handle /imports POST-request"""

    try:
        post_obj = await request.json()

        # Check data & invert field "date" (dd.mm.yyyy -> yyyy.mm.dd)
        # TODO: status 400, rels
        for citizen_obj in post_obj['citizens']:
            invert_data(citizen_obj)
            check_citizen_data(citizen_obj)

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
                for citizen_obj in post_obj['citizens']:
                    vals = []
                    for field in citizen_fields:
                        v = citizen_obj[field]
                        if isinstance(v, int):
                            vals.append(str(v))
                        else:
                            vals.append(f"'{v}'")
                    await cur.execute('INSERT INTO {} VALUES ({});'.format(
                        import_id, ', '.join(vals)))

                # Create table for citizens family relationship
                await cur.execute(f'CREATE TABLE {rel_id} (x int, y int);')

                # Fill table
                sql = ''
                for citizen_obj in post_obj['citizens']:
                    x = citizen_obj['citizen_id']
                    for y in citizen_obj['relatives']:
                        await cur.execute(
                            f'INSERT INTO {rel_id} VALUES ({x}, {y});')

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
        # TODO: status 400, rels
        if 'birth_date' in patch_obj:
            invert_data(patch_obj)
        check_citizen_data(patch_obj)

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
                await cur.execute(
                    f'DELETE FROM {rel_id} WHERE x = {i} OR y = {i};')
                for j in patch_obj['relatives']:
                    await cur.execute(
                        f'INSERT INTO {rel_id} VALUES ({i}, {j});' +
                        f'INSERT INTO {rel_id} VALUES ({j}, {i});')

                # control reading for response to client
                try:
                    await cur.execute(f'SELECT * FROM {import_id} ' +
                                      f'WHERE citizen_id = {citizen_id};')
                    response = await cur.fetchone()
                    citizen_obj = dict(zip(citizen_fields, response))
                    invert_data(citizen_obj)
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
                citizens_obj_list = []
                async for r in cur:
                    citizen_obj = dict(zip(citizen_fields, r))
                    invert_data(citizen_obj)
                    citizens_obj_list.append(citizen_obj)
                response_obj = {'data': citizens_obj_list}

                # read data from rel_id
                try:
                    await cur.execute(f'SELECT * FROM {rel_id};')
                except Exception as e:
                    return web.json_response({'error': str(e)}, status=404)
                rels = dict()
                async for r in cur:
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

async def load_donators_by_months(request):
    """Handle /imports/{numeric_id}/citizens/birthdays GET-request"""

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
                    await cur.execute(
                        f'SELECT citizen_id, birth_date FROM {import_id};'
                    )
                except Exception as e:
                    return web.json_response({'error': str(e)}, status=404)
                id_to_info = dict()
                async for r in cur:
                    id_to_info[r[0]] = {'bdate': r[1], 'rels': []}

                # read data from rel_id
                try:
                    await cur.execute(f'SELECT * FROM {rel_id};')
                except Exception as e:
                    return web.json_response({'error': str(e)}, status=404)
                async for r in cur:
                    id_to_info[r[0]]['rels'].append(r[1])

                # calc distribution by months
                months_dist = [dict() for _ in range(13)]
                for i, obj in id_to_info.items():
                    month = int(obj['bdate'].split('.')[1])
                    for donator in obj['rels']:
                        present_cnt = months_dist[month]
                        if donator not in present_cnt:
                            present_cnt[donator] = 0
                        present_cnt[donator] += 1

                # convert distribution to json-response
                response_obj = {str(i): [] for i in range(1, 13)}
                for month, present_cnt in enumerate(months_dist[1:], 1):
                    for donator, cnt in present_cnt.items():
                        response_obj[str(month)].append({'citizen_id': donator,
                                                         'presents': cnt})
                response_obj = {'data': response_obj}

        pool.close()

        return web.json_response(response_obj, status=200)

    except Exception as e:
        traceback.print_exc()
        return web.json_response({'error': str(e)}, status=500)

async def load_agestat_by_towns(request):
    """Handle /imports/{numeric_id}/towns/stat/percentile/age GET-request"""

    try:

        pool = await aiomysql.create_pool(
            host='localhost', port=3306, user='gift_server',
            password='Qwerty!0', db='gift_db', loop=loop, charset='utf8')

        async with pool.acquire() as conn:

            numeric_id = int(request.match_info['numeric_id'])
            import_id = 'import_' + str(numeric_id)

            async with conn.cursor() as cur:

                # read towns tuple
                try:
                    await cur.execute(f'SELECT DISTINCT town FROM {import_id};')
                except Exception as e:
                    return web.json_response({'error': str(e)}, status=404)
                towns = await cur.fetchall() # TODO: rid of fetchall

                #datetime.utcnow().date()
                #numpy.percentile interpolation='linear'
                response_obj = {'data': None}

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
    app.router.add_patch(
        '/imports/{numeric_id:[0-9]+}/citizens/{citizen_id:[0-9]+}',
        alter_import
    )
    app.router.add_get('/imports/{numeric_id:[0-9]+}/citizens', load_import)
    app.router.add_get(
        '/imports/{numeric_id:[0-9]+}/citizens/birthdays',
        load_donators_by_months
    )
    app.router.add_get(
        '/imports/{numeric_id:[0-9]+}/towns/stat/percentile/age',
        load_agestat_by_towns
    )

    web.run_app(app)

if __name__ == "__main__":
    main()
