#! /usr/bin/env python3

"""
One-thread asynchronous server for storage and analysis data on citizens
"""

from aiohttp import web
import aiomysql, asyncio, sys, pymysql, traceback, json, datetime, numpy

class IncorrectData(Exception): pass

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

class CheckRelsStruct():
    def __init__(self):
        self.citizens = set()
        self.relatives = set()

def check_citizen_data(citizen_obj, rel_check_str, is_post=False):

    # POST- & PATCH-specific checks
    if is_post:
        if len(citizen_obj) != 9:
            raise IncorrectData(f'Too few fields for citizen {citizen_obj}')
        # N.B.: It's a pity, but the standard expression `assert` here and in
        # many other places is risky, because the code can be run in optimized
        # mode. To implement own function `Assert` is also not desirable,
        # because that will create a lot of unnecessary strings. So, code
        # consist many bulky if-raise combinations
    else:
        if 'citizen_id' in citizen_obj:
            raise IncorrectData(
                f"Field 'citizen_id' is not changeable by PATCH-request")

    # check every field
    for field, value in citizen_obj.items():

        # check 'citizen_id', 'apartment'
        if field in {'citizen_id', 'apartment'}:
            if not (isinstance(value, int) and value >= 0):
                raise IncorrectData(
                    f"Field '{field}' must be non-negative " +
                    "integer, not {} '{}'".format(type(value), value))
            if field == 'citizen_id':
                if value in rel_check_str.citizens:
                    raise IncorrectData(
                        f"Duplicated citizen with id '{citizen_id}'")
                rel_check_str.citizens.add(value)
            continue

        # pass 'relatives'
        elif field == 'relatives':
            if not isinstance(value, list):
                raise IncorrectData(f"Field '{field}' should be " +
                                    f"list of integers, not '{value}'")
            for rel in value:
                if not isinstance(rel, int):
                    raise IncorrectData(f"Field '{field}' should be " +
                                        f"list of integers, not '{value}'")
                if is_post:
                    # for POST-request save tuples:
                    # {(citizen_id, relation0), (citizen_id, relation1), ...}
                    rel_pair = (citizen_obj['citizen_id'], rel)
                    if rel_pair in rel_check_str.relatives:
                        raise IncorrectData(f"Duplicated relation: {rel_pair}")
                    rel_check_str.relatives.add(rel_pair)
                else:
                    # for PATCH-request save just id of relations:
                    # {relation0, relation1, ...}
                    if rel in rel_check_str.relatives:
                        raise IncorrectData(f"Duplicated relation: {rel}")
                    rel_check_str.relatives.add(rel)
            continue

        # other fields should be strings, ecxept 'relatives'
        if not isinstance(value, str):
            if value in citizen_fields: # don't overlap `incorrect field` case
                raise IncorrectData(f"Field '{field}' must be string, not " +
                                    "{} '{}'".format(type(value), value))

        # check 'town', 'street', 'building'
        if field in {'town', 'street', 'building'}:
            for ch in value:
                if ch.isalpha() or ch.isdigit():
                    break
            else:
                raise IncorrectData(f"Incorrect field '{field}': '{value}'")

        # check 'name'
        elif field == 'name':
            if not value.strip():
                raise IncorrectData(f"Incorrect field '{field}': '{value}'")

        # check 'birth_date'
        elif field == 'birth_date':
            try:
                d = datetime.date(*(int(i) for i in value.split('.')[::-1]))
            except:
                raise IncorrectData(f"Incorrect field '{field}': '{value}'")
            if d > datetime.datetime.utcnow().date():
                raise IncorrectData(f"Incorrect field '{field}': '{value}'")

        # check 'gender'
        elif field == 'gender':
            if value != 'male' and value != 'female':
                raise IncorrectData(f"Unknown gender: '{value}'")

        # incorrect field
        else:
            raise IncorrectData(f"Incorrect field name: '{field}'")

def invert_data(citizen_obj):
    """Convert field \"date\" 'DD.MM.YYYY' -> 'YYYY.MM.DD' or back"""
    citizen_obj['birth_date'] = \
        '.'.join(map(str.strip, citizen_obj['birth_date'].split('.')[::-1]))

async def store_import(request):
    """Handle /imports POST-request"""

    try:

        try:
            post_obj = await request.json()
        except Exception as e:
            response_obj = {'error': 'Incorrect JSON-object'}
            return web.json_response(response_obj, status=400)

        # check data correctness & invert date (dd.mm.yyyy -> yyyy.mm.dd)
        try:

            # check all fields except 'relations' & invert date
            rel_check_str = CheckRelsStruct()
            for citizen_obj in post_obj['citizens']:
                check_citizen_data(citizen_obj, rel_check_str, True)
                invert_data(citizen_obj)

            # check relations
            cits = rel_check_str.citizens
            rels = rel_check_str.relatives
            while rels:
                rel_pair = rels.pop()
                if rel_pair[0] not in cits:
                    raise IncorrectData(f"Incorrect relation {rel_pair}")
                if rel_pair[1] not in cits:
                    raise IncorrectData(f"Incorrect relation {rel_pair}")
                inv_pair = rel_pair[::-1]
                if inv_pair not in rels:
                    raise IncorrectData(f"Incorrect relation {rel_pair}")
                rels.remove(inv_pair)

        except IncorrectData as e:
            return web.json_response({'error': str(e)}, status=400)

        pool = request.config_dict['pool']

        async with pool.acquire() as conn:

            numeric_id = await update_global_id(conn)
            import_id = 'import_' + str(numeric_id)
            rel_id = 'rel_' + str(numeric_id)

            async with conn.cursor() as cur:

                # create table for citizen list
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

                # fill table
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

                # create table for citizens family relationship
                await cur.execute(f'CREATE TABLE {rel_id} (x int, y int);')

                # fill table
                sql = ''
                for citizen_obj in post_obj['citizens']:
                    x = citizen_obj['citizen_id']
                    for y in citizen_obj['relatives']:
                        await cur.execute(
                            f'INSERT INTO {rel_id} VALUES ({x}, {y});')

            await conn.commit()

        response_obj = {'data': {'import_id': numeric_id}}
        return web.json_response(response_obj, status=201)

    except Exception as e:

        traceback.print_exc()

        # clean incomplete tables in case of error
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute(f"DROP TABLE {import_id};")
                except:
                    pass
                try:
                    await cur.execute(f"DROP TABLE {rel_id};")
                except:
                    pass
            await conn.commit()

        return web.json_response({'error': str(e)}, status=500)

async def alter_import(request):
    """Handle /imports/{numeric_id}/citizens/citizen_id PATCH-request"""

    try:

        try:
            patch_obj = await request.json()
        except Exception as e:
            response_obj = {'error': 'Incorrect JSON-object'}
            return web.json_response(response_obj, status=400)

        # check data correctness
        try:

            # check all fields except 'relations'
            rel_check_str = CheckRelsStruct()
            check_citizen_data(patch_obj, rel_check_str)
            rels = rel_check_str.relatives

            # invert date
            if 'birth_date' in patch_obj:
                invert_data(patch_obj)

        except IncorrectData as e:
            return web.json_response({'error': str(e)}, status=400)

        pool = request.config_dict['pool']

        async with pool.acquire() as conn:

            numeric_id = int(request.match_info['numeric_id'])
            import_id = 'import_' + str(numeric_id)
            rel_id = 'rel_' + str(numeric_id)
            citizen_id = int(request.match_info['citizen_id'])

            async with conn.cursor() as cur:

                # check relations
                await cur.execute(f'SELECT citizen_id FROM {import_id};')
                async for r in cur:
                    r = r[0]
                    if r in rels:
                        rels.remove(r)
                if rels:
                    response_obj = {'error': 'Wrong relations: {}'.format(
                        [(citizen_id, r) for r in rels])}
                    return web.json_response(response_obj, status=400)

                # check citizen existence
                await cur.execute(f'SELECT citizen_id FROM {import_id} ' +
                                  f'WHERE citizen_id = {citizen_id};')
                if not await cur.fetchone():
                    response_obj = {'error': f'Wrong citizen_id: {citizen_id}'}
                    return web.json_response(response_obj, status=400)

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

        response_obj = {'data': citizen_obj}
        return web.json_response(response_obj, status=200)

    except Exception as e:
        traceback.print_exc()
        return web.json_response({'error': str(e)}, status=500)

async def load_import(request):
    """Handle /imports/{numeric_id}/citizens GET-request"""

    try:

        pool = request.config_dict['pool']

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

        return web.json_response(response_obj, status=200)

    except Exception as e:
        traceback.print_exc()
        return web.json_response({'error': str(e)}, status=500)

async def load_donators_by_months(request):
    """Handle /imports/{numeric_id}/citizens/birthdays GET-request"""

    try:

        pool = request.config_dict['pool']

        async with pool.acquire() as conn:

            numeric_id = int(request.match_info['numeric_id'])
            import_id = 'import_' + str(numeric_id)
            rel_id = 'rel_' + str(numeric_id)

            async with conn.cursor() as cur:

                # read data from import_id
                try:
                    await cur.execute(
                        f'SELECT citizen_id, birth_date FROM {import_id};')
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

        return web.json_response(response_obj, status=200)

    except Exception as e:
        traceback.print_exc()
        return web.json_response({'error': str(e)}, status=500)

async def load_agestat_by_towns(request):
    """Handle /imports/{numeric_id}/towns/stat/percentile/age GET-request"""

    try:

        pool = request.config_dict['pool']

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

        return web.json_response(response_obj, status=200)

    except Exception as e:
        traceback.print_exc()
        return web.json_response({'error': str(e)}, status=500)

async def init(app):
    app['pool'] = await aiomysql.create_pool(
        host='localhost', port=3306, user='gift_server',
        password='Qwerty!0', db='gift_db', loop=loop, charset='utf8')
    yield
    app['pool'].close()
    await app['pool'].wait_closed()

def main():

    global loop, glob_id

    loop = asyncio.get_event_loop()
    glob_id = init_global_id()

    app = web.Application()
    app.cleanup_ctx.append(init)
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
