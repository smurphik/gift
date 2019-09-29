#! /usr/bin/env python3

"""
One-thread asynchronous server for storage and analysis data on citizens
"""

from aiohttp import web
import traceback
import datetime
import aiomysql
import asyncio
import numpy


class IncorrectData(Exception):
    pass


CITIZEN_FIELDS = ('citizen_id', 'town', 'street', 'building',
                  'apartment', 'name', 'birth_date', 'gender')


async def create_unique_id(cursor):
    """Getting unique id for new table"""

    await cursor.execute('INSERT INTO unique_ids VALUES();')
    await cursor.execute('SELECT LAST_INSERT_ID();')
    r = await cursor.fetchone()
    return r[0]


class CheckRelsStruct():
    def __init__(self):
        self.citizens = set()
        self.relatives = set()


def check_citizen_data(citizen_obj, rel_check_str, is_post=False):
    """Check of data related to one citizen.
    `rel_check_str` - instance of `CheckRelsStruct` class - for
    check relations of citizen with same `import_id`"""

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

        # incorrect field
        if field not in CITIZEN_FIELDS and field != 'relatives':
            raise IncorrectData(f"Incorrect field name: '{field}'")

        # check 'citizen_id', 'apartment'
        if field in {'citizen_id', 'apartment'}:
            if not (isinstance(value, int) and value >= 0):
                raise IncorrectData(
                    f"Field '{field}' must be non-negative " +
                    "integer, not {} '{}'".format(type(value), value))
            if field == 'citizen_id':
                if value in rel_check_str.citizens:
                    raise IncorrectData(
                        f"Duplicated citizen with id '{value}'")
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
                    cit = citizen_obj['citizen_id']
                    if cit != rel:
                        # ordinary case (not relative to himself case)
                        rel_pair = (cit, rel)
                        if rel_pair in rel_check_str.relatives:
                            raise IncorrectData(
                                f"Duplicated relation: {rel_pair}")
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
            raise IncorrectData(f"Field '{field}' must be string, not " +
                                "{} '{}'".format(type(value), value))

        # check 'town', 'street', 'building'
        if field in {'town', 'street', 'building'}:
            if not field:
                raise IncorrectData(f"Incorrect field '{field}': '{value}'")
            for ch in value:
                if ch.isalpha() or ch.isdigit():
                    break
            else:
                raise IncorrectData(f"Incorrect field '{field}': '{value}'")

        # check 'name'
        elif field == 'name':
            if not value or not value.strip():
                raise IncorrectData(f"Incorrect field '{field}': '{value}'")

        # check 'birth_date'
        elif field == 'birth_date':
            try:
                d = datetime.date(*(int(i) for i in value.split('.')[::-1]))
            except Exception:
                raise IncorrectData(f"Incorrect field '{field}': '{value}' " +
                                    "must be a format 'DD.MM.YYYY' string")
            if any((d > datetime.datetime.utcnow().date(),
                    len(value.split('.')[0]) != 2,
                    len(value.split('.')[1]) != 2,
                    len(value.split('.')[2]) != 4)):
                raise IncorrectData(f"Incorrect field '{field}': '{value}' " +
                                    "must be a format 'DD.MM.YYYY' string")

        # check 'gender'
        elif field == 'gender':
            if value != 'male' and value != 'female':
                raise IncorrectData(f"Unknown gender: '{value}'")

        # incorrect field
        else:
            raise IncorrectData(f"#Incorrect field name: '{field}'")


def invert_date(citizen_obj):
    """Convert field \"date\" 'DD.MM.YYYY' -> 'YYYY.MM.DD' or back"""
    fields = citizen_obj['birth_date'].split('.')[::-1]
    fields = list(map(str.strip, fields))
    # fields = list(map(lambda s: s.zfill(2) if len(s) == 1 else s, fields))
    citizen_obj['birth_date'] = '.'.join(fields)


def sub_years(x, y):
    """date x, date y -> years delta between x and y"""
    delta = x.year - y.year
    if x.month < y.month:
        delta -= 1
    elif x.month == y.month:
        if x.day < y.day:
            delta -= 1
    return delta


async def store_import(request):
    """Handle /imports POST-request"""

    try:

        try:
            post_obj = await request.json()
        except Exception as e:
            response_obj = {'error': f'Incorrect JSON-object: {e}'}
            return web.json_response(response_obj, status=400)

        # check data correctness & invert date (dd.mm.yyyy -> yyyy.mm.dd)
        try:

            # check all fields except 'relations' & invert date
            rel_check_str = CheckRelsStruct()
            for citizen_obj in post_obj['citizens']:
                check_citizen_data(citizen_obj, rel_check_str, True)
                invert_date(citizen_obj)

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

        pool = await aiomysql.create_pool(
            host='localhost', port=3306, user='gift_server',
            password=db_password, db='gift_db', loop=loop, charset='utf8')

        async with pool.acquire() as conn:
            async with conn.cursor() as cur:

                # create unique id
                import_id = await create_unique_id(cur)

                # fill imports table by rows with import_id
                text = ('INSERT INTO imports VALUES '
                        '(%s, %s, %s, %s, %s, %s, %s, %s, %s);')
                for citizen_obj in post_obj['citizens']:
                    vals = [import_id]
                    vals += [citizen_obj[f] for f in CITIZEN_FIELDS]
                    await cur.execute(text, vals)

                # fill relations table by rows with import_id
                text = 'INSERT INTO relations VALUES (%s, %s, %s);'
                for citizen_obj in post_obj['citizens']:
                    x = citizen_obj['citizen_id']
                    for y in citizen_obj['relatives']:
                        await cur.execute(text, (import_id, x, y))

                # mark import like 'posted'
                await cur.execute('INSERT INTO posted_ids VALUES (%s);',
                                  import_id)

            await conn.commit()

        pool.close()
        await pool.wait_closed()

        response_obj = {'data': {'import_id': import_id}}
        return web.json_response(response_obj, status=201)

    except Exception as e:

        traceback.print_exc()

        pool = await aiomysql.create_pool(
            host='localhost', port=3306, user='gift_server',
            password=db_password, db='gift_db', loop=loop, charset='utf8')

        # clean incomplete data in case of error
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute('DELETE FROM imports '
                                      'WHERE import_id = %s;', import_id)
                except Exception:
                    pass
                try:
                    await cur.execute('DELETE FROM relations '
                                      'WHERE import_id = %s;', import_id)
                except Exception:
                    pass
            await conn.commit()

        pool.close()
        await pool.wait_closed()

        return web.json_response({'error': str(e)}, status=500)


async def alter_import(request):
    """Handle /imports/{import_id}/citizens/citizen_id PATCH-request"""

    try:

        try:
            patch_obj = await request.json()
        except Exception as e:
            response_obj = {'error': f'Incorrect JSON-object: {e}'}
            return web.json_response(response_obj, status=400)

        # check data correctness
        try:

            # check all fields except 'relations'
            rel_check_str = CheckRelsStruct()
            check_citizen_data(patch_obj, rel_check_str)
            rels = rel_check_str.relatives

            # invert date
            if 'birth_date' in patch_obj:
                invert_date(patch_obj)

        except IncorrectData as e:
            return web.json_response({'error': str(e)}, status=400)

        # inits
        import_id = int(request.match_info['import_id'])
        citizen_id = int(request.match_info['citizen_id'])
        pool = await aiomysql.create_pool(
            host='localhost', port=3306, user='gift_server',
            password=db_password, db='gift_db', loop=loop, charset='utf8')

        async with pool.acquire() as conn:
            async with conn.cursor() as cur:

                # check data existance
                await cur.execute(
                    'SELECT id FROM posted_ids WHERE id = %s;', import_id)
                if not (await cur.fetchall()):
                    response_obj = {'error': 'Import not found'}
                    return web.json_response(response_obj, status=404)

                # check relations
                await cur.execute('SELECT citizen_id FROM imports '
                                  'WHERE import_id = %s;', import_id)
                if citizen_id in rels:
                    # for the relative himself case
                    rels.remove(citizen_id)
                async for r in cur:
                    r = r[0]
                    if r in rels:
                        rels.remove(r)
                if rels:
                    response_obj = {'error': 'Wrong relations: {}'.format(
                        [(citizen_id, r) for r in rels])}
                    return web.json_response(response_obj, status=400)

                # check citizen existence
                await cur.execute('SELECT citizen_id FROM imports '
                                  'WHERE import_id = %s AND citizen_id = %s;',
                                  (import_id, citizen_id))
                if not await cur.fetchone():
                    response_obj = {'error': f'Wrong citizen_id: {citizen_id}'}
                    return web.json_response(response_obj, status=404)

                # alter fields in imports table with import_id
                pairs = []
                vals = []
                for field, value in patch_obj.items():
                    if field == 'relatives':
                        continue
                    pairs.append(f'{field} = %s')
                    vals.append(value)
                if vals:
                    text = ('UPDATE imports SET {} '.format(', '.join(pairs)) +
                            'WHERE import_id = %s AND citizen_id = %s;')
                    await cur.execute(text, vals + [import_id, citizen_id])

                # alter relations in table with import_id
                # (ceate a single string for single transaction - this ensure
                #  data correctness, if client send many requests in parallel)
                if 'relatives' in patch_obj:
                    i = citizen_id
                    text = ('DELETE FROM relations '
                            'WHERE import_id = %s '
                            'AND (x = %s OR y = %s);')
                    await cur.execute(text, (import_id, i, i))
                    text = ('INSERT INTO relations VALUES (%s, %s, %s);'
                            'INSERT INTO relations VALUES (%s, %s, %s);')
                    for j in patch_obj['relatives']:
                        await cur.execute(text,
                                          (import_id, i, j, import_id, j, i))

                # control reading citizen data for response to client
                fields = ', '.join(CITIZEN_FIELDS)
                text = (f'SELECT {fields} FROM imports '
                        'WHERE import_id = %s AND citizen_id = %s;')
                await cur.execute(text, (import_id, citizen_id))
                response = await cur.fetchone()
                citizen_obj = dict(zip(CITIZEN_FIELDS, response))
                invert_date(citizen_obj)
                text = ('SELECT y FROM relations '
                        'WHERE import_id = %s AND x = %s;')
                await cur.execute(text, (import_id, citizen_id))
                rels = await cur.fetchone()
                citizen_obj['relatives'] = list(rels) if rels else list()

            await conn.commit()

        pool.close()
        await pool.wait_closed()

        response_obj = {'data': citizen_obj}
        return web.json_response(response_obj, status=200)

    except Exception as e:
        traceback.print_exc()
        return web.json_response({'error': str(e)}, status=500)


async def load_import(request):
    """Handle /imports/{import_id}/citizens GET-request"""

    try:

        # inits
        import_id = int(request.match_info['import_id'])
        pool = await aiomysql.create_pool(
            host='localhost', port=3306, user='gift_server',
            password=db_password, db='gift_db', loop=loop, charset='utf8')

        async with pool.acquire() as conn:
            async with conn.cursor() as cur:

                # check data existance
                text = 'SELECT id FROM posted_ids WHERE id = %s;'
                await cur.execute(text, import_id)
                if not (await cur.fetchall()):
                    response_obj = {'error': 'Import not found'}
                    return web.json_response(response_obj, status=404)

                # read data from imports table with import_id
                fields = ', '.join(CITIZEN_FIELDS)
                text = f'SELECT {fields} FROM imports WHERE import_id = %s;'
                await cur.execute(text, import_id)
                citizens_obj_list = []
                async for r in cur:
                    citizen_obj = dict(zip(CITIZEN_FIELDS, r))
                    invert_date(citizen_obj)
                    citizens_obj_list.append(citizen_obj)
                response_obj = {'data': citizens_obj_list}

                # read data from relations table with import_id
                text = 'SELECT x, y FROM relations WHERE import_id = %s;'
                await cur.execute(text, import_id)
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
        await pool.wait_closed()

        return web.json_response(response_obj, status=200)

    except Exception as e:
        traceback.print_exc()
        return web.json_response({'error': str(e)}, status=500)


async def load_donators_by_months(request):
    """Handle /imports/{import_id}/citizens/birthdays GET-request"""

    try:

        # inits
        import_id = int(request.match_info['import_id'])
        pool = await aiomysql.create_pool(
            host='localhost', port=3306, user='gift_server',
            password=db_password, db='gift_db', loop=loop, charset='utf8')

        async with pool.acquire() as conn:
            async with conn.cursor() as cur:

                # check data existance
                text = 'SELECT id FROM posted_ids WHERE id = %s;'
                await cur.execute(text, import_id)
                if not (await cur.fetchall()):
                    response_obj = {'error': 'Import not found'}
                    return web.json_response(response_obj, status=404)

                # read data from imports table with import_id
                text = ('SELECT citizen_id, birth_date FROM imports '
                        'WHERE import_id = %s;')
                await cur.execute(text, import_id)
                id_to_info = dict()
                async for r in cur:
                    id_to_info[r[0]] = {'bdate': r[1], 'rels': []}

                # read data from relations table with import_id
                text = 'SELECT x, y FROM relations WHERE import_id = %s;'
                await cur.execute(text, import_id)
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
        await pool.wait_closed()

        return web.json_response(response_obj, status=200)

    except Exception as e:
        traceback.print_exc()
        return web.json_response({'error': str(e)}, status=500)


async def load_agestat_by_towns(request):
    """Handle /imports/{import_id}/towns/stat/percentile/age GET-request"""

    try:

        # inits
        import_id = int(request.match_info['import_id'])
        pool = await aiomysql.create_pool(
            host='localhost', port=3306, user='gift_server',
            password=db_password, db='gift_db', loop=loop, charset='utf8')

        async with pool.acquire() as conn:
            async with conn.cursor() as cur:

                # check data existance
                text = 'SELECT id FROM posted_ids WHERE id = %s;'
                await cur.execute(text, import_id)
                if not (await cur.fetchall()):
                    response_obj = {'error': 'Import not found'}
                    return web.json_response(response_obj, status=404)

                # read data from imports table with import_id
                text = ('SELECT town, birth_date FROM imports '
                        'WHERE import_id = %s ORDER BY birth_date DESC;')
                await cur.execute(text, import_id)
                cur_date = datetime.datetime.utcnow().date()
                town_ages_dict = {}
                async for r in cur:
                    town = r[0]
                    bdate = datetime.date(*(int(i) for i in r[1].split('.')))
                    if town not in town_ages_dict:
                        town_ages_dict[town] = []
                    town_ages_dict[town].append(sub_years(cur_date, bdate))

                # calc percentiles
                percentiles_obj = []
                for town, bdates in town_ages_dict.items():
                    town_obj = {"town": town}
                    town_obj["p50"] = round(numpy.percentile(bdates, 50), 2)
                    town_obj["p75"] = round(numpy.percentile(bdates, 75), 2)
                    town_obj["p99"] = round(numpy.percentile(bdates, 99), 2)
                    percentiles_obj.append(town_obj)

                response_obj = {'data': percentiles_obj}

        pool.close()
        await pool.wait_closed()

        return web.json_response(response_obj, status=200)

    except Exception as e:
        traceback.print_exc()
        return web.json_response({'error': str(e)}, status=500)


async def init(app):

    pool = await aiomysql.create_pool(
        host='localhost', port=3306, user='gift_server',
        password=db_password, db='gift_db', loop=loop, charset='utf8')

    # create tables
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:

            engine = 'InnoDB'  # 'MyISAM'

            # create table for citizen imports
            try:
                await cur.execute('CREATE TABLE imports ('
                                  '    import_id  int,'
                                  '    citizen_id int,'
                                  '    town       varchar(255),'
                                  '    street     varchar(255),'
                                  '    building   varchar(255),'
                                  '    apartment  int,'
                                  '    name       varchar(255),'
                                  '    birth_date varchar(255),'
                                  '    gender     varchar(255)) '
                                  f'ENGINE = {engine};')
                await cur.execute('CREATE INDEX id_index '
                                  'ON imports (import_id);')
            except Exception:
                pass

            # create table for citizens family relationship
            try:
                await cur.execute('CREATE TABLE relations '
                                  '(import_id int, x int, y int) '
                                  f'ENGINE = {engine};')
                await cur.execute('CREATE INDEX id_index '
                                  'ON relations (import_id);')
            except Exception:
                pass

            # create table for unique identifiers
            try:
                await cur.execute('CREATE TABLE unique_ids('
                                  'id int NOT NULL AUTO_INCREMENT, '
                                  'PRIMARY KEY (id)) '
                                  f'ENGINE = {engine};')
            except Exception:
                pass

            # create posted import ids table
            try:
                await cur.execute('CREATE TABLE posted_ids('
                                  'id int NOT NULL AUTO_INCREMENT, '
                                  'PRIMARY KEY (id)) '
                                  f'ENGINE = {engine};')
            except Exception:
                pass

        await conn.commit()

    pool.close()
    await pool.wait_closed()

    yield


def main():

    global loop, db_password

    # init globals
    loop = asyncio.get_event_loop()
    db_password = 'Qwerty!0'

    # make application
    app = web.Application(client_max_size=1024*1024*10)
    app.cleanup_ctx.append(init)
    app.router.add_post('/imports', store_import)
    app.router.add_patch(
        '/imports/{import_id:[0-9]+}/citizens/{citizen_id:[0-9]+}',
        alter_import
    )
    app.router.add_get('/imports/{import_id:[0-9]+}/citizens', load_import)
    app.router.add_get(
        '/imports/{import_id:[0-9]+}/citizens/birthdays',
        load_donators_by_months
    )
    app.router.add_get(
        '/imports/{import_id:[0-9]+}/towns/stat/percentile/age',
        load_agestat_by_towns
    )

    # run
    web.run_app(app)


if __name__ == "__main__":
    main()
