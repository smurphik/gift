#! /usr/bin/env python3

"""
One-thread asynchronous server for storage and analysis data on citizens
"""

from aiohttp import web
import traceback
import datetime
import asyncio
import numpy
from gino import Gino
from sqlalchemy import and_, or_

CITIZEN_FIELDS = ('citizen_id', 'town', 'street', 'building',
                  'apartment', 'name', 'birth_date', 'gender')
db = Gino()


class IncorrectData(Exception):
    pass


class Imports(db.Model):
    """Table with common data about citizens"""

    __tablename__ = 'imports'

    import_id = db.Column(db.Integer())
    citizen_id = db.Column(db.Integer())
    town = db.Column(db.Unicode())
    street = db.Column(db.Unicode())
    building = db.Column(db.Unicode())
    apartment = db.Column(db.Integer())
    name = db.Column(db.Unicode())
    birth_date = db.Column(db.Unicode())
    gender = db.Column(db.Unicode())

    _idx1 = db.Index('imps_import_id_idx', 'import_id')


class Relations(db.Model):
    """Table with family relationships data"""

    __tablename__ = 'relations'

    import_id = db.Column(db.Integer())
    x = db.Column(db.Integer())
    y = db.Column(db.Integer())

    _idx1 = db.Index('rels_import_id_idx', 'import_id')


class UniqueIds(db.Model):
    """Table with unique identifiers of import tables"""

    __tablename__ = 'unique_ids'

    id = db.Column(db.Integer(), primary_key=True)


class PostedIds(db.Model):
    """Table with identifiers of posted imports"""

    __tablename__ = 'posted_ids'

    id = db.Column(db.Integer(), primary_key=True)


async def create_unique_id():
    """Getting unique id for new table"""

    row = await UniqueIds.create()
    return row.id


class CheckRelsStruct():
    """Auxiliary structure for user data check"""
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

        # create unique id
        import_id = await create_unique_id()

        # fill imports table by rows with import_id
        async with db.transaction():
            for citizen_obj in post_obj['citizens']:
                await Imports.create(
                    import_id=import_id,
                    **{f: citizen_obj[f] for f in CITIZEN_FIELDS})

        # fill relations table by rows with import_id
        async with db.transaction():
            for citizen_obj in post_obj['citizens']:
                x = citizen_obj['citizen_id']
                for y in citizen_obj['relatives']:
                    await Relations.create(import_id=import_id, x=x, y=y)

        # mark import like 'posted'
        await PostedIds.create(id=import_id)

        response_obj = {'data': {'import_id': import_id}}
        return web.json_response(response_obj, status=201)

    except Exception as e:

        traceback.print_exc()

        try:
            await Imports.delete.where(Imports.import_id == import_id)
            await Relations.delete.where(Imports.import_id == import_id)
        except Exception:
            pass

        return web.json_response({'error': str(e)}, status=500)


async def alter_import(request):
    """Handle /imports/{import_id}/citizens/{citizen_id} PATCH-request"""

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
            relations = rel_check_str.relatives

            # invert date
            if 'birth_date' in patch_obj:
                invert_date(patch_obj)

        except IncorrectData as e:
            return web.json_response({'error': str(e)}, status=400)

        # inits
        import_id = int(request.match_info['import_id'])
        citizen_id = int(request.match_info['citizen_id'])

        # check data existance
        row = await PostedIds.query.where(
            PostedIds.id == import_id).gino.scalar()
        if not row:
            response_obj = {'error': 'Import not found'}
            return web.json_response(response_obj, status=404)

        # check relations (part 2 - check existance of relatives in import)
        rows = await Imports.select('citizen_id').where(
            Imports.import_id == import_id).gino.all()
        for row in rows:
            cit_id = row[0]
            if cit_id in relations:
                relations.remove(cit_id)
        if relations:
            response_obj = {'error': 'Wrong relations: {}'.format(
                [(citizen_id, r) for r in relations])}
            return web.json_response(response_obj, status=400)

        # check citizen existence
        citizen = await Imports.query.where(
            and_(Imports.import_id == import_id,
                 Imports.citizen_id == citizen_id)).gino.scalar()
        if not citizen:
            response_obj = {'error': f'Wrong citizen_id: {citizen_id}'}
            return web.json_response(response_obj, status=404)

        # prepare data for alter imports table
        patch_norel_obj = dict(patch_obj)
        if 'relatives' in patch_norel_obj:
            del patch_norel_obj['relatives']

        # change data transaction
        async with db.transaction():

            # alter fields in imports table with import_id
            if patch_norel_obj:
                await Imports.update.values(**patch_norel_obj).where(
                    and_(Imports.import_id == import_id,
                         Imports.citizen_id == citizen_id)).gino.status()

            # alter relations in table with import_id
            # (ceate a single string for single transaction - this ensure
            #  data correctness, if client send many requests in parallel)
            if 'relatives' in patch_obj:
                i = citizen_id
                await Relations.delete.where(
                    and_(Relations.import_id == import_id,
                         or_(Relations.x == i,
                             Relations.y == i))).gino.status()
                for j in patch_obj['relatives']:
                    await Relations.create(import_id=import_id, x=i, y=j)
                    if i != j:
                        await Relations.create(import_id=import_id, x=j, y=i)

        # control reading citizen data for response to client
        rows = await Imports.select(*CITIZEN_FIELDS).where(
            and_(Imports.import_id == import_id,
                 Imports.citizen_id == citizen_id)).gino.all()
        citizen_obj = dict(zip(CITIZEN_FIELDS, rows[0]))
        invert_date(citizen_obj)
        rows = await Relations.select('y').where(
            and_(Relations.import_id == import_id,
                 Relations.x == citizen_id)).gino.all()
        rels = [row[0] for row in rows]
        citizen_obj['relatives'] = list(rels) if rels else list()

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

        # check data existance
        row = await PostedIds.query.where(
            PostedIds.id == import_id).gino.scalar()
        if not row:
            response_obj = {'error': 'Import not found'}
            return web.json_response(response_obj, status=404)

        # read data from imports table with import_id
        rows = await Imports.select(*CITIZEN_FIELDS).where(
            Imports.import_id == import_id).gino.all()
        citizens_obj_list = []
        for row in rows:
            citizen_obj = dict(zip(CITIZEN_FIELDS, row))
            invert_date(citizen_obj)
            citizens_obj_list.append(citizen_obj)
        response_obj = {'data': citizens_obj_list}

        # read data from relations table with import_id
        rows = await Relations.select('x', 'y').where(
            Relations.import_id == import_id).gino.all()
        rels = dict()
        for row in rows:
            i, j = row[0], row[1]
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
    """Handle /imports/{import_id}/citizens/birthdays GET-request"""

    try:

        # inits
        import_id = int(request.match_info['import_id'])

        # check data existance
        row = await PostedIds.query.where(
            PostedIds.id == import_id).gino.scalar()
        if not row:
            response_obj = {'error': 'Import not found'}
            return web.json_response(response_obj, status=404)

        # read data from imports table with import_id
        rows = await Imports.select('citizen_id', 'birth_date').where(
            Imports.import_id == import_id).gino.all()
        id_to_info = dict()
        for row in rows:
            id_to_info[row[0]] = {'bdate': row[1], 'rels': []}

        # read data from relations table with import_id
        rows = await Relations.select('x', 'y').where(
            Relations.import_id == import_id).gino.all()
        for row in rows:
            id_to_info[row[0]]['rels'].append(row[1])

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
    """Handle /imports/{import_id}/towns/stat/percentile/age GET-request"""

    try:

        # inits
        import_id = int(request.match_info['import_id'])

        # check data existance
        row = await PostedIds.query.where(
            PostedIds.id == import_id).gino.scalar()
        if not row:
            response_obj = {'error': 'Import not found'}
            return web.json_response(response_obj, status=404)

        # read data from imports table with import_id
        rows = await Imports.select('town', 'birth_date').where(
            Imports.import_id == import_id).order_by(
                Imports.birth_date.desc()).gino.all()
        cur_date = datetime.datetime.utcnow().date()
        town_ages_dict = {}
        for row in rows:
            town = row[0]
            bdate = datetime.date(*(int(i) for i in row[1].split('.')))
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
        return web.json_response(response_obj, status=200)

    except Exception as e:
        traceback.print_exc()
        return web.json_response({'error': str(e)}, status=500)


async def init(app):

    await db.set_bind(
        f'postgresql://gift_server:{db_password}@localhost/gift_db')

    # create tables fir classes: Imports, Relations, UniqueIds, PostedIds
    await db.gino.create_all()

    yield


def main():

    global loop, db_password

    # init globals
    loop = asyncio.get_event_loop()
    db_password = 'Qwerty?0'

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
