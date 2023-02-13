import logging, datetime, configparser, aiocron
import asyncio, aiocfscrape
import pymongo

from pymongo.errors import BulkWriteError
from aiogram import Bot, Dispatcher, executor, types
from aiogram.utils.callback_data import CallbackData
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery

# Config.ini set
config = configparser.ConfigParser()
config.read("config.ini")


# MongoDB set
client = pymongo.MongoClient(config["database"]["db_link"])
db_main = client.main_db.main_coll

logging.basicConfig(level=logging.INFO)
# Telegram set
channel_id = config["tg_bot"]["main_channel_id"]
moderation_id = config["tg_bot"]["moder_chat_id"]
bot = Bot(token=config["tg_bot"]["bot_api_key"])
dp = Dispatcher(bot)


def dayofweek():
    return ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"][
        datetime.datetime.today().weekday()]


def actual_api_links():
    return [v for k, v in config.items(dayofweek()) if k.startswith("api_link")]


# Buttons
cb_mongo = CallbackData('mongo', 'action', '_id')


def create_button(_id):
    return InlineKeyboardMarkup().add(
        InlineKeyboardButton("Bruh", callback_data=cb_mongo.new(action='run', _id=_id)),
        InlineKeyboardButton("Top", callback_data=cb_mongo.new(action='top', _id=_id)))


@dp.callback_query_handler(cb_mongo.filter(action='run'))
async def press_button(query: CallbackQuery, callback_data: dict):
    _id = int(callback_data["_id"])
    client.main_db.main_coll.update_one({'_id': _id}, {"$set": {'shit?': True}})
    client.main_db.main_coll.update_one({'_id': _id}, {"$set": {'posted?': True}})
    await query.message.edit_caption(caption=f"{_id} Delete from DB, " + str(query.from_user.first_name))


@dp.callback_query_handler(cb_mongo.filter(action='top'))
async def press_button(query: CallbackQuery, callback_data: dict):
    _id = int(callback_data["_id"])
    client.main_db.main_coll.update_one({'_id': _id}, {"$set": {'score': 99999}})
    client.main_db.main_coll.update_one({'_id': _id}, {"$set": {'shit?': False}})
    client.main_db.main_coll.update_one({'_id': _id}, {"$set": {'posted?': False}})
    await query.message.edit_caption(caption=f"{_id} Has been added to the top, " + str(query.from_user.first_name))


# webm to mp4
def extract_url(d: dict):
    sub = lambda x: ([s for s in x["urls"] if type(s) is str and s.endswith("mp4")] + [None])[0]
    x = None
    if "urls" in d:
        x = sub(d)
    if x == None:
        for v in d.values():
            if "urls" in v:
                # print(v)
                x = sub(v)
            if x != None:
                break
    return x


# Get json
@aiocron.crontab(str(config["crontab"]["moder"]))
async def fetch_more():
    async with aiocfscrape.CloudflareScraper(headers={"x-api-key": f"{config['tg_bot']['api_key']}"}) as session:
        print(dayofweek())
        await bot.send_message(moderation_id,
                               text=f"{db_main.count_documents({'posted?': False, 'shit?': False, 'dayofpost': dayofweek()})}"
                                    f" Left from last {dayofweek()}")
        for api_link in actual_api_links():
            async with session.get(api_link) as resp:
                json_file = await resp.json()
                print(f'get! {api_link}')
                _db_main = client.main_db.main_coll
                try:
                    _db_main.insert_many(
                        [{
                            "_id": x["id"],
                            "file_type": "jpg",
                            "file_link": x["url"],
                            "shorty": x["url"],
                            "sources": "url",
                            "file_size": int(12345),
                            "width": x["width"],
                            "height": x["height"],
                            "dayofpost": dayofweek(),
                            "search_date": datetime.datetime.now().strftime("%d/%m/%Y %I:%M (%A)"),
                            "children": "None",
                            "parent": "None",
                            "has_children": "None",
                            "score": int(999),
                            "album?": False,
                            "mod_posted?": False,
                            "posted?": False,
                            "shit?": False
                        } for x in json_file], ordered=False)
                except BulkWriteError as e:
                    if {x['code'] for x in e.details['writeErrors']} != {11000}:
                        raise e
                except TypeError:
                    print('\nNothing find today', api_link)
                except Exception as e:
                    await bot.send_message(moderation_id,
                                           text=f"Search is broken {api_link} \n {e}")
                finally:
                    await asyncio.sleep(3)

    resp = db_main.find({"mod_posted?": False}).sort("score", pymongo.DESCENDING)
    await bot.send_photo(moderation_id, photo=(config['search']['search_pic']),
                         caption=f"Start searching on {dayofweek()}"
                                 f"\n{db_main.count_documents({'mod_posted?': False})} posts for today"
                                 f"\n{db_main.count_documents({'posted?': False, 'shit?': False, 'dayofpost': dayofweek()})}"
                                 f"main_posts for {dayofweek()}")
    _n = 0
    for mod in resp:
        _n += 1
        _id = mod['_id']
        print('\nMod start :', _id)
        if _n % 19 == 0:
            await asyncio.sleep(45)
        if mod['file_link'] is None:
            print(_id, 'link is Nome, Moder')
            _db_main.update_one({'_id': mod['_id']}, {"$set": {'mod_posted?': True}})
            _db_main.update_one({'_id': mod['_id']}, {"$set": {'posted?': True}})
            _db_main.update_one({'_id': mod['_id']}, {"$set": {'shit?': True}})
            await bot.send_message(moderation_id,
                                   text=f"{config['search']['simple_req']}{_id} delete, scr: {mod['score']}, "
                                        f"\nError: url is not found source is {mod['sources']}")

        # Children
        elif mod['parent'] is None and mod["mod_posted?"] is False \
                and mod["file_type"] != "webm" and mod["file_type"] != "gif":
            print('\n!!===', _id, 'IS CHILDREN (ATTACH), HIS PARENT IS', mod['parent'])
            par_link = f"{config['search']['api_req']}{mod['parent']}"
            print(f"Parent (ATTACH) link: {par_link}")

            _db_main.update_one({'_id': mod['_id']}, {"$set": {'mod_posted?': True}})
            _db_main.update_one({'_id': mod['_id']}, {"$set": {'posted?': True}})
            _db_main.update_one({'_id': mod['_id']}, {"$set": {'shit?': True}})

            async with aiocfscrape.CloudflareScraper(
                    headers={"x-api-key": f"{config['tg_bot']['api_key']}"}) as session:
                async with session.get(par_link) as child:
                    _json_file = await child.json()
                    print("!====", _id, "ADD PARENTS (ATTACH)", par_link)
                    try:
                        _db_main.insert_many(
                            [{
                                "_id": x["id"],
                                "file_type": "jpg",
                                "file_link": x["url"],
                                "shorty": x["url"],
                                "sources": "url",
                                "file_size": int(12345),
                                "width": x["width"],
                                "height": x["height"],
                                "dayofpost": dayofweek(),
                                "search_date": datetime.datetime.now().strftime("%d/%m/%Y %I:%M (%A)"),
                                "children": "None",
                                "parent": "None",
                                "has_children": "None",
                                "score": int(999),
                                "album?": False,
                                "mod_posted?": False,
                                "posted?": False,
                                "shit?": False
                            } for x in json_file], ordered=False)
                        print("Parent (ATTACH)", mod['parent'], "added")
                    except BulkWriteError as e:
                        if {x['code'] for x in e.details['writeErrors']} != {11000}:
                            raise e
                    finally:
                        await asyncio.sleep(3)

            _db_main.update_one({'_id': mod['parent']}, {"$set": {'album?': True}})
            curs_par = _db_main.find_one({"_id": mod['parent']})
            print('!===Attached', mod['parent'], 'HAS SOME CHILDREN:', curs_par['children'], 'create album_link')
            for x in curs_par['children']:
                cild_link = f"{config['search']['api_req']}{x}"
                print('Children link:', cild_link)
                async with aiocfscrape.CloudflareScraper(
                        headers={"x-api-key": f"{config['tg_bot']['api_key']}"}) as session:
                    print("!====", curs_par['_id'], "ADD CHILDREN LINK (ATTACH)")
                    async with session.get(cild_link) as child:
                        _json_file = await child.json()
                        print(f'get! {cild_link}')
                        try:
                            print("Added first (ATTACH) album_link", cild_link)
                            for _x in _json_file["posts"]:
                                if _x["file"]["size"] < 5211422 and _x["file"]["width"] < 3001 and \
                                        _x["file"]["height"] < 3001:
                                    _db_main.update_one({
                                        "_id": _x["relationships"]["parent_id"]},
                                        {"$addToSet": {"album_link": _x["file"]["url"]}})
                                    print('Added second (ATTACH) album_link to', curs_par['_id'], 'from', _x["id"],
                                          _x["file"]["url"])
                                else:
                                    _db_main.update_one({
                                        "_id": _x["relationships"]["parent_id"]},
                                        {"$addToSet": {"album_link": _x["sample"]["url"]}})
                                    print('Added SHORTY (ATTACH) second album_link to', curs_par['_id'], 'from',
                                          _x["id"],
                                          _x["sample"]["url"])
                            print("$$ADD TO DB (ATTACH) CHILDREN DONE")
                        except BulkWriteError as e:
                            if {x['code'] for x in e.details['writeErrors']} != {11000}:
                                raise e
                        finally:
                            await asyncio.sleep(3)

            # children
            try:
                cursor = _db_main.find_one({"_id": curs_par['_id']}, {"_id": 0, "album_link": 1})
                media = types.MediaGroup()
                if curs_par["file_size"] < 5211422 and curs_par["width"] < 3001 and curs_par["height"] < 3001:
                    media.attach_photo(curs_par["file_link"], "ORIG album")
                    print('! ATTACH Attach 1st pic link from parent', curs_par["file_link"])
                else:
                    media.attach_photo(curs_par["shorty"], "ORIG SHORTY album")
                    print('!ATTACH Attach SHORTY 1st pic link from parent', curs_par["file_link"])
                print("$$ (ATTACH) CHILDREN DONE")
                for i in cursor["album_link"]:
                    media.attach_photo(i)
                    print(type(i), i, 'child link added')
                await bot.send_media_group(moderation_id, media=media)
                await asyncio.sleep(4)
                await bot.send_photo(moderation_id, photo=(curs_par["shorty"]),
                                     caption=f"Click the button to mark a comic (ATTACH) as shit, \n"
                                             f"parent is {curs_par['_id']}, scr: {curs_par['score']}",
                                     reply_markup=create_button(curs_par['_id']))
                _db_main.update_one({'_id': curs_par['_id']}, {"$set": {'mod_posted?': True}})
                print({"_id": curs_par['_id']}, "Mod_album((ATTACH)) posted, MOD_POSTED : TRUE")
                await asyncio.sleep(25)

            except Exception as e:
                print('ERROR: Album ((ATTACH)) posting failed, parent:', mod['parent'], '\n',
                      e)
                _db_main.update_one({'_id': mod['_id']}, {"$set": {'mod_posted?': True}})
                _db_main.update_one({'_id': mod['_id']}, {"$set": {'posted?': True}})
                _db_main.update_one({'_id': mod['_id']}, {"$set": {'shit?': True}})
                await bot.send_photo(moderation_id, photo=(curs_par['shorty']),
                                     caption=f"Album ((ATTACH)) posting failed, \nparent:, {curs_par['file_link']}, "
                                             f"first ch: {mod['parent']},  cld: {curs_par['children']}, "
                                             f"scr: {curs_par['score']}")
            finally:
                await asyncio.sleep(25)
            _db_main.update_one({'_id': mod['parent']}, {"$set": {'mod_posted?': True}})

        # Parent
        elif mod['parent'] is None and mod["has_children"] is True and mod["mod_posted?"] is False \
                and mod["file_type"] != "webm" and mod["file_type"] != "gif":
            print('\n!!!~~~', _id, '(ORIG) HAS SOME CHILDREN:', mod['children'], 'create album_link')
            _db_main.update_one({'_id': _id}, {"$set": {'album?': True}})
            for x in mod['children']:
                cild_link = f"{config['search']['api_req']}{x}"
                print('(ORIG) Children link:', cild_link)
                async with aiocfscrape.CloudflareScraper(
                        headers={"x-api-key": f"{config['tg_bot']['api_key']}"}) as session:
                    print("!====", _id, "ADD CHILDREN LINK (ORIG)")
                    async with session.get(cild_link) as child:
                        _json_file = await child.json()
                        print(f'get! {cild_link}')
                        try:
                            print("Added first (ORIG) album_link", _id)
                            for _x in _json_file["posts"]:
                                if _x["file"]["size"] < 5211422 and _x["file"]["width"] < 3001 and \
                                        _x["file"]["height"] < 3001:
                                    _db_main.update_one({
                                        "_id": _x["relationships"]["parent_id"]},
                                        {"$addToSet": {"album_link": _x["file"]["url"]}})
                                    print('Added second (ORIG) album_link to', _id, 'from', _x["id"], _x["file"]["url"])
                                else:
                                    _db_main.update_one({
                                        "_id": _x["relationships"]["parent_id"]},
                                        {"$addToSet": {"album_link": _x["sample"]["url"]}})
                                    print('Added SHORTY (ORIG) second album_link to', _id, 'from', _x["id"],
                                          _x["sample"]["url"])
                            print("(ORIG) ADD CHILDREN DONE")
                        except BulkWriteError as e:
                            if {x['code'] for x in e.details['writeErrors']} != {11000}:
                                raise e
                        finally:
                            await asyncio.sleep(3)

            # Clear parent
            try:
                cursor = _db_main.find_one({"_id": _id}, {"_id": 0, "album_link": 1})
                media = types.MediaGroup()
                if mod["file_size"] < 5211422 and mod["width"] < 3001 and mod["height"] < 3001:
                    media.attach_photo(mod["file_link"], "ORIG album")
                    print('ORIG Attach 1st pic link from parent', mod["file_link"])
                else:
                    media.attach_photo(mod["shorty"], "ORIG SHORTY album")
                    print('ORIG Attach SHORTY 1st pic link from parent', mod["file_link"])
                print("$ADD (ATTACH) CHILDREN DONE")
                for i in cursor["album_link"]:
                    media.attach_photo(i)
                    print(type(i), i, 'child link added (ORIG)')
                await bot.send_media_group(moderation_id, media=media)
                print(mod["shorty"])
                await asyncio.sleep(4)
                await bot.send_photo(moderation_id, photo=(mod["shorty"]),
                                     caption=f"Click the button to mark a comic (ORIG) as shit \nparent is {_id}, "
                                             f"scr: {mod['score']}",
                                     reply_markup=create_button(_id))
                _db_main.update_one({'_id': _id}, {"$set": {'mod_posted?': True}})
                print({"_id": _id}, "Mod_album((ORIG)) posted, MOD_POSTED: TRUE")
                await asyncio.sleep(20)

            except Exception as e:
                print('ERROR: Album ((ORIG)) posting failed, parent:', _id, e)
                _db_main.update_one({'_id': mod['_id']}, {"$set": {'mod_posted?': True}})
                _db_main.update_one({'_id': mod['_id']}, {"$set": {'posted?': True}})
                _db_main.update_one({'_id': mod['_id']}, {"$set": {'shit?': True}})
                await bot.send_photo(moderation_id, photo=(mod['shorty']),
                                     caption=f"Album (ORIG) posting failed, \nparent:, {mod['file_link']}, "
                                             f"parent?: {mod['parent']}, has_children?: "
                                             f"{mod['has_children']}, cld: {mod['children']}, scr: {mod['score']}")
            finally:
                await asyncio.sleep(10)
            _db_main.update_one({'_id': mod['_id']}, {"$set": {'mod_posted?': True}})

        else:
            try:
                if mod["mod_posted?"] is False and mod["file_size"] < 52420000:
                    if mod['file_type'] == "webm":
                        print(_id, "WEBM_moder")
                        await bot.send_video(moderation_id, video=(mod['480p']),
                                             caption=f"{mod['_id']}, scr:{mod['score']} \nsrc: {mod['sources']}",
                                             reply_markup=create_button(_id))
                        _db_main.update_one({'_id': mod['_id']}, {"$set": {'mod_posted?': True}})
                        await asyncio.sleep(5)
                    elif mod['file_type'] == "gif":
                        print(_id, "GIF_moder")
                        await bot.send_animation(moderation_id, animation=(mod['file_link']),
                                                 caption=f"{mod['_id']}, scr:{mod['score']} \nsrc: {mod['sources']}",
                                                 reply_markup=create_button(_id))
                        _db_main.update_one({'_id': mod['_id']}, {"$set": {'mod_posted?': True}})
                        await asyncio.sleep(5)
                    else:
                        if mod["file_size"] < 5242800:
                            print(_id, 'Normal_pic_moder')
                            await bot.send_photo(moderation_id, photo=(mod['file_link']),
                                                 caption=f"{mod['_id']}, scr:{mod['score']}, Normal",
                                                 reply_markup=create_button(_id))
                            _db_main.update_one({'_id': mod['_id']}, {"$set": {'mod_posted?': True}})
                            await asyncio.sleep(5)
                        else:
                            print(_id, 'LARGE_pic_moder')
                            await bot.send_photo(moderation_id, photo=(mod['shorty']),
                                                 caption=f"{mod['_id']}, , scr: {mod['score']}, Large",
                                                 reply_markup=create_button(_id))
                            _db_main.update_one({'_id': mod['_id']}, {"$set": {'mod_posted?': True}})
                            await asyncio.sleep(5)
                else:
                    print(_id, "Too huge")
                    await bot.send_photo(moderation_id, photo=(mod['shorty']),
                                         caption=f"{config['search']['simple_req']}{mod['_id']}, \n{mod['sources']},"
                                                 f" \nscr: {mod['score']} Too huge for uploading id:{_id}")
                    await asyncio.sleep(10)
                    _db_main.update_one({'_id': mod['_id']}, {"$set": {'mod_posted?': True}})
                    _db_main.update_one({'_id': mod['_id']}, {"$set": {'posted?': True}})
                    _db_main.update_one({'_id': mod['_id']}, {"$set": {'shit?': True}})

            except AttributeError as e:
                print(_id, 'Error:', e)
                await bot.send_message(moderation_id, text=f'Error: {e}, mod_delete id:{_id}, scr: {mod["score"]}')
                _db_main.update_one({'_id': mod['_id']}, {"$set": {'mod_posted?': True}})
                _db_main.update_one({'_id': mod['_id']}, {"$set": {'posted?': True}})
                _db_main.update_one({'_id': mod['_id']}, {"$set": {'shit?': True}})
                await asyncio.sleep(10)
                continue
            except Exception as e:
                await asyncio.sleep(5)
                print(_id, 'mod_post_error\n', e)
                await bot.send_message(moderation_id, text=f"{config['search']['simple_req']}{_id}, "
                                                           f"\n{mod['sources']}, \nscr: {mod['score']} "
                                                           f"Error id:{_id}")
                _db_main.update_one({'_id': mod['_id']}, {"$set": {'mod_posted?': True}})
                _db_main.update_one({'_id': mod['_id']}, {"$set": {'posted?': True}})
                _db_main.update_one({'_id': mod['_id']}, {"$set": {'shit?': True}})
                await asyncio.sleep(10)
                continue
    await asyncio.sleep(30)
    await bot.send_message(moderation_id, text=f"Posting found on {dayofweek()} finished")
    print('Mod_posted done')


# Channel posting
@aiocron.crontab(str(config["crontab"]["channel"]))
async def ch_posting():
    print('~~~POSTING IN CHANNEL STARTED')
    post = db_main.find({"posted?": False, 'shit?': False, "dayofpost": dayofweek()}).sort("score", pymongo.DESCENDING)
    _db_main = client.main_db.main_coll
    for pst in post:
        _id = pst['_id']
        await bot.send_message(moderation_id,
                               text=f"Start posting on channel, id: {pst['_id']} "
                                    f"\n{db_main.count_documents({'posted?': False, 'shit?': False, 'dayofpost': dayofweek()})}"
                                    f" main_posts for {dayofweek()}")
        try:
            _id = pst['_id']
            if pst["shit?"] is False:
                if pst['file_type'] == "gif":
                    print("GIF_channel", pst["_id"])
                    await bot.send_animation(channel_id, animation=(pst['file_link']), caption="#GIF")
                    _db_main.update_one({'_id': pst['_id']}, {"$set": {'posted?': True}})
                    break
                elif pst['file_type'] == "webm":
                    print("WEBM_channel", pst["_id"])
                    await bot.send_video(channel_id, video=(pst['480p']), caption="#video")
                    _db_main.update_one({'_id': pst['_id']}, {"$set": {'posted?': True}})
                    break
                elif pst['album?'] is True:
                    media = types.MediaGroup()
                    if pst["file_size"] < 5242800 and pst["width"] < 3001 and pst["height"] < 3001:
                        media.attach_photo(pst["file_link"])
                        print('CHAN Attach 1st pic link from parent', pst["file_link"])
                    else:
                        media.attach_photo(pst["shorty"])
                        print('CHAN Attach SHORTY 1st pic link from parent', pst["file_link"])
                    # media.attach_photo(pst["file_link"])
                    for i in pst["album_link"]:
                        media.attach_photo(i)
                        print(type(i), i, 'child link added')
                    await bot.send_media_group(channel_id, media=media)
                    print('Channel album post', pst["_id"])
                    _db_main.update_one({'_id': pst['_id']}, {"$set": {'posted?': True}})
                    break
                else:
                    if pst["file_size"] < 5242800:
                        await bot.send_photo(channel_id, photo=(pst['file_link']))
                        print(_id, 'Normal_pic_channel', pst["_id"])
                        _db_main.update_one({'_id': pst['_id']}, {"$set": {'posted?': True}})
                        break
                    else:
                        await bot.send_photo(channel_id, photo=(pst['shorty']))
                        print(_id, 'LARGE_pic_channel', pst["_id"])
                        _db_main.update_one({'_id': pst['_id']}, {"$set": {'posted?': True}})
                        break
        except AttributeError as er:
            await bot.send_message(moderation_id, text=f'Channel Error: {er}, ch_delete id:{_id}')
            print(_id, 'Error (channel):', er)
            await asyncio.sleep(15)
        except Exception as e:
            print("Idn, err post in channel \n", e)


#
if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    executor.start_polling(dp, skip_updates=True, loop=loop)
