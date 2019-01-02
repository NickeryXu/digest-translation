import pymongo
import re
import json
from datetime import datetime
url = 'mongodb://dds-bp159c71a9b119841194-pub.mongodb.rds.aliyuncs.com:3717,dds-bp159c71a9b119842982-pub.mongodb.rds.aliyuncs.com:3717'
# URL = 'mongodb://localhost:27017/book'
# CLIENT = pymongo.MongoClient(URL)
# local = CLIENT.book
# data_t = local.t_books
# data_e = local.t_excerpts
replicaset = 'mgset-11082973'
user = 'book'
password = 'welcome1'
client = pymongo.MongoReplicaSetClient(url, replicaSet=replicaset)
db = client['tbooks']
db.authenticate(user, password)
data_t = db.t_books
data_e = db.t_excerpts
data_c = db.category_list
book_list = {}
# author_id = {}

source_3 = open('/opt/miaozhai_data/DataSource03.json', 'r', encoding='utf-8')
digest = source_3.readline()
count_3 = 0
# digest_2 = data_3.find()
# for digest in digest_2:
while digest:
    count_3 += 1
    if count_3 == 10000:
        break
    try:
        digest = json.loads(digest)
        if '&#' in digest['bookName'] or bool(re.search('[a-z]', digest['bookName'])):
            continue
        if not digest.get('status'):
            key_data = 0
            # 03源isbn只存在为空字符串，没有暂无
            if digest['isbn'] != '':
                data = data_t.find_one({'publish_info': {'isbn': digest['isbn']}})
            else:
                # 03源中bookName一定存在
                data = data_t.find_one({'book_name': digest['bookName']})
                # 判断是否存在data，不存在就创建新的书籍，内容之后填充
            if not data:
                key_data = 1
                data = {
                    'book_name': '',
                    'subtitle': '',
                    'original_name': '',
                    'cover_thumbnail': '',
                    'summary': '',
                    'category': [],
                    'tags': [],
                    'score': '',
                    'author_list': [],
                    'publish_info': {'ISBN': digest['isbn'], 'publisher': '', 'publish_date': '', \
                                     'binding': '', 'words': '', 'price': '', 'pages': ''},
                    'catalog_info': [],
                    'series': {},
                    'all_version': []
                }
            if data['book_name'] == '':
                # 03源中没有bookName为空
                data['book_name'] = digest['bookName']
            if data['cover_thumbnail'] == '':
                data['cover_thumbnail'] = digest['picUrl']
            if data['summary'] == '':
                data['summary'] = digest['desc']
            if digest['categorys'] != []:
                for d_cate in digest['categorys']:
                    key_cate = 0
                    for da_cate in data['category']:
                        if d_cate['name'] == da_cate['name']:
                            key_cate = 1
                            break
                    if key_cate == 0:
                        ck_cate = {'id': 100, 'name': d_cate['name']}
                        check = data_c.find_one({'category': d_cate['name']})
                        if not check:
                            data_c.insert({'category': d_cate['name']})
                        data['category'].append(ck_cate)
                # 判断data中无该字段数据或03源有该字段数据，则以03源为优
                # data['category'] = digest['categorys']
            if digest['tagInfo'] != []:
                # 取并集
                tag = []
                for taginfo in digest['tagInfo']:
                    tag.append(taginfo['name'])
                tag = set(tag).union(data['tags'])
                tag = list(tag)
                data['tags'] = tag
            if digest['attribute']['star'] != 0:
                # 03源中star字段有为double类型，没有为int32的0
                if '$numberInt' in digest['attribute']['star'].keys():
                    data['score'] = digest['attribute']['star']['$numberInt']
                elif '$numberDouble' in digest['attribute']['star'].keys():
                    data['score'] = digest['attribute']['star']['$numberDouble']
                else:
                    data['score'] = digest['attribute']['star']
            # 03源中作者类型为字符串，多个作者空格分开
            if digest['authorList'] != []:
                writer_list = []
                for writer in digest['authorList']:
                    # # 作者id和名字都不在list中，直接导入
                    # if writer['id'] not in author_id.values() and writer['name'] not in author_id.keys():
                    #     author_id[writer['name']] = writer['id']
                    #     writer_info = {'id': writer['id'], 'author_name': writer['name']}
                    # # id被占用，就生成一个新的id
                    # elif writer['id'] in author_id.values() and writer['name'] not in author_id.keys():
                    #     while True:
                    #         n = random.randint(100000, 999999)
                    #         if n not in author_id.values():
                    #             author_id[writer['name']] = n
                    #             writer_info = {'id': n, 'author_name': writer['name']}
                    #             break
                    # # list中已有作者，取列表中的id
                    # elif writer['name'] in author_id.keys():
                    #     writer_info = {'id': author_id[writer['name']], 'author_name': writer['name']}
                    # else:
                    #     writer_info = {'id': author_id[writer['name']], 'author_name': writer['name']}
                    # writer_list.append(writer_info)
                    writer_list.append({"id": 100000, "author_name": writer['name']})
                data['author_list'] = writer_list
            # 03源中有1w多条没有publisher，为空字符串，没有暂无字段
            if data['publish_info']['publisher'] == '':
                data['publish_info']['publisher'] = digest['publisher']
            # 03源中wordCount可以直接用,没有空字符串也没有暂无字段
            if data['publish_info']['words'] == '':
                data['publish_info']['words'] = digest['wordCount']
            # catalog_info取id和章节名称
            if digest['catalog'] != []:
                catalog_list = []
                # for cata in digest['catalog']:
                #     digest_catalog = {}
                #     digest_catalog['catalog_id'] = cata['id']
                #     digest_catalog['catalog_title'] = cata['chapterName']
                #     catalog_list.append(digest_catalog)
                for cata in digest['catalog']:
                    catalog_list.append(cata['chapterName'])
                data['catalog_info'] = catalog_list
            data['change_status'] = '1'
            data['check_status'] = '1'
            data['shelf_status'] = '1'
            if key_data == 1 and digest['excerpt'] != []:
                data_t.insert(data)
            elif key_data != 1:
                data_t.update({'_id': data['_id']}, {'$set': data})
                # data = data_t.find_one(data)
            # bookid = str(data.get('_id', '')) ## _id
            bookid = str(data.get('_id', ''))
            book_name = data.get('book_name', '')
            excerpt_insert = []
            for excerpt in digest['excerpt']:
                if len(excerpt['ext_summary']) < 5:
                    continue
                exp_chp_id = excerpt['id']
                exp_chp_title = excerpt['ext_chapterName']
                exp_text = excerpt['ext_summary']
                is_hot_exp = "0"
                excerpt_insert.append({
                    'bookid': bookid,
                    'book_name': book_name,
                    'exp_chp_id': exp_chp_id,
                    'exp_chp_title': exp_chp_title,
                    'exp_text': exp_text,
                    'is_hot_exp': is_hot_exp
                })
            if excerpt_insert != []:
                data_e.insert_many(excerpt_insert)
            # digest['status'] = 'success'
            # data_2.update({'_id': digest['_id']}, {'$set': digest})
    except Exception as e:
        print('Error when: ' + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + 'with: ' + str(e))
        # digest['status'] = e
        # data_2.update({'_id': digest['_id']}, {'$set': digest})
        # print('digest_2 error', e, digest)
        # with open('./Errorlog03.log', 'w+') as log:
        #     info = 'Error when: ' + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + 'with: ' + e + 'in: ' + digest
        #     log.write(info)
    finally:
        digest = source_3.readline()
        if count_3 % 500 == 0:
            print(datetime.now().strftime('%Y-%m-%d %H:%M:%S') + ':' + 'DataSource03 loading:%d / 74911' % count_3)
