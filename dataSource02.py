import pymongo
import re
import json
from datetime import datetime
url = 'mongodb://dds-bp159c71a9b119841194-pub.mongodb.rds.aliyuncs.com:3717,dds-bp159c71a9b119842982-pub.mongodb.rds.aliyuncs.com:3717'
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
author_id = {}

# 02源category补充
source_2 = open('/opt/miaozhai_data/DataSource02.json', 'r', encoding='utf-8')
digest = source_2.readline()
# digests_1 = data_2.find()
# for digest in digest_1:
count_2 = 0
while digest:
    count_2 += 1
    if count_2 == 10000:
        break
    try:
        digest = json.loads(digest)
        if '&#' in digest['book_name'] or bool(re.search('[a-z]', digest['book_name'])):
            continue
        if not digest.get('status'):
            key_data = 0
            # 02源中ISBN只有为空没有为暂无的
            if digest['publish_info']['ISBN'] != '':
                # 若存在isbn，则直接以isbn判断
                data = data_t.find_one({'publish_info.isbn': digest['publish_info']['ISBN']})
            else:
                # 若不存在isbn，则暂时以书名判断，作者判断后续跟进
                data = data_t.find_one({'book_name': digest['book_name']})
            if not data:
                key_data = 1
                # 判断是否存在data，不存在就创建新的书籍，内容之后填充
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
                    'publish_info': {'ISBN': digest['publish_info']['ISBN'], 'publisher': '', 'publish_date': '',\
                                       'binding': '', 'words': '', 'price': '', 'pages': ''},
                    'catalog_info': [],
                    'series': {},
                    # 暂时没用到，后续讨论数据类型
                    'all_version': []
                }
            # 此时有匹配的书籍则正常整理，未匹配的为新生成的只有ISBN的新书籍进行整理
            if data['book_name'] == '':
                # 02源中有14条book_name为空字符串，暂时不考虑
                data['book_name'] = digest['book_name']
            if data['cover_thumbnail'] == '':
                data['cover_thumbnail'] = digest['cover_thumbnail']
            if data['summary'] == '':
                data['summary'] = digest['summery']
            if data['score'] == '':
                # 分数为0时为字符串，推测可能等同于空,另外02源分数除以10
                if digest['score'] == '0':
                    data['score'] = ''
                else:
                    data['score'] = str(int(digest['score']) / 10)
            # 02源中作者类型为字符串，多个作者空格分开
            if data['author_list'] == []:
                writer_list = []
                authors = digest['book_author'].split(' ')
                for writer in authors:
                    writers = {'id': 100000, 'author_name': writer}
                    writer_list.append(writers)
                #     if writer not in author_id.keys():
                #         while True:
                #             n = random.randint(100000, 999999)
                #             if n not in author_id.values():
                #                 author_id[writer] = n
                #                 writer_info = {'id': n, 'author_name': writer}
                #                 writer_list.append(writer_info)
                #                 break
                #     else:
                #         writer_info = {'id': author_id[writer], 'author_name': writer}
                #         writer_list.append(writer_info)
                data['author_list'] = writer_list
            # 02源中publish_info中数据都是全的，不会出现字段丢失的问题
            if data['publish_info']['publish_date'] == '':
                data['publish_info']['publish_date'] = digest['publish_info']['publish_time']
            # 02源中words类型为int32，为0表示为空
            if data['publish_info']['words'] == '':
                # 除以一万保留两位小数
                data['publish_info']['words'] = str(round(digest['publish_info']['words'] / 10000, 2)) + '万字'
            # 02源的price类型为double，为0时表示为空，01源中为字符串，暂无为空
            if data['publish_info']['price'] == '':
                data['publish_info']['price'] = digest['publish_info']['price']
            if data['category'] == []:
                categorys = digest['publish_info']['category'].split('-')
                for category in categorys:
                    data['category'].append({'id': 100, 'name': category})
                    check = data_c.find_one({'category': category})
                    if not check:
                        data_c.insert({'category': category})
            # 02源catalog不为空则优先选用02源
            if digest['catalog'] != []:
                catalog_list = []
                for cata in digest['catalog']:
                    digest_catalog = cata['chapter_title']
                    catalog_list.append(digest_catalog)
                data['catalog_info'] = catalog_list
            data['change_status'] = '1'
            data['check_status'] = '1'
            data['shelf_status'] = '1'
            if key_data != 1:
                data_t.update({'_id': data['_id']}, {'$set': data})
            else:
                data_t.insert(data)
            bookid = str(data['_id'])
            book_name = data['book_name']
            excerpt_insert = []
            for hot_excerpt in digest['hot_excerpt']:
                if len(hot_excerpt['mark_text']) <= 5:
                    continue
                # 02源中章节id类型为int32，实际为2n-1
                exp_chp_id = hot_excerpt['chapterUid']
                # 02源中hot_excerpt的章节id并不准确对应标题，暂时不取
                # try:
                #     chp_id = 2 * exp_chp_id - 1
                #     # 结合列表求章节标题，若超出列表最大则留空
                #     exp_chp_title = data['catalog_info'][chp_id]
                # except Exception as e:
                #     exp_chp_title = ''
                exp_chp_title = ''
                exp_text = hot_excerpt['mark_text']
                is_hot_exp = "1"
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
            discuss_insert = []
            for hot_discuss in digest['hot_discuss']:
                if len(hot_discuss['mark_text']) <= 5:
                    continue
                exp_chp_id = str(hot_discuss['chapter_id'])
                exp_chp_title = hot_discuss['chapter_title']
                exp_text = hot_discuss['mark_text']
                is_hot_exp = "1"
                discuss_insert.append({
                    'bookid': bookid,
                    'book_name': book_name,
                    'exp_chp_id': exp_chp_id,
                    'exp_chp_title': exp_chp_title,
                    'exp_text': exp_text,
                    'is_hot_exp': is_hot_exp
                })
            if discuss_insert != []:
                data_e.insert_many(discuss_insert)
            # digest['status'] = 'success'
            # data_2.update({'_id': digest['_id']}, {'$set': digest})
    except Exception as e:
        # digest['status'] = e
        # data_2.update({'_id': digest['_id']}, {'$set': digest})
        # print('digest_1 error', e, digest)
        print('Error when: ' + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + 'with: ' + str(e))
        # with open('./Errorlog.log', 'w+') as log:
        #     info = 'Error when: ' + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + 'with: ' + e + 'in: ' + digest
        #     log.write(info)
    finally:
        digest = source_2.readline()
        if count_2 % 500 == 0:
            print('DataSource02 loading: %d / 47906' % count_2)
        #     with open('./translog02.log', 'w+') as log:
        #         log.write('DataSource02 loading: %d / 47906' % count_2)

# source_3 = open('/opt/miaozhai_data/DataSource03.json', 'r', encoding='utf-8')
# digest = source_3.readline()
# count_3 = 0
# # digest_2 = data_3.find()
# # for digest in digest_2:
# while digest:
#     count_3 += 1
#     if count_3 == 10000:
#         break
#     try:
#         digest = json.loads(digest)
#         if '&#' in digest['bookName'] or bool(re.search('[a-z]', digest['bookName'])):
#             continue
#         if not digest.get('status'):
#             key_data = 0
#             # 03源isbn只存在为空字符串，没有暂无
#             if digest['isbn'] != '':
#                 data = data_t.find_one({'publish_info': {'isbn': digest['isbn']}})
#             else:
#                 # 03源中bookName一定存在
#                 data = data_t.find_one({'book_name': digest['bookName']})
#                 # 判断是否存在data，不存在就创建新的书籍，内容之后填充
#             if not data:
#                 key_data = 1
#                 data = {
#                     'book_name': '',
#                     'subtitle': '',
#                     'original_name': '',
#                     'cover_thumbnail': '',
#                     'summary': '',
#                     'category': [],
#                     'tags': [],
#                     'score': '',
#                     'author_list': [],
#                     'publish_info': {'ISBN': digest['isbn'], 'publisher': '', 'publish_date': '', \
#                                      'binding': '', 'words': '', 'price': '', 'pages': ''},
#                     'catalog_info': [],
#                     'series': {},
#                     'all_version': []
#                 }
#             if data['book_name'] == '':
#                 # 03源中没有bookName为空
#                 data['book_name'] = digest['bookName']
#             if data['cover_thumbnail'] == '':
#                 data['cover_thumbnail'] = digest['picUrl']
#             if data['summary'] == '':
#                 data['summary'] = digest['desc']
#             if digest['categorys'] != []:
#                 for d_cate in digest['categorys']:
#                     key_cate = 0
#                     for da_cate in data['category']:
#                         if d_cate['name'] == da_cate['name']:
#                             key_cate = 1
#                             break
#                     if key_cate == 0:
#                         data['category'].append(d_cate)
#                 # 判断data中无该字段数据或03源有该字段数据，则以03源为优
#                 data['category'] = digest['categorys']
#             if digest['tagInfo'] != []:
#                 # 取并集
#                 tag = []
#                 for taginfo in digest['tagInfo']:
#                     tag.append(taginfo['name'])
#                 tag = set(tag).union(data['tags'])
#                 tag = list(tag)
#                 data['tags'] = tag
#             if digest['attribute']['star'] != 0:
#                 # 03源中star字段有为double类型，没有为int32的0
#                 data['score'] = digest['attribute']['star']
#             # 03源中作者类型为字符串，多个作者空格分开
#             if digest['authorList'] != []:
#                 writer_list = []
#                 for writer in digest['authorList']:
#                     # # 作者id和名字都不在list中，直接导入
#                     # if writer['id'] not in author_id.values() and writer['name'] not in author_id.keys():
#                     #     author_id[writer['name']] = writer['id']
#                     #     writer_info = {'id': writer['id'], 'author_name': writer['name']}
#                     # # id被占用，就生成一个新的id
#                     # elif writer['id'] in author_id.values() and writer['name'] not in author_id.keys():
#                     #     while True:
#                     #         n = random.randint(100000, 999999)
#                     #         if n not in author_id.values():
#                     #             author_id[writer['name']] = n
#                     #             writer_info = {'id': n, 'author_name': writer['name']}
#                     #             break
#                     # # list中已有作者，取列表中的id
#                     # elif writer['name'] in author_id.keys():
#                     #     writer_info = {'id': author_id[writer['name']], 'author_name': writer['name']}
#                     # else:
#                     #     writer_info = {'id': author_id[writer['name']], 'author_name': writer['name']}
#                     # writer_list.append(writer_info)
#                     writer_list.append(writer['name'])
#                 data['author_list'] = writer_list
#             # 03源中有1w多条没有publisher，为空字符串，没有暂无字段
#             if data['publish_info']['publisher'] == '':
#                 data['publish_info']['publisher'] = digest['publisher']
#             # 03源中wordCount可以直接用,没有空字符串也没有暂无字段
#             if data['publish_info']['words'] == '':
#                 data['publish_info']['words'] = digest['wordCount']
#             # catalog_info取id和章节名称
#             if digest['catalog'] != []:
#                 catalog_list = []
#                 # for cata in digest['catalog']:
#                 #     digest_catalog = {}
#                 #     digest_catalog['catalog_id'] = cata['id']
#                 #     digest_catalog['catalog_title'] = cata['chapterName']
#                 #     catalog_list.append(digest_catalog)
#                 for cata in digest['catalog']:
#                     catalog_list.append(cata['chapterName'])
#                 data['catalog_info'] = catalog_list
#             if key_data == 1 and digest['excerpt'] != []:
#                 data_t.insert(data)
#                 # data = data_t.find_one(data)
#             # bookid = str(data.get('_id', '')) ## _id
#             bookid = str(data.get('_id', ''))
#             book_name = data.get('book_name', '')
#             excerpt_insert = []
#             for excerpt in digest['excerpt']:
#                 if len(excerpt['ext_summary']) < 5:
#                     continue
#                 exp_chp_id = excerpt['id']
#                 exp_chp_title = excerpt['ext_chapterName']
#                 exp_text = excerpt['ext_summary']
#                 is_hot_exp = "0"
#                 excerpt_insert.append({
#                     'bookid': bookid,
#                     'book_name': book_name,
#                     'exp_chp_id': exp_chp_id,
#                     'exp_chp_title': exp_chp_title,
#                     'exp_text': exp_text,
#                     'is_hot_exp': is_hot_exp
#                 })
#             if excerpt_insert != []:
#                 data_e.insert_many(excerpt_insert)
#             # digest['status'] = 'success'
#             # data_2.update({'_id': digest['_id']}, {'$set': digest})
#             data_t.update({'_id': data['_id']}, {'$set': data})
#     except Exception as e:
#         print('Error when: ' + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + 'with: ' + e + 'in: ' + digest)
#         # digest['status'] = e
#         # data_2.update({'_id': digest['_id']}, {'$set': digest})
#         # print('digest_2 error', e, digest)
#         # with open('./Errorlog03.log', 'w+') as log:
#         #     info = 'Error when: ' + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + 'with: ' + e + 'in: ' + digest
#         #     log.write(info)
#     finally:
#         digest = source_3.readline()
#         if count_3 % 500 == 0:
#             print(datetime.now().strftime('%Y-%m-%d %H:%M:%S') + ':' + 'DataSource03 loading:%d / 74911' % count_3)
#             # with open('./translog03.log', 'w+') as log:
#             #     log.write(datetime.now().strftime('%Y-%m-%d %H:%M:%S') + ':' + 'DataSource03 loading:%d / 74911' % count_3)
