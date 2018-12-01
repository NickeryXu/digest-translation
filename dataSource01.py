import pymongo
import random
import json
import multiprocessing
import datetime
url = 'mongodb://book:welcome1@joyeainfo.goss.top:38213/tbooks'
URL = 'mongodb://localhost:27017/book'
client = pymongo.MongoClient(url)
CLIENT = pymongo.MongoClient(URL)
db = client.tbooks
local = CLIENT.book
data_t = db.t_books
# data_e = db.t_excerpts
book_list = {}
author_id = {}

DataSource01 = ['/opt/miaozhai_data/DataSource01-1.json', '/opt/miaozhai_data/DataSource01-15.json',
                '/opt/miaozhai_data/DataSource01-4.json', '/opt/miaozhai_data/DataSource01-10.json',
                '/opt/miaozhai_data/DataSource01-16.json', '/opt/miaozhai_data/DataSource01-5.json',
                '/opt/miaozhai_data/DataSource01-11.json', '/opt/miaozhai_data/DataSource01-17.json',
                '/opt/miaozhai_data/DataSource01-6.json', '/opt/miaozhai_data/DataSource01-12.json',
                '/opt/miaozhai_data/DataSource01-18.json', '/opt/miaozhai_data/DataSource01-7.json',
                '/opt/miaozhai_data/DataSource01-13.json', '/opt/miaozhai_data/DataSource01-2.json',
                '/opt/miaozhai_data/DataSource01-8.json', '/opt/miaozhai_data/DataSource01-14.json',
                '/opt/miaozhai_data/DataSource01-3.json', '/opt/miaozhai_data/DataSource01-9.json'
]

def book_clear(source01):
    source_1 = open(source01, 'r', encoding='utf-8')
    book = source01.readline()
    print(datetime.now().strftime('%Y-%m-%d %H:%M:%S') + ': start' + source01)
    # with open('./translog01.log', 'w+') as log:
    #     info = datetime.now().strftime('%Y-%m-%d %H:%M:%S') + ': start' + source01
    #     log.write(info)
    # books = data_1.find()
    # for book in books:
    count_1 = 0
    while book:
        count_1 += 1
        try:
            book = json.loads(book)
            # 01源若出现异常则status为'exception'，无异常且处理完成则为‘success’
            if not book.get('status'):
                # isbn为暂无和空字符串的情况都有，另外01源中name字段没有值为空或暂无的
                if book['isbn'] == '' or book['isbn'] == '暂无':
                    isbn = ''
                elif book['isbn'] in book_list.keys():
                    #已去重，不作处理
                    continue
                elif book['isbn'] not in book_list.keys():
                    #book_list中没有key为book['isbn']说明该isbn目前唯一，直接开始整理
                    book_list[book['isbn']] = book['_id']
                    isbn = book['isbn']
                book_name = book['name']
                subtitle = book['subtitle']
                original_name = book['original_name']
                cover_thumbnail = book['cover']
                summary = book['summary']	#确实一个e一个a
                # category为分类，01源暂时不考虑
                category = []
                tags = book['tags']
                score = ''
                writer_list = []
                for writer in book['writers']:
                    if writer not in author_id.keys():
                        while True:
                            n = random.randint(10000000, 99999999)
                            if n not in author_id.values():
                                author_id[writer] = n
                                writer_info = {'id': n, 'author_name': writer}
                                writer_list.append(writer_info)
                                break
                    else:
                        writer_info = {'id': author_id[writer], 'author_name': writer}
                        writer_list.append(writer_info)
                author_list = writer_list
                publisher = book['publisher']
                publish_date = book['release_date']

                binding = book['binding']
                words = ''
                price = book['price']
                # 暂时直接取01数据源数据不作去空格等处理
                pages = book['pages_count']
                publish_info = {
                    'publisher': publisher,
                    'publish_date': publish_date,
                    'isbn': isbn,
                    'binding': binding,
                    'words': words,
                    'price': price,
                    'pages': pages,
                }
                # 01源中catalog为字符串，有空格有\n,先分割成列表，再去掉冗余元素去除空格
                catalog_info = []
                catalogs = book['catalog'].split('\n')
                for catalog in catalogs:
                    if catalog != '' and catalog != ' ':
                        catalog = catalog.replace(' ', '')
                        catalog_info.append(catalog)
                        catalog_info.append('')
                # excerpts = []
                series = book['series']
                all_version = ''
                # 数据插入数据库
                data_t.insert({
                    'book_name': book_name,
                    'subtitle': subtitle,
                    'original_name': original_name,
                    'cover_thumbnail': cover_thumbnail,
                    'summary': summary,
                    'category': category,
                    'tags': tags,
                    'score': score,
                    'author_list': author_list,
                    'publish_info': publish_info,
                    'catalog_info': catalog_info,
                    'series': series,
                    'all_version': all_version
                })
                # book['status'] = 'success'
                # data_1.update({'_id': book['_id']}, {'$set': book})
        except Exception as e:
            # book['status'] = e
            # data_1.update({'_id': book['_id']}, {'$set': book})
            # print('book error', e, book)
            print('Error when: ' + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + 'with: ' + e + 'in: ' + book)
            # with open('./Errorlog01.log', 'w+') as log:
            #     info = 'Error when: ' + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + 'with: ' + e + 'in: ' + book
            #     log.write(info)
        finally:
            book = source_1.readline()
            if count_1 % 500 == 0:
                with open('./translog01.log', 'w+') as log:
                    log.write(datetime.now().strftime('%Y-%m-%d %H:%M:%S') + ':' + source01 + 'loading: %d / 1000000' % count_1)
    # with open('./translog01.log', 'w+') as log:
    #     info = datetime.now().strftime('%Y-%m-%d %H:%M:%S') + ': end' + source01
    #     log.write(info)
    print(datetime.now().strftime('%Y-%m-%d %H:%M:%S') + ': end' + source01)


pool_book = multiprocessing.Pool(processes=5)
for source01 in DataSource01:
    pool_book.apply_async(book_clear, (source01,))
pool_book.close()
pool_book.join()