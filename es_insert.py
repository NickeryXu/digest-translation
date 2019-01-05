from elasticsearch import Elasticsearch
import pymongo

url = 'mongodb://dds-bp159c71a9b119841194-pub.mongodb.rds.aliyuncs.com:3717,dds-bp159c71a9b119842982-pub.mongodb.rds.aliyuncs.com:3717'
replicaset = 'mgset-11082973'
user = 'book'
password = 'welcome1'
client = pymongo.MongoReplicaSetClient(url, replicaSet=replicaset)
db = client['tbooks']
db.authenticate(user, password)
data_t = db.t_books
data_e = db.t_excerpts

es = Elasticsearch(['es-cn-0pp0wdtno00026tz5.public.elasticsearch.aliyuncs.com'], http_auth=('elastic', 'N+8atre&lt'), port=9200)
# es = Elasticsearch(['localhost:9200'])

books = data_t.find({'shelf_status': '1'})
# n = 0
book_doc = []
count = 0
for book in books:
    count += 1
    bid = str(book['_id'])
    del book['_id']
    doc = {'index': {'_index': 't_books', '_type': 'digest', '_id': bid}}
    book_doc.append(doc)
    book_doc.append(book)
    # 每500条插入一次
    if count % 500 == 0:
        es.bulk(index='t_books', doc_type='digest', body=book_doc, request_timeout=60)
        book_doc = []
# 剩余的再插入一次
es.bulk(index='t_books', doc_type='digest', body=book_doc, request_timeout=60)

# excerpts = data_e.find({'shelf_status': '1'})
# # m = 0
# excerpt_doc = []
# for excerpt in excerpts:
#     # m += 1
#     eid = str(excerpt['_id'])
#     del excerpt['_id']
#     doc = {'index': {'_index': 't_excerpts', '_type': 'digest', '_id': eid}}
#     excerpt_doc.append(doc)
#     excerpt_doc.append(excerpt)
#     # if m == 10:
#     #     break
# es.bulk(index='t_excerpts', doc_type='digest', body=excerpt_doc, request_timeout=60)
