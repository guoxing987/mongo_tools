# -*- coding: utf-8 -*-
# @FileName: check_index.py
# @Time    : 2019/11/8 9:06
# @Author  : Damon Guo
# @version : 0.1

from pymongo import MongoClient
from concurrent.futures import ThreadPoolExecutor

class MONGODB():
    def __init__(self,uri):
        self.uri = uri
        self.client = MongoClient(uri)



def index_use_count(client,db,coll):
    res = client[db][coll].aggregate( [ { "$indexStats": {}}])
    for i in res:
        if i["name"] != "_id_":
            # print(db + "." + coll +" 索引名称: \'"+ i["name"]  +  "\' 索引被使用的次数为: " + str(i["accesses"]["ops"]))
            print(db + "\t" + coll +"\t"+ i["name"]  +  "\t" + str(i["accesses"]["ops"]))

def exec_incident(client):
    eliminate_db = ["admin", "config", "local", "DB_MONITOR", "system"]
    all_databases = client.list_database_names()
    app_dbs = list(set(all_databases).difference(set(eliminate_db)))

    for db in app_dbs:
        all_colls = client[db].list_collection_names()
        eliminate_colls = ["system.js", "system.profile", ]
        colls = list(set(all_colls).difference(set(eliminate_colls)))
        # print(list(colls))
        for coll in colls:
            index_use_count(client,db, coll)


if '__name__' == '__main__':
    try:
        uri_list = ["mongodb://root:AdminDBA%2456@10.25.14.109:27067,10.26.14.52:27067,10.26.14.53:27067,10.26.14.51:27067,10.25.14.110:27067/?replicaSet=trackgw&authSource=admin",
        "mongodb://HCC_DAMON_GUO:123321QQ!@10.25.14.109:27047,10.26.14.52:27047,10.26.14.53:27047,10.25.14.110:27047,10.26.14.51:27047/?replicaSet=rs1&authSource=admin",
        "mongodb://HCC_DAMON_GUO:123321QQ!@10.25.14.109:27027,10.25.14.110:27027,10.26.14.51:27027,10.26.14.52:27027,10.26.14.53:27027/?replicaSet=rs1&authSource=admin",
        "mongodb://HCC_DAMON_GUO:123321QQ!@10.25.14.109:27037,10.25.14.110:27037,10.26.14.52:27037,10.26.14.53:27037,10.26.14.51:27037/?replicaSet=onlineRS1&authSource=admin",
        "mongodb://HCC_DAMON_GUO:123321QQ!@10.25.14.109:27057,10.26.14.52:27057,10.26.14.51:27057,10.26.14.53:27057,10.25.14.110:27057/?replicaSet=capprs1&authSource=admin"]

        for uri in uri_list:
            client = MongoClient(uri)
            exec_incident()
            client.close()

    except Exception as e:
        print(e)






