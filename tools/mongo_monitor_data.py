#!/usr/bin/env python
# _*_ coding: UTF-8 _*_

########################################
# Created by Johnny.Jia
# Version: 1.5
# Created Time: 2018-01-01
#
# Updated by Damon.Guo
# Updated Time: 2019-11-12
########################################

from datetime import datetime, timedelta
import os
from pymongo import MongoClient
import pymongo
import yaml
import configparser


def getBaseDir(conf):
    f = open(conf)
    y = yaml.load(f, Loader=yaml.FullLoader)
    dbPath = y["storage"]["dbPath"]
    basedir = os.path.join("/", dbPath.split("/")[1])
    return basedir

def getPort(conf):
    f = open(conf)
    y = yaml.load(f, Loader=yaml.FullLoader)
    port = y["net"]["port"]
    f.close()
    return port


def get_db_names():
    all_db_infos = conn.admin.command('listDatabases')
    db_name_list = []
    for db_info in all_db_infos['databases']:
        db_name = db_info['name']
        if db_name == 'local' or db_name == 'admin' or db_name == 'config':
            pass
        else:
            db_name_list.append(db_name)
    return db_name_list


def getCollName(database):
    CollnameList = []
    collList = database.command('listCollections')['cursor']['firstBatch']
    for coll in collList:
        collName = coll['name']
        if collName != "system.profile":
            CollnameList.append(collName)
        else:
            continue
    return CollnameList


def get_oplog(conn):
    tmpResult = {}
    db = conn.local
    ol = db['oplog.rs']
    oplogInfo = db2['oplogInfo']
    oplog = "oplog.rs"
    data = db.command('collStats', oplog)

    ol_size_MB = round(data['size'] / 1024 / 1024, 2)
    firstc = ol.find().sort("$natural", pymongo.ASCENDING).limit(1)[0]['ts']
    lastc = ol.find().sort("$natural", pymongo.DESCENDING).limit(1)[0]['ts']
    time_in_oplog = (lastc.as_datetime() - firstc.as_datetime())
    hour_in_oplog = round((time_in_oplog.total_seconds() / 60 / 60), 2)
    utcnow = datetime.utcnow()
    cDate = endTime.strftime('%Y-%m-%d')

    tmpResult['oplog_size_MB'] = ol_size_MB
    tmpResult['first_time'] = firstc.as_datetime()
    tmpResult['last_time'] = lastc.as_datetime()
    tmpResult['hour_in_oplog'] = hour_in_oplog
    tmpResult['now_Time'] = utcnow
    tmpResult['insertDate'] = utcnow
    tmpResult['cDate'] = cDate

    oplogInfo.insert(tmpResult)


def check_replset_state(conn):
    tmpResult = {}
    replSetInfo = db2['replSetInfo']
    utcnow = datetime.utcnow()
    cDate = endTime.strftime('%Y-%m-%d')

    data = db.command('replSetGetStatus')
    heartbeatIntervalMillis = data['heartbeatIntervalMillis']
    members = data['members']
    # message = ""
    allNodes = []
    for member in members:
        message = ""
        their_state = int(member['state'])
        message += " %s : %d (%s)" % (member['name'], their_state, state_text(their_state))
        allNodes.append(message)

    tmpResult['heartbeatIntervalMillis'] = heartbeatIntervalMillis
    tmpResult['members'] = allNodes
    tmpResult['insertDate'] = utcnow
    tmpResult['cDate'] = cDate

    replSetInfo.insert(tmpResult)


def state_text(state):
    if state == 10:
        return "Removed"
    elif state == 9:
        return "Rollback"
    elif state == 8:
        return "Down"
    elif state == 7:
        return "Arbiter"
    elif state == 6:
        return "UNKOWN"
    elif state == 5:
        return "Startup2"
    elif state == 3:
        return "Recovering"
    elif state == 2:
        return "Secondary"
    elif state == 1:
        return "Primary"
    elif state == 0:
        return "Startup"
    else:
        return "Unkown State"


def check_all_database_size(conn):
    all_dbs_data = conn.admin.command('listDatabases')
    dbSizes = db2['dbSizes']
    tmpResult = {}
    dbInfo = []
    utcnow = datetime.utcnow()
    cDate = endTime.strftime('%Y-%m-%d')
    total_storage_size = 0
    total_data_size = 0
    total_index_size = 0
    total_documents = 0
    for db in all_dbs_data['databases']:
        dbname = db['name']
        if dbname == 'admin' or dbname == 'local':
            continue
        else:
            tmpdata = {}
            data = conn[dbname].command('dbstats')
            storage_size = round(data['storageSize'] / 1024 / 1024, 1)
            data_size = round(data['dataSize'] / 1024 / 1024, 1)
            index_size = round(data['indexSize'] / 1024 / 1024, 1)
            documents_count = data['objects']

            tmpdata['dbname'] = data['db']
            tmpdata['storageSize_MB'] = storage_size
            tmpdata['dataSize_MB'] = data_size
            tmpdata['indexSize_MB'] = index_size
            tmpdata['indexes'] = data['indexes']
            tmpdata['collections'] = data['collections']
            tmpdata['documents'] = documents_count

            dbInfo.append(tmpdata)
            total_storage_size += storage_size
            total_data_size += data_size
            total_index_size += index_size
            total_documents += documents_count

    tmpResult['cDate'] = cDate
    tmpResult['total_storage_size_MB'] = round(total_storage_size, 2)
    tmpResult['total_data_size_MB'] = round(total_data_size, 2)
    tmpResult['total_index_size_MB'] = round(total_index_size, 2)
    tmpResult['total_documents'] = total_documents
    tmpResult['dbInfo'] = dbInfo
    tmpResult['insertDate'] = utcnow
    dbSizes.insert(tmpResult)


def check_profile(conn, startTime, endTime):
    all_dbs_data = conn.admin.command('listDatabases')
    tmpResult = {}
    utcnow = datetime.utcnow()
    cDate = endTime.strftime('%Y-%m-%d')
    getProfileInfo = db2['getProfileInfo']
    for db in all_dbs_data['databases']:
        dbname = db['name']
        if dbname == 'admin' or dbname == 'local':
            continue
        else:
            dbTemp = conn[dbname]
            sp = dbTemp['system.profile']
            data = sp.find({"ts": {"$gte": startTime, "lte": endTime}}).count()
            tmpResult[dbname] = data
    tmpResult['insertDate'] = utcnow
    tmpResult['cDate'] = cDate
    getProfileInfo.insert(tmpResult)

class CONFIG():
    def __init__(self, config_file):
        self.cf = configparser.ConfigParser()
        self.cf.read(config_file)

    def _getConf(self):
        _list = self.cf["dbconf"]["confile"].split(",")
        return _list

    def _getServers(self):
        _servers = self.cf["server"]["list"].split(",")
        return _servers

    def _getUserPasswd(self):
        user = self.cf["DEFAULT"]["user"]
        password = self.cf["DEFAULT"]["password"]
        return user,password

    def _getEmail(self):
        from_addr = self.cf["email"]["from_addr"]
        to_addr = self.cf["email"]["to_addr"]
        return from_addr,to_addr


class Space(object):

    def __init__(self, path):
        self.path = path

    def get_free_space_MB(self):
        st = os.statvfs(self.path)
        fr_size = st.f_bavail * st.f_frsize
        return fr_size / 1024 / 1024

    def get_used_space_MB(self):
        st = os.statvfs(self.path)
        used_size = (st.f_blocks - st.f_bfree) * st.f_frsize
        return used_size / 1024 / 1024

    def get_total_space_MB(self):
        st = os.statvfs(self.path)
        total_size = st.f_frsize * st.f_blocks
        return total_size / 1024 / 1024

    def get_space_percent(self):
        used = self.get_used_space_MB()
        total = self.get_total_space_MB()
        used_perct = round((float(used) / total) * 100)
        return used_perct


def get_space(path):
    space = Space(path)
    space_records = db2['space_records']
    cDate = endTime.strftime('%Y-%m-%d')
    tmp = {}
    tmp['path_name'] = path
    tmp['free_space_MB'] = space.get_free_space_MB()
    tmp['used_space_MB'] = space.get_used_space_MB()
    tmp['total_space_MB'] = space.get_total_space_MB()
    tmp['disk_taken_percent'] = space.get_space_percent()
    tmp['cDate'] = cDate
    tmp['insertDate'] = utcnowTime
    space_records.insert(tmp)


def get_all_coll_info():
    db_name_list = get_db_names()
    tmp_list = []
    cdate = endTime.strftime('%Y-%m-%d')
    for db_name in db_name_list:
        database = conn[db_name]
        coll_name_list = getCollName(database)
        for coll_name in coll_name_list:
            document = {}
            coll_info = database.command('collStats', coll_name)

            document['db_name'] = db_name
            document['coll_name'] = coll_name
            document['size'] = coll_info['size']
            document['count'] = coll_info['count']
            document['storageSize'] = coll_info['storageSize']
            document['avgObjSize'] = coll_info.get('avgObjSize', 0)
            document['nindexes'] = coll_info['nindexes']
            document['totalIndexSize'] = coll_info['totalIndexSize']
            uri_str = coll_info['wiredTiger']['uri']
            document['data_file'] = uri_str[uri_str.index("table") + 6:]  # 截取datafile路径，使用string操作方法
            document['createDate'] = utcnowTime
            document['cdate'] = cdate
            tmp_list.append(document)
    check_data = list(db2['collections_info'].find({"cdate": cdate}))
    if len(check_data) != 0:
        db2['collections_info'].remove({"cdate": cdate})
    else:
        pass
    if len(tmp_list) > 0:
        db2['collections_info'].insert_many(tmp_list)


def get_SlowQuery(startTime, endTime):
    db_name_list = get_db_names()
    cdate = datetime.now()
    tmp_list = []
    for db_name in db_name_list:
        db_name_conn = conn[db_name]
        coll_slow_query = list(db_name_conn['system.profile'].find({"ts": {"$gt": startTime, "$lt": endTime}},
                                                                   {"op": 1, "ns": 1, "millis": 1, "ts": 1, "client": 1,
                                                                    "planSummary": 1, "nreturned": 1, "user": 1}).sort(
            "millis", pymongo.DESCENDING).limit(10))
        # print(coll_slow_query)
        for col_row in coll_slow_query:
            print(col_row["op"])
            document = {}
            document["db_name"] = db_name
            document["op"] = col_row["op"]
            document["ns"] = col_row["ns"]
            document["millis"] = col_row["millis"]
            document["ts"] = col_row["ts"]
            document["client"] = col_row["client"]
            document["planSummary"] = col_row["planSummary"]
            # document["nreturned"] = col_row["nreturned"]
            document["user"] = col_row["user"]
            document["cdate"] = cdate
            tmp_list.append(document)
    # print(coll_slow_query)
    # print("start")
    if len(tmp_list) > 0:
        db2['slow_query'].insert_many(tmp_list)


if __name__ == '__main__':

    utcnowTime = datetime.utcnow()
    today = datetime.today()
    yestday = today - timedelta(days=1)
    bfryestday = today - timedelta(days=2)
    startTime = datetime(bfryestday.year, bfryestday.month, bfryestday.day, 16, 0, 0)
    endTime = datetime(yestday.year, yestday.month, yestday.day, 16, 0, 0)

    conf = CONFIG("conf/server.conf")
    _list = conf._getConf()

    host = conf._getServers()

    uri = "mongodb://" + getServerValue("username") + ":Zabbix!2341@" + getServerValue(
        "hostname_1") + ":" + getServerValue(
        "port") + "," + getServerValue("hostname_2") + ":" + getServerValue("port") + "/" + getServerValue("authDB")

    uri = "mongodb://" +


    try:
        conn = MongoClient(uri)
        db = conn['admin']
        db2 = conn['DB_MONITOR']
        serverInfo = db2['serverInfo']
        dbInfo = db2['dbStats']
        get_oplog(conn)
        check_replset_state(conn)
        check_all_database_size(conn)
        check_profile(conn, startTime, endTime)
        get_all_coll_info()
        get_SlowQuery(startTime, endTime)


        for i in _list:
            dir = getBaseDir(i)
            get_space(dir)

    except Exception as e:
        print(e)





