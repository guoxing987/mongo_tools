# -*- coding: utf-8 -*-
# @FileName: t2.py.py
# @Time    : 2019/11/12 14:52
# @Author  : Damon Guo
# @version : 

# -*- coding: utf-8 -*-
# @FileName: t2.py
# @Time    : 2019/11/10 10:40
# @Author  : Damon Guo
# @version :

from pymongo import MongoClient
import pymongo
from datetime import datetime, timedelta
import os, sys
import configparser
import yaml


def getServerValue(config_file):
    # config_file = "./conf/server.conf"
    cf = configparser.ConfigParser()
    cf.read(config_file)
    _list = cf["dbconf"]["confile"].split(',')
    return _list


def getBaseDir(conf):
    f = open(conf)
    y = yaml.load(f, Loader=yaml.FullLoader)
    dbPath = y["storage"]["dbPath"]
    basedir = os.path.join("/", dbPath.split("/")[1])
    f.close()
    return basedir

def getPort(conf):
    f = open(conf)
    y = yaml.load(f, Loader=yaml.FullLoader)
    port = y["net"]["port"]
    f.close()
    return port


class CONFIG():
    def __init__(self, config_file):
        self.cf = configparser.ConfigParser()
        self.cf.read(config_file)

    def getConf(self):
        _list = self.cf["dbconf"]["confile"].split(",")
        return _list

    def getServers(self):
        _servers = self.cf["server"]["list"]
        # _servers = self.cf["server"]["list"].split(",")
        return _servers

    def getUserPasswd(self):
        user = self.cf["DEFAULT"]["user"]
        password = self.cf["DEFAULT"]["password"]
        return user,password

    def getEmail(self):
        from_addr = self.cf["email"]["from_addr"]
        to_addr = self.cf["email"]["to_addr"]
        return from_addr,to_addr



if __name__ == '__main__':
    # _list = getServerValue("conf/server.conf")
    conf = CONFIG("conf/server.conf")
    _list = conf.getConf()
    server = conf.getServers()
    print(server)
    user , password = conf.getUserPasswd()
    print(user)
    print(password)

    for i in _list:
        t = getBaseDir(i)
        print(t)
        port = getPort(i)
        print(port)