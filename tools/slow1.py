# -*- coding: utf-8 -*-
# @FileName: analysis.py
# @Time    : 2019/11/26 13:59
# @Author  : Damon Guo
# @version : 


import sys, subprocess, os
import paramiko
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor


class SSH():
    def __init__(self, hostname, username, port=22, password=None, pkey=None):
        self.hostname = hostname
        self.port = port
        self.username = username
        self.password = password
        self.pkey = pkey

        # 判断登录方式，password or key
        if self.password is None and self.pkey is None:
            print("please input the one of password or pkey")
            sys.exit(0)
        elif self.password:
            self.c = paramiko.SSHClient()
            self.c.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            self.c.connect(self.hostname, self.port, self.username, self.password, timeout=10)
            self.t = paramiko.Transport(self.hostname, self.port)
            self.t.connect(username=self.username, password=self.password)
        elif self.pkey:
            self.c = paramiko.SSHClient()
            self.c.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            private_key = paramiko.RSAKey.from_private_key_file(self.pkey)
            self.c.connect(self.hostname, self.port, self.username, pkey=private_key, timeout=10)
            self.t = paramiko.Transport(self.hostname, self.port)
            self.t.connect(username=self.username, pkey=private_key)

    def exec_cmd(self, command):
        '''执行shell命令'''
        stdin, stdout, stderr = self.c.exec_command(command)
        # print(stdout.read().decode())
        return stdout.read().decode()

    def get_file(self, remotefile, localfile):
        '''下载文件'''
        getfile = paramiko.SFTPClient.from_transport(self.t)
        getfile.get(remotefile, localfile)
        return

    def put_file(self, localfile, remotefile):
        '''上传文件'''
        putfile = paramiko.SFTPClient.from_transport(self.t)
        putfile.put(localfile, remotefile)
        return

    def close_ssh(self):
        self.t.close()
        self.c.close()


def analysis(logname):
    '''分析慢查询'''
    logpath = os.path.join(BACKPATH, str(ydate))
    slowfile = "slow_" + os.path.split(logname)[1]

    cmd = " script -t -c  'sudo /usr/bin/mlogfilter " + logname + " --slow 200 ' "
    res = subprocess.run(cmd,capture_output=True,shell=True,encoding="utf-8")

    with open(os.path.join(logpath, slowfile), 'a') as f:
        f.write(res.stdout)

def exec(log):
    ssh = SSH("10.26.14.51", "mongo", pkey="/home/mongo/.ssh/id_rsa")
    cmd = "ls " + log + "." + str(ydate) + "*"
    remotefile = ssh.exec_cmd(cmd).strip("\n")
    localfile = os.path.join(localpath, log.split("/")[-1])
    print(localfile)
    ssh.get_file(remotefile, localfile)
    ssh.close_ssh()
    analysis(localfile)


if __name__ == '__main__':
    try:
        BACKPATH = "/backup/logs/"
        ydate = datetime.date(datetime.now() - timedelta(days=1))
        localpath = os.path.join(BACKPATH + str(ydate))
        os.mkdir(localpath)
        t = ThreadPoolExecutor(10)
        log_lists = ["/data/logs/onlineRS1_logs/onlineRS1.log", "/data/logs/rs1_logs/rs1.log", "/data2/logs/capp_logs/capp.log","/data2/logs/notirs1_logs/notirs1.log", "/data3/logs/trackgw_logs/trackgw.log"]
        for log in log_lists:
            t.submit(exec, log)
    except Exception as e:
        print('Error:', e)
