#!/usr/bin/python
# -*- coding: UTF-8 -*-

import logging
import time
import sys
from server_pool import ServerPool
import Config
import json

try:
    import urllib2 as urllib;
except:
    import urllib;

class DbTransfer(object):

    instance = None

    def __init__(self):
        self.last_get_transfer = {}

    @staticmethod
    def get_instance():
        if DbTransfer.instance is None:
            DbTransfer.instance = DbTransfer()
        return DbTransfer.instance

    def push_db_all_user(self):
        #更新用户流量到数据库
        last_transfer = self.last_get_transfer
        curr_transfer = ServerPool.get_instance().get_servers_transfer()
        #上次和本次的增量
        dt_transfer = {}
        for id in curr_transfer.keys():
            if id in last_transfer:
                if last_transfer[id][0] == curr_transfer[id][0] and last_transfer[id][1] == curr_transfer[id][1]:
                    continue
                elif curr_transfer[id][0] == 0 and curr_transfer[id][1] == 0:
                    continue
                elif last_transfer[id][0] <= curr_transfer[id][0] and \
                last_transfer[id][1] <= curr_transfer[id][1]:
                    dt_transfer[id] = [curr_transfer[id][0] - last_transfer[id][0],
                                       curr_transfer[id][1] - last_transfer[id][1]]
                else:
                    dt_transfer[id] = [curr_transfer[id][0], curr_transfer[id][1]]
            else:
                if curr_transfer[id][0] == 0 and curr_transfer[id][1] == 0:
                    continue
                dt_transfer[id] = [curr_transfer[id][0], curr_transfer[id][1]]

        self.last_get_transfer = curr_transfer
        last_time = time.time()
        info = []
        for id in dt_transfer.keys():
            tmp = {
                'u':dt_transfer[id][0],
                'd':dt_transfer[id][1],
                'p':id,
                'time':last_time
            }
            info.append(tmp)
        info = json.dumps(info)
        stream = urllib.urlopen(Config.apiurl,info)
        data = stream.read()

    @staticmethod
    def pull_db_all_user():
        #数据库所有用户信息
        users = []
        try:
            stream = urllib.urlopen(Config.apiurl,timeout=30);
            users = stream.read();
            users = json.loads(users)
        except IOError:
            print 'ERROR: Reading users timeout.'
            sys.exit()
        except:
            print "ERROR: Reading users error unknow"
            sys.exit()

        rows = []
        for r in users:
            r[0] = int(r[0])
            r[1] = int(r[1])
            r[2] = int(r[2])
            r[3] = int(r[3])
            r[4] = str(r[4])
            r[5] = int(r[5])
            r[6] = int(r[6])
            rows.append(r)
        return rows

    @staticmethod
    def del_server_out_of_bound_safe(rows):
    #停止超流量的服务
    #启动没超流量的服务
    #修改下面的逻辑要小心包含跨线程访问
        for row in rows:
            if ServerPool.get_instance().server_is_run(row[0]) is True:
                if row[5] == 0 or row[6] == 0:
                    #stop disable or switch off user
                    logging.info('db stop server at port [%s] reason: disable' % (row[0]))
                    ServerPool.get_instance().del_server(row[0])
                elif row[1] + row[2] >= row[3]:
                    #stop out bandwidth user
                    logging.info('db stop server at port [%s] reason: out bandwidth' % (row[0]))
                    ServerPool.get_instance().del_server(row[0])
                if ServerPool.get_instance().tcp_servers_pool[row[0]]._config['password'] != row[4]:
                    #password changed
                    logging.info('db stop server at port [%s] reason: password changed' % (row[0]))
                    ServerPool.get_instance().del_server(row[0]) 
            else:
                if row[5] == 1 and row[6] == 1 and row[1] + row[2] < row[3]:
                    logging.info('db start server at port [%s] pass [%s]' % (row[0], row[4]))
                    ServerPool.get_instance().new_server(row[0], row[4])

    @staticmethod
    def thread_db():
        import socket
        import time
        timeout = 60
        socket.setdefaulttimeout(timeout)
        while True:
            #logging.warn('db loop')
            try:
                DbTransfer.get_instance().push_db_all_user()
                rows = DbTransfer.get_instance().pull_db_all_user()
                DbTransfer.del_server_out_of_bound_safe(rows)
            except Exception as e:
                logging.warn('db thread except:%s' % e)
            finally:
                time.sleep(15)


#SQLData.pull_db_all_user()
#print DbTransfer.get_instance().test()
