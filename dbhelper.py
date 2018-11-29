from pysqlite2 import dbapi2 as sqlite
import logging
from sharedconfig import *

class dbhelper():

    def __init__(self,dbFile,credentials,cursor=None):
        self.db=sqlite.connect(dbFile,timeout=dbTimeout)
        self.name=credentials
        if cursor is None:
            self.cursor=self.db.cursor()

    def execute(self,command,params=None):
        if params is None:
            self.cursor.execute(command)
        else:
            self.cursor.execute(command,params)
    def execom(self,command,params=None):
        self.execute(command,params)
        self.db.commit()
    def exefetchall(self,command,params=None):
        self.execute(command,params)
        return self.cursor.fetchall()
    def fetchone(self):
        return self.cursor.fetchone()
    def commit(self):
        self.db.commit()
    def close(self):
        self.cursor.close()
