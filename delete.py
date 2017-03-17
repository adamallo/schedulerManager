from __future__ import print_function
import sys, os, re, imp, argparse,getpass
from pysqlite2 import dbapi2 as sqlite

from signal import signal, SIGPIPE, SIG_DFL
signal(SIGPIPE, SIG_DFL) ##To avoid piping problems with head and other programs that stop the pipe

#Configuration variables
confvars=imp.load_source("config", os.path.dirname(os.path.abspath(__file__))+"/config.txt")
defaultPartition=confvars.defaultPartition
dbFile=confvars.dbFile

def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)


##Argument parsing
parser = argparse.ArgumentParser(description="Deletes either a specified job or a list of jobs read from stdin")
parser.add_argument("-j",type=int,default=-1,required=False,help="Job id")
parser.add_argument("-u",type=str,default=getpass.getuser(),required=False,help="User")
args = parser.parse_args()

##DB connection
try:
    db=sqlite.connect(dbFile)
except:
    eprint("Error connecting to the database %d" % (dbFile))
    raise
            
curdb=db.cursor()

curdb.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?",("pendingJobs",))
if curdb.fetchone()==None:
    raise ValueError("The database does not contain the pendingJobs table")
                
#curdb.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?",("submittedJobs",))
#if curdb.fetchone()==None:
#    raise ValueError("The database does not contain the submittedJobs table")
##

if not args.j==-1:
    ##Delete job
    curdb.execute("DELETE FROM pendingJobs WHERE id=?",(args.j,))
    ndeleted=curdb.rowcount
    db.commit()
    if not ndeleted==1:
        eprint("Job %d has not been deleted properly" % (args.j))
        sys.exit(1)
    else:
        print("Job %d has been deleted" % (args.j))
else:
    while 1:
        line = sys.stdin.readline()
        if not line:
            break
        else:
            ##Delete stdin
            job=int(line)
            curdb.execute("DELETE FROM pendingJobs WHERE id=?",(job,))
            ndeleted=curdb.rowcount
            db.commit()
            if not ndeleted==1:
                eprint("Job %d has not been deleted properly" % (job))
            else:
                print("Job %d has been deleted" % (job))

sys.exit(0)
