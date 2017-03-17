from __future__ import print_function
import sys, os, re, imp, argparse, getpass
from pysqlite2 import dbapi2 as sqlite

from signal import signal, SIGPIPE, SIG_DFL
signal(SIGPIPE, SIG_DFL) ##To avoid piping problems with head and other programs that stop the pipe

#Configuration variables
confvars=imp.load_source("config", os.path.dirname(os.path.abspath(__file__))+"/config.txt")
defaultPartition=confvars.defaultPartition
dbFile=confvars.dbFile
defPrior=confvars.defaultPrior

def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)

usage="Usage: %s sbatch_arguments" % sys.argv[0]

##Argument parsing
parser = argparse.ArgumentParser(description="Edit specified job or list of jobs given by stdin")
parser.add_argument("-j",type=int,default=-1,required=False,help="Job id")
parser.add_argument("-p",type=str,required=False,help="Partition")
parser.add_argument("--prior",type=int,default=defPrior,required=False,help="Priority")
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

commandvars=list()

execommand="UPDATE pendingJobs SET"
execommandtail=" WHERE id=?"
needed=False

if not args.p is None:
    execommand=execommand+" partition=?"
    commandvars.append(args.p)
    needed=True
if not args.prior is None:
    execommand=execommand+" priority=?"
    commandvars.append(args.prior)
    needed=True

commandvars.append(args.j)

if needed==True:
    if not args.j==-1:
        curdb.execute(execommand+execommandtail,commandvars)
        db.commit()
        ##This is not checking if the update actually happened. I should improve this
        print("Edited job %d" % commandvars[-1])
    else:
        while 1:
            line = sys.stdin.readline()
            if not line:
                break
            else:
                commandvars[-1] = int(line)
                curdb.execute(execommand+execommandtail,commandvars)
                db.commit()
                ##This is not checking if the update actually happened. I should improve this
                print("Edited job %d" % commandvars[-1])
else:
    eprint("No edition has been carried out")

sys.exit(0)
