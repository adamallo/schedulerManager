from __future__ import print_function
import sys, os, re, getpass, subprocess
from pysqlite2 import dbapi2 as sqlite
import argparse
import imp

#Configuration variables
confvars=imp.load_source("config", os.path.dirname(os.path.abspath(__file__))+"/config.txt")
defaultPartition=confvars.defaultPartition
dbFile=confvars.dbFile

def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)

##Argument parsing
parser = argparse.ArgumentParser(description="Shows the status of submitted jobs")
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
                
curdb.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?",("submittedJobs",))
if curdb.fetchone()==None:
    raise ValueError("The database does not contain the submittedJobs table")
##

if not args.j ==-1:
    ##This job
    curdb.execute("SELECT * FROM pendingJobs WHERE id=?",(args.j,))
    job=curdb.fetchone()
    if job is not None:
        print("The job %d is waiting to be submitted to the partition %s" % (job[0],job[2]))
        sys.exit(0)
    else:
        curdb.execute("SELECT * FROM submittedJobs WHERE id=?",(args.j,))
        job=curdb.fetchone()
        if job is not None:
            print("The job %d was submitted to the partition %s with jobid %d" % (job[0],job[3],job[1]))
            squeueObj=subprocess.Popen(['squeue','-u',args.u,'-p',job[3],'-j',str(job[1])],stdout=subprocess.PIPE,stderr=subprocess.PIPE)
            squeueOut,squeueErr=squeueObj.communicate()
            print(squeueOut)
            scontrolObj=subprocess.Popen(['scontrol','show','job',str(job[1])],stdout=subprocess.PIPE,stderr=subprocess.PIPE)
            scontrolOut,scontrolErr=scontrolObj.communicate()
            print(scontrolOut)
            sys.exit(0)
        else:
            eprint("The jobid %d is not valid" % (args.j,))
            sys.exit(1)
else:
    raise NotImplementedError("All jobs option has not been implemented yet")
    ##All jobs 
    ##Print table pendingjobs
    ##Execute squeue -u with all partitions in the table

sys.exit(0)
