from __future__ import print_function
import sys, os, re, imp
from pysqlite2 import dbapi2 as sqlite

#Configuration variables
confvars=imp.load_source("config", os.path.dirname(os.path.abspath(__file__))+"/config.txt")
defaultPartition=confvars.defaultPartition
dbFile=confvars.dbFile

def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)

usage="Usage: %s sbatch_arguments" % sys.argv[0]

##Argument parsing
iargv=0
partition=defaultPartition
arguments=list()

if len(sys.argv) <= 1:
    print(usage)
    sys.exit(1)

reopt=re.compile("^-")
repart=re.compile("--partition.")

iargv=1

while iargv < len(sys.argv):
    arg=sys.argv[iargv]
    if re.match(reopt,arg) is not None : #Options
        if arg == "-h" or arg == "--help":
            print(usage)
            sys.exit(0)
        elif arg == "-p":
            partition=sys.argv[iargv+1]
            iargv=iargv+1
        elif re.match(repart,arg) is not None : ##--partition=part
            partition=re.sub(repart,"",arg)
        else:
            arguments.append(arg) ##Other things
    else:
        if os.path.exists(arg): ##File or directory
            path=os.path.abspath(arg)
            arguments.append(path)
        else:
            arguments.append(arg) ##Other things
    iargv=iargv+1

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

sep=" "
curdb.execute("INSERT INTO pendingJobs (command,partition) VALUES (?,?)", (sep.join(["-D",os.getcwd()]+arguments),partition))
ownId=curdb.lastrowid
db.commit()
print("Queued job %d" % (ownId,))
sys.exit(0)
