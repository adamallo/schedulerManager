from __future__ import print_function
import sys, os, re, imp
from pysqlite2 import dbapi2 as sqlite
from job import job, pendingjob, submittedjob, stoppedjob
from signal import signal, SIGPIPE, SIG_DFL
signal(SIGPIPE, SIG_DFL) ##To avoid piping problems with head and other programs that stop the pipe

#Configuration variables
confvars=imp.load_source("config", os.path.dirname(os.path.abspath(__file__))+"/config.txt")
defaultPartition=confvars.defaultPartition
dbFile=confvars.dbFile
defPrior=confvars.defaultPrior

def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)

usage="Usage: %s sbatch_arguments\n\n--prior: priority for the schedule wrapper\n--dependencies= dependencies, :separated and with slumr code first (e.g., afterok)" % sys.argv[0]

##Argument parsing
iargv=0
partition=defaultPartition
dependency=""
arguments=list()

if len(sys.argv) <= 1:
    print(usage)
    sys.exit(1)

reopt=re.compile("^-")
repart=re.compile("--partition.")
redep=re.compile("^--dependency=(.*)")

fprior=defPrior

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
        elif arg == "--prior":
            fprior=sys.argv[iargv+1]
            iargv=iargv+1
        elif re.match(redep,arg):
            dependency=re.match(redep,arg).group(1)
        elif re.match(repart,arg) is not None : ##--partition=part
            partition=re.sub(repart,"",arg)
        else:
            arguments.append(arg) ##Other non-parsed options things
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
dir=os.getcwd()
curdb.execute("INSERT INTO pendingJobs (command,dir,partition,priority,dependency_id,attempts,depattempts) VALUES (?,?,?,?,?,?,?)", (sep.join(arguments),dir,partition,fprior,dependency,0,0))
ownId=curdb.lastrowid
db.commit()
print("Queued job %d" % (ownId,))
sys.exit(0)
