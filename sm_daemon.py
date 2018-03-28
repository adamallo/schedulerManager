import sys, time, logging, os, subprocess, re, imp
from dbhelper import dbhelper
from daemon import Daemon

##Configuration varibles
confvars=imp.load_source("config", os.path.dirname(os.path.abspath(__file__))+"/config.txt")
dbFile=confvars.dbFile
logFile=confvars.logFile
loggingLevel=confvars.loggingLevel
pidFile=confvars.pidFile
wTime=confvars.wTime
maxAttempts=confvars.maxAttempts
maxDepAttempts=confvars.maxDepAttempts

##TODO: Still a good number of hardcoded things

class job (object):
    sqlitecolumns=(("id","INTEGER PRIMARY KEY AUTOINCREMENT"), ("command", "text"), ("partition", "text"), ("priority", "INTEGER"), ("dependency_id", "text"), ("attempts", "INTEGER"), ("depattempts", "INTEGER"), ("rtime", "TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL"))

    def __init__(self,string="",sep=","):
        self.elements={ self.sqlitecolumns[i][2]:i for i in xrange(0,len(self.sqlitecolumns)) }
        self.content=string.split(sep)

    def get(self,listid):
        retlist=[]
        if isinstance(listid,basestring):
            listid=(listid,)
        for i in listid:
            retlist.append(self.content[self.elements[i]])
        return retlist

    def gendepjid(self,db):
        depids=self.get("dependency_id").split(",")
        depjids=[]
        try:
            dicids={ element[0]:element[1] for element in db.exefetchall("SELECT id,jobid FROM submittedJobs")}
        except:
            logging.error("Error generating the list of ids and jids")
            raise

        for id in depids:
            if id in depids:
                depjids.append(depids[id])
            else:
                logging.debug(" ".join("The dependency ",id," has not been fulfilled yet for job ",self.get("id")))
                return False

        return ":".join(depjids)

    def getsqlite(self):
        return ", ".join([ " ".join((tup[0],tup[1])) for tup in self.sqlitecolumns ])

    def getsqliteinsert(self):
        nondefkeys=[ self.sqlitecolumns[i][0] for i in xrange(0,len(self.sqlitecolumns)) if re.match(".*DEFAULT.*",cls.sqlitecolumns[i][1]) is None ]
        nondefvalues=[ self.content[self.elements[key]] for key in nondefkeys ]

        stringkey="("+",".join(nondefkeys)+") VALUES("+",".join([ "?" for i in nondefvalues ])+")"
        return [stringkey,nondefvalues]

    def mutatejob(self,cls,**kwargs):
        dictsub={ cls.sqlitecolumns[i][0]:i for i in xrange(0,len(cls.sqlitecolumns)) if re.match(".*DEFAULT.*",cls.sqlitecolumns[i][1]) is None } ##Default sqlite values are not necessary
        dictdata={ self.sqlitecolumns[i][0]:self.content[i] for i in xrange(0,len(self.sqlitecolumns)) }
        for key,value in kwargs.iteritems():
            dictdata[key]=value

        flist=[]
        for key in dictsub:
            if key in dictdata:
                flist[dictsub[key]]=dictdata[key]
            else:
                loggin.error(" ".join(("Impossible to generate the job of the new class",cls.__name__," from current class",self.__class__.__name__,", missing variable",key)))
                raise ValueError()

        fstring=",".join(flist)

        return cls(fstring)

class pendingjob(job):
    def stop(self, db):
        try:
            db.execute("DELETE FROM pendingJobs WHERE id=?",self.get("id"))
            stopjob=self.mutatejob(stoppedjob)
            (string,values)=self.getsqliteinsert()
            db.execom("INSERT INTO stoppedJobs "+string,values)
            logging.warning(" ".join(("Job ",self.get("id"),"moved from pending to stopped with ",self.get("attempts")," attempts and ",self.get("depattempts"),"depattempts")))
        except:
            logging.error(" ".join(("Error moving job ",self.get("id"),"from pending to stopped with ",self.get("attempts")," attempts and ",self.get("depattempts"),"depattempts")))
            raise

    def submit(self,db,dependency_jid=None):
        if dependency_jid is not None or self.get("dependency_id") != "":
            if dependency_jid is not None:
                deps=dependency_jid
            else: #there was a dependency but it isn't specified
                deps=self.gendepjid(db)
            depcommand=" "+"".join(("--dependency=afterok:",deps)) ##Formatted for slurm
        else:
            depcommand=""
            deps=""

        myid=self.get("id")
        logging.debug("Submitting job %s" % myid)
        logging.warning(" ".join(['. ./home/'+db.name+"/.bashrc;",'sbatch','-p',self.get("partition")+depcommand]+self.get("command").split(" ")))
        sbatchObj=subprocess.Popen(" ".join(['. ./home/'+db.name+"/.bashrc;",'sbatch','-p',self.get("partition")+depcommand]+self.get("command").split(" ")),stdout=subprocess.PIPE,stderr=subprocess.PIPE,shell=True,executable="/bin/bash")
        sbatchOut,sbatchErr=sbatchObj.communicate()
        logging.debug("Submitted, stdout:"+sbatchOut+"; stderr:"+sbatchErr)
        sbatchErrCode=abs(sbatchObj.returncode)

        if sbatchErrCode == 0:
            #sucess
            logging.debug("Job  %s was submitted properly" % myid)
            jobid=re.sub(self.regexp,"\g<1>",sbatchOut)
            retjob=self.mutatejob(submittedjob,jobid=jobid,dependency_fid=deps)
            try:
                db.execute("DELETE FROM pendingJobs WHERE id=?",(myid,))
                subjob=self.mutatejob(submittedjob)
                (string,values)=self.getsqliteinsert()
                db.execom("INSERT INTO submittedJobs "+string,values)
            except:
                logging.error("Error changing job from pending to submited")
                raise
        else:
            logging.debug("Job %s submission failed, stdout %s, stderr %s, error %d" % (myid,sbatchOut,sbatchErr,sbatchErrCode))
            try:
                db.execom("UPDATE pendingJobs SET attempts = attempts + 1 WHERE id=?",myid)
            except:
                loggin.error(" ".join(("Error updating the number of attempts of job ",myid)))
                raise
#            if myid in self.attempts:
#                self.attempts[myid]=self.attempts[myid]+1
#            else:
#                self.attempts[myid]=1
            logging.debug("%d failed submissions of the job %s" % (self.attempts[myid],myid))

    def adddepattempt(self,db):
        try:
            db.execom("UPDATE pendingJobs SET depattempts = depattempts + 1 WHERE id=?",self.get("id"))
        except:
            loggin.error(" ".join(("Error updating the number of depattempts of job ",self.get("id"))))
            raise

class submittedjob(job):
    sqlitecolumns=(("id","INTEGER PRIMARY KEY"), ("jobid", "INTEGER"), ("command", "text"), ("partition", "text"), ("priority", "INTEGER"), ("dependency_id", "text"), ("dependency_fid", "text"), ("attempts", "INTEGER"), ("depattempts", "INTEGER"), ("rtime", "TIMESTAMP"), ("stime", "TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL"))

class stoppedjob(job):
            sqlitecolumns=(("id","INTEGER PRIMARY KEY"), ("jobid", "INTEGER"), ("command", "text"), ("partition", "text"), ("priority", "INTEGER"), ("dependency_id", "text"), ("dependency_fid", "text"), ("attempts", "INTEGER"), ("depattempts", "INTEGER"), ("rtime", "TIMESTAMP"), ("stime","TIMESTAMP"), ("etime","TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL"))

class MyDaemon(Daemon):
        wait=False
        regexp=re.compile("^Submitted batch job (\d*)\n")
        def __init__(self,pidfile=None,credentials=None):
            super( MyDaemon , self ).__init__(pidfile)
            self.name=credentials
            self.getjobs="qstat -u "+credentials+" | tail -n +6 | wc -l"
            #self.attempts=[]

        def run(self):#Override of the run method
            ##The log is reseted
            try:
                os.remove(logFile)
            except OSError:
                pass

            logging.basicConfig(filename=logFile,level=loggingLevel,format='%(asctime)s %(message)s') ##Change debug to info for production
            logging.info("Daemon starting")

            ##DB connection and initialization if necessary
            try:
                db=dbhelper(dbFile,self.name)
                self.db=db
            except:
                logging.error("Error with the database connection")
                raise

            db.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?","pendingJobs")
            if not db.fetchone()==None:
                logging.info("The database had been already initialized")
            else:
                logging.info("Creating databases")
                try:
                    db.execom(" ".join(("CREATE TABLE pendingJobs (",pendingjob.getsqlite(),")")))
                except:
                    logging.error("Error creating the table pendingJobs in the mysqlite database")
                    raise
                try:
                    db.execom(" ".join(("CREATE TABLE submittedJobs (",submittedjob.getsqlite(),")")))
                except:
                    logging.error("Error creating the table submittedJobs in the mysqlite database")
                    raise
                try:
                    db.execom(" ".join(("CREATE TABLE stoppedJobs (",stoppedjob.getsqlite(),")")))
                except:
                    logging.error("Error creating the table stoppedJobs in the mysqlite database")
                    raise
                logging.info("Done")

            submitted=-1
            dataversion=-1
            while True:
                newsubmitted=int(subprocess.Popen(self.getjobs,stdout=subprocess.PIPE,shell=True).communicate()[0])
                db.execute("PRAGMA data_version")
                newdataversion=int(curdb.fetchone()[0])
                if newdataversion!=dataversion or (newsubmitted != submitted and failed==1): ##If the database has been changed or if it does not but some jobs have failed and the number of queued jobs has changed
                    failed=0
                    logging.debug("Submitting jobs")
                    jobs=db.exefetchall("SELECT * FROM pendingJobs ORDER BY priority DESC, time ASC")

                    for job in jobs:
                        job=pendingJob(job)
                        if job.get("attempts") >= maxAttempts:
                            job.stop(db)
                        elif job.get("dependency_id") != "":
                            if job.get("depattempts") >= maxDepAttempts:
                                job.stop(db)
                            else:
                                dependency_jid=job.gendepjid(db)
                                if not dependency_jid:
                                    job.adddepattempt(db)
                                else:
                                    job.submit(db,dependency_jid)
                        else:
                            job.submit(db)

                    submitted=int(subprocess.Popen(self.getjobs,stdout=subprocess.PIPE,shell=True).communicate()[0])
                    db.execute("PRAGMA data_version")
                    dataversion=int(db.fetchone()[0])
                else:
                    logging.debug("Just waiting, there is no reason to try to submit jobs, newsubmitted %d, submitted %d, failed %d, newdataversion %d, dataversion %d" %(newsubmitted,submitted,failed,newdataversion,dataversion))
                time.sleep(wTime)
            self.stop()

if __name__ == "__main__":
        if len(sys.argv) == 3:

                FNULL = open(os.devnull, 'w')
                stderr=subprocess.Popen(["squeue","-u",sys.argv[1]],stderr=subprocess.PIPE,stdout=FNULL).communicate()
                if stderr[1]=="":
                    daemon = MyDaemon(pidfile=pidFile,credentials=sys.argv[1])
                    if 'start' == sys.argv[2]:
                        daemon.start()
                    elif 'stop' == sys.argv[2]:
                        daemon.stop()
                    elif 'restart' == sys.argv[2]:
                        daemon.restart()
                    elif 'run' == sys.argv[2]:
                        daemon.run()
                    else:
                        print "Unknown command"
                        sys.exit(2)
                else:
                    print "The username is incorrect"
                    sys.exit(2)
                sys.exit(0)
        else:
                print "usage: %s slurm_username start|stop|restart|run" % sys.argv[0]
                sys.exit(2)
