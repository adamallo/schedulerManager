import sys, time, logging, os, subprocess, re, imp
from pysqlite2 import dbapi2 as sqlite
from daemon import Daemon

##Configuration varibles
confvars=imp.load_source("config", os.path.dirname(os.path.abspath(__file__))+"/config.txt")
dbFile=confvars.dbFile
logFile=confvars.logFile
loggingLevel=confvars.loggingLevel
pidFile=confvars.pidFile
wTime=confvars.wTime
 
class MyDaemon(Daemon):
        db=None
        wait=False
        name=""
        regexp=re.compile("^Submitted batch job (\d*)\n")
        trials=dict()
        getjobs=""
        
        def __init__(self,pidfile=None,credentials=None):
            super( MyDaemon , self ).__init__(pidfile)
            self.name=credentials
            self.getjobs="qstat -u "+credentials+" | tail -n +6 | wc -l"

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
                db=sqlite.connect(dbFile)
                self.db=db
            except:
                logging.error("Error with the database connection")
                raise
            
            curdb=db.cursor()
            curdb.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?",("pendingJobs",))
            if not curdb.fetchone()==None:
                logging.info("The database had been already initialized")
            else:
                logging.info("Creating databases")
                try:
                    curdb.execute("CREATE TABLE pendingJobs (id INTEGER PRIMARY KEY AUTOINCREMENT, command text, partition text, priority INTEGER, time TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL)")
                    db.commit()
                except:
                    logging.error("Error creating the table pendingJobs in the mysqlite database");
                    raise
                try:
                    curdb.execute("CREATE TABLE submittedJobs (id INTEGER PRIMARY KEY, jobid INTEGER, command text, partition text, rtime TIMESTAMP, stime TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL)")
                    db.commit()
                except:
                    logging.error("Error creating the table submittedJobs in the mysqlite database");
                    raise
                logging.info("Done")
            
            failed=0
            submitted=-1 ##Placeholder for first iteration
            dataversion=-1 ##Placeholder for the first interation
            while True: ##Infinite loop
                newsubmitted=int(subprocess.Popen(self.getjobs,stdout=subprocess.PIPE,shell=True).communicate()[0])
                curdb.execute("PRAGMA data_version")
                newdataversion=int(curdb.fetchone()[0])
                if newdataversion!=dataversion or (newsubmitted != submitted and failed==1): ##If the database has been changed or if it does not but some jobs have failed and the number of queued jobs has changed
                    failed=0
                    logging.debug("Submitting jobs")
                    curdb.execute("SELECT id, command, partition, priority, time FROM pendingJobs ORDER BY priority DESC, time ASC");
                    jobs=curdb.fetchall();
                    for job in jobs: ##TODO add any kind of check for jobs that fail constantly and do something with them
                        logging.debug("Submitting job %s" % job[0])
                        sbatchObj=subprocess.Popen(['sbatch','-p',job[2]]+job[1].split(" "),stdout=subprocess.PIPE,stderr=subprocess.PIPE)
                        sbatchOut,sbatchErr=sbatchObj.communicate()
                        sbatchErrCode=abs(sbatchObj.returncode)
                        if sbatchErrCode == 0:
                            #sucess
                            logging.info("Job  %s was submitted properly" % job[0])
                            jobid=re.sub(self.regexp,"\g<1>",sbatchOut)
                            try:
                                curdb.execute("DELETE FROM pendingJobs WHERE id=?",(job[0],))
                                curdb.execute("INSERT INTO submittedJobs (id,jobid,command,partition,rtime) VALUES (?,?,?,?,?)",(job[0],int(jobid),job[1],job[2],job[4]))
                                db.commit()
                            except:
                                logging.error("Error changing job from pending to submited")
                                raise
                        else:
                            #Error, skip this entry and add it to a dictionary
                            logging.warning("Job %s submission failed, stdout %s, stderr %s, error %d" % (job[0],sbatchOut,sbatchErr,sbatchErrCode))
                            if job[0] in self.trials:
                                self.trials[job[0]]=self.trials[job[0]]+1
                            else:
                                self.trials[job[0]]=1
                            logging.debug("%d failed submissions of the job %s" % (self.trials[job[0]],job[0]))
                            failed=1
 
                    submitted=int(subprocess.Popen(self.getjobs,stdout=subprocess.PIPE,shell=True).communicate()[0])
                    curdb.execute("PRAGMA data_version")
                    dataversion=int(curdb.fetchone()[0])
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
