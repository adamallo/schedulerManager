import sys, time, logging, os, subprocess
from dbhelper import dbhelper
from daemon import Daemon
from job import job, pendingjob, submittedjob, stoppedjob
from sharedconfig import *

##TODO: Still a good number of hardcoded things
class MyDaemon(Daemon):
        def __init__(self,pidfile=None,credentials=None):
            super( MyDaemon , self ).__init__(pidfile)
            self.name=credentials
            self.getjobs="qstat -u "+credentials+" -i | tail -n +6 | wc -l"
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

            db.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?",("pendingJobs",))
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
                newdataversion=int(db.fetchone()[0])
                if newdataversion!=dataversion or (newsubmitted != submitted and failed==1): ##If the database has been changed or if it does not but some jobs have failed and the number of queued jobs has changed
                    failed=0
                    logging.debug("Submitting jobs")
                    jobs=db.exefetchall("SELECT * FROM pendingJobs ORDER BY priority DESC, rtime ASC")

                    for job in jobs:
                        job=pendingjob(job)
                        if job.get("attempts") >= maxAttempts:
                            job.stop(db)
                        elif job.get("dependency_id") != "":
                            if job.get("depattempts") >= maxDepAttempts:
                                job.stop(db)
                            else:
                                dependency_jid=job.gendepjid(db)
                                if not dependency_jid:
                                    job.adddepattempt(db)
                                    failed=1
                                else:
                                    failed=failed|job.submit(db,dependency_jid)
                        else:
                            failed=failed|job.submit(db)

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
