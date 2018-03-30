import logging, subprocess, re
from sharedconfig import *

##TODO: Still a good number of hardcoded things
##TODO: I could implement defaults for every class to reduce hardcoding in the implementation of submit
class job (object):
    sqlitecolumns=(("id","INTEGER PRIMARY KEY AUTOINCREMENT"), ("command", "text"), ("partition", "text"), ("priority", "INTEGER"), ("dependency_id", "text"), ("attempts", "INTEGER"), ("depattempts", "INTEGER"), ("rtime", "TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL"))

    def __init__(self,content):
        self.elements={ self.sqlitecolumns[i][0]:i for i in xrange(0,len(self.sqlitecolumns)) }
        self.content=content

    def get(self,param):
#        retlist=[]
#        if isinstance(listid,basestring):
#            listid=(listid,)
#        for i in listid:
#            retlist.append(self.content[self.elements[i]])
        return self.content[self.elements[param]]

    def gendepjid(self,db):
        if self.get("dependency_id") == "":
            return ""
        try:
            depids=self.get("dependency_id").split(":") #Formatted for slurm
            depjids=[]
            dicids={ element[0]:element[1] for element in db.exefetchall("SELECT id,jobid FROM submittedJobs")}
        except:
            logging.error("Error generating the list of ids and jids")
            raise
        depjids.append(depids.pop(0)) #The first element of the list is a slurm command
        assert depjids[0] in ["after","afterany","afterok","afternotok","singleton"], "%s is not a valid slurm dependency option" % depjids[0] ##
        for id in depids:
            if int(id) in dicids:
                depjids.append(dicids[int(id)])
            else:
                logging.debug(" ".join(("The dependency ",str(id)," has not been fulfilled yet for job ",str(self.get("id")))))
                return False
        return ":".join(str(x) for x in depjids)

    def getsqlite(self):
        return ", ".join([ " ".join((tup[0],tup[1])) for tup in self.sqlitecolumns ])

    def getsqliteinsert(self):
        nondefkeys=[ self.sqlitecolumns[i][0] for i in xrange(0,len(self.sqlitecolumns)) if re.match(".*DEFAULT.*",self.sqlitecolumns[i][1]) is None ]
        nondefvalues=[ self.content[self.elements[key]] for key in nondefkeys ]

        stringkey="("+",".join(nondefkeys)+") VALUES("+",".join([ "?" for i in nondefvalues ])+")"
        return [stringkey,nondefvalues]

    def mutatejob(self,cls,**kwargs):
        dictsub={ cls.sqlitecolumns[i][0]:i for i in xrange(0,len(cls.sqlitecolumns)) if re.match(".*DEFAULT.*",cls.sqlitecolumns[i][1]) is None } ##Default sqlite values are not necessary
        dictdata={ self.sqlitecolumns[i][0]:self.content[i] for i in xrange(0,len(self.sqlitecolumns)) }
        for key,value in kwargs.iteritems():
            dictdata[key]=value

        flist=[None]*len(dictsub)
        for key in dictsub:
            if key in dictdata:
                flist[dictsub[key]]=dictdata[key]
            else:
                logging.error(" ".join(("Impossible to generate the job of the new class",cls.__name__," from current class",self.__class__.__name__,", missing variable",key)))
                raise ValueError()

        return cls(flist)

class pendingjob(job):
    def stop(self, db):
        try:
            db.execute("DELETE FROM pendingJobs WHERE id=?",(self.get("id"),))
            stopjob=self.mutatejob(stoppedjob)
            (string,values)=stopjob.getsqliteinsert()
            db.execom("INSERT INTO stoppedJobs "+string,values)
            logging.warning(" ".join(("Job ",self.get("id"),"moved from pending to stopped with ",self.get("attempts")," attempts and ",self.get("depattempts"),"depattempts")))
        except:
            logging.error(" ".join(("Error moving job ",self.get("id"),"from pending to stopped with ",self.get("attempts")," attempts and ",self.get("depattempts"),"depattempts")))
            raise

    def submit(self,db,dependency_jid=""):
        if dependency_jid is not "" or self.get("dependency_id") != "":
            if dependency_jid is not "":
                deps=dependency_jid
            else: #there was a dependency but it isn't specified
                deps=self.gendepjid(db)
            depcommand=" "+"".join(("--dependency=",deps)) ##Formatted for slurm
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
            jobid=re.sub(regexp,"\g<1>",sbatchOut)
            try:
                db.execute("DELETE FROM pendingJobs WHERE id=?",(myid,))
                subjob=self.mutatejob(submittedjob,jobid=jobid,dependency_fid=deps)
                (string,values)=subjob.getsqliteinsert()
                db.execom("INSERT INTO submittedJobs "+string,values)
            except:
                logging.error("Error changing job from pending to submited")
                raise
        else:
            logging.debug("Job %s submission failed, stdout %s, stderr %s, error %d" % (myid,sbatchOut,sbatchErr,sbatchErrCode))
            try:
                db.execom("UPDATE pendingJobs SET attempts = attempts + 1 WHERE id=?",(myid,))
            except:
                logging.error(" ".join(("Error updating the number of attempts of job ",myid)))
                raise
#            if myid in self.attempts:
#                self.attempts[myid]=self.attempts[myid]+1
#            else:
#                self.attempts[myid]=1
            logging.debug("%d failed submissions of the job %s" % (self.get("attempts"),myid))
        return sbatchErrCode

    def adddepattempt(self,db):
        try:
            db.execom("UPDATE pendingJobs SET depattempts = depattempts + 1 WHERE id=?",(self.get("id"),))
        except:
            logging.error(" ".join(("Error updating the number of depattempts of job ",self.get("id"))))
            raise

class submittedjob(job):
    sqlitecolumns=(("id","INTEGER PRIMARY KEY"), ("jobid", "INTEGER"), ("command", "text"), ("partition", "text"), ("priority", "INTEGER"), ("dependency_id", "text"), ("dependency_fid", "text"), ("attempts", "INTEGER"), ("depattempts", "INTEGER"), ("rtime", "TIMESTAMP"), ("stime", "TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL"))

class submittedjob(job):
    sqlitecolumns=(("id","INTEGER PRIMARY KEY"), ("jobid", "INTEGER"), ("command", "text"), ("partition", "text"), ("priority", "INTEGER"), ("dependency_id", "text"), ("dependency_fid", "text"), ("attempts", "INTEGER"), ("depattempts", "INTEGER"), ("rtime", "TIMESTAMP"), ("stime", "TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL"))

class stoppedjob(job):
    sqlitecolumns=(("id","INTEGER PRIMARY KEY"), ("jobid", "INTEGER"), ("command", "text"), ("partition", "text"), ("priority", "INTEGER"), ("dependency_id", "text"), ("dependency_fid", "text"), ("attempts", "INTEGER"), ("depattempts", "INTEGER"), ("rtime", "TIMESTAMP"), ("stime","TIMESTAMP"), ("etime","TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL"))
