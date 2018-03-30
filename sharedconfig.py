import imp, re, os

##Configuration varibles
confvars=imp.load_source("config", os.path.dirname(os.path.abspath(__file__))+"/config.txt")
dbFile=confvars.dbFile
logFile=confvars.logFile
loggingLevel=confvars.loggingLevel
pidFile=confvars.pidFile
wTime=confvars.wTime
maxAttempts=confvars.maxAttempts
maxDepAttempts=confvars.maxDepAttempts
regex=confvars.regex
regexp=re.compile(regex)
