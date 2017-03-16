# schedulerManager
Python scripts to add a layer of control on top of SLURM. Intended to be used to overcome strict limits of number of queued jobs

# Dependencies
It requires the python packages sys, time, logging, os, subprocess, re and pysqlite2 compiled with a sqlite3 version > 3.8.8

# Configuration
Rename the file example_config.txt to config.txt and change the logging level directories and files for your specific plataform.

# Usage
Start the daemon with python sm_daemon.py username start. You can stop it with python sm_daemon.py username stop. The daemon is intended to run in the background and it should not be necessary to do this procedures often.
To submit jobs use python submit.py arguments. To get information about submitted jobs, use python stat.py [-j jobid].

**It is advisable to generate bash scritps calling the previous commands in your path** This allow to use these scripts in a much convenient manner. E.g:
```
stats 1
```

# Installation
TBD

