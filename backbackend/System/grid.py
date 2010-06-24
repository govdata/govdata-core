import drmaa
import time
import os

try:
	import System.config.PerMachineSetup as P
	PATH_TO_PYTHON = P.PATH_TO_PYTHON
except:
	PATH_TO_PYTHON = 'python'

def submitJobs(joblist):

    Session = drmaa.Session()
    jobs = []
    
    for j in joblist:
        argstr = j['argstr']
        name = j['name']
        outfile = j['outfile']
    
        jt = Session.createJobTemplate()
        jt.remoteCommand = PATH_TO_PYTHON
        jt.workingDirectory = os.getcwd()       

        jt.args = ["-c","execfile('../System/initialize_for_production'); " + argstr]
        jt.joinFiles = True
        jt.jobEnvironment = dict([(k,os.environ[k]) for k in ['PYTHONPATH','PATH','V8_HOME','LD_LIBRARY_PATH']])        
        jt.outputPath = ':' + os.environ['DataEnvironmentDirectory'] + '/Temp/' + outfile
        jt.errorPath = jt.outputPath
        jt.jobName = name
        jobid = Session.runJob(jt)
        jobs.append(jobid)
        print 'Loading job', name, 'with id', jobid
        
    retvals = [None]*len(jobs)
    while True:
        running = False
        for (i,id) in enumerate(jobs):
            js = Session.jobStatus(id)
            if js == 'done':
                retval = Session.wait(id,drmaa.Session.TIMEOUT_WAIT_FOREVER)
                print 'retval=', retval
                if retval.exitStatus != 0:
                    
                    for j in jobs:
                        try:
                            Session.control(j,drmaa.JobControlAction.TERMINATE)
			except drmaa.InvalidJobException:
                            pass
                    Session.exit()
                    raise Exception, 'Job ' + joblist[i]['name'] + ' threw an exception during grid run.  See error in ' + joblist[i]['outfile'] + '.'
                else:
                    print 'job', id, '(' + joblist[i]['name'] + ')', 'succeeded.'
                    retvals[i] = retval 
                
            elif js == 'failed':
                for j in jobs:
                    try:
                        Session.control(j,drmaa.JobControlAction.TERMINATE)
	    	    except drmaa.InvalidJobException:
	                pass

                Session.exit()
                raise Exception, 'Job ' + joblist[i]['name'] + ' failed during grid run.  See error in ' + joblist[i]['outfile'] + '.'

            else:
                print 'job', id, 'running.'
                running = True
                
        if not running:
            break
        else:
            time.sleep(60)
    
    Session.exit()
    
    return retvals
