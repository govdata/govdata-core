import drmaa

def submitJobs(jobList):

	Session = drmaa.Session()
	jobs = []
	
	for j in joblist:
	    argstr = j['argstr']
	    name = j['name']
		outfile = j['outfile']
	    
		skip = block_size*i
		jt = Session.createJobTemplate()
		jt.remoteCommand = PATH_TO_PYTHON
		jt.workingDirectory = os.getcwd() 		

		jt.args = ["-c",argstr]
		jt.joinFiles = True
		jt.jobEnvironment = dict([(k,os.environ[k]) for k in ['PYTHONPATH','PATH','V8_HOME','LD_LIBRARY_PATH']])		
		jt.outputPath = ':' + os.environ['DataEnvironmentDirectory'] + '/' + outfile
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
				if retval.exitStatus != 0:
					for j in jobs:
						Session.control(j,'terminate')
					Session.exit()
					raise Exception, 'Job ' + jobs[i]['name'] + ' threw an exception during grid run.  See error in ' + jobs[i]['outfile'] + '.'
				else:
					retvals[i] = retval	
				
			elif js == 'failed':
				for j in jobs:
					Session.control(j,'terminate')
				Session.exit()
				raise Exception, 'bork'
			else:
				running = True
				
		if not running:
			break
		else:
			time.sleep(60)
	
	Session.exit()
	
	return retvals