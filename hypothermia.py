#!/usr/bin/env python

'''

    This "pipeline" demonstrates a SIMD (single-instruction-multiple-data)
    architecture implementation within the context of the CHB PICES MOSIX
    cluster.

    Each stage of the pipeline essentially runs the same command (single
    instruction) on multiple subjects and hemispheres (multiple-data).

    Organizationally, each stage constructs for each of its data targets
    a single command string and then schedules this command on each data
    target on the PICES cluster.

    Stages are completely "fire-and-forget". Once scheduled, the stage has
    no direct mechanism of communicating with each job. Since each stage
    schedules multiple hundreds of jobs, it attempts in its postconditions
    check to query the MOSIX scheduler and count the instances of the
    job it has fired. When this count falls to zero, the jobs are all
    considered complete and the stage postconditions are satisfied.

    Stage postconditions are responsible for blocking processing. Only when
    these postconditions are satisfied does the main "thread" continue.
    Subsequent stages define their precondition check as a direct
    predecessor's postconditions.

'''

import  os
import  sys
import  string
import  argparse
import  time

from    _common import systemMisc       as misc
from    _common import crun
from    _common._colors import Colors

import  error
import  message
import  stage


import  fnndsc  as base
import  socket

_str_b0         = 'b0.nii'
_str_adc        = 'adc.nii'
_str_asl        = 'asl.nii'
_str_outDir     = 'outDir'

scriptName      = os.path.basename(sys.argv[0])

class FNNDSC_hypothermia(base.FNNDSC):
    '''
    This class is a specialization of the FNNDSC base and generates
    MOSIX cluster scheduled runs of a B0/ADC/ASL analysis.
    
    '''

    # 
    # Class member variables -- if declared here are shared
    # across all instances of this class
    #
    _dictErr = {
        'subjectSpecFail'   : {
            'action'        : 'examining command line arguments, ',
            'error'         : 'it seems that no subjects were specified.',
            'exitCode'      : 10},
        'noFreeSurferEnv'   : {
            'action'        : 'examining environment, ',
            'error'         : 'it seems that the FreeSurfer environment has not been sourced.',
            'exitCode'      : 11},
        'notClusterNode'    : {
            'action'        : 'checking the execution environemt, ',
            'error'         : 'script can only run on a cluster node.',
            'exitCode'      : 12},
        'subjectDirnotExist': {
            'action'        : 'examining the <subjectDirectories>, ',
            'error'         : 'the directory does not exist.',
            'exitCode'      : 13},
        'noB0'              : {
            'action'        : 'attempting to load the B0 volume, ',
            'error'         : 'no file was found.',
            'exitCode'      : 14},
        'noADC'             : {
            'action'        : 'attempting to load the ADC volume, ',
            'error'         : 'no file was found.',
            'exitCode'      : 15},
        'noASL'             : {
            'action'        : 'attempting to load the ASL volume, ',
            'error'         : 'no file was found.',
            'exitCode'      : 16},
        'stageExec'         : {
            'action'        : 'executing stage, ',
            'error'         : 'an external error was detected.',
            'exitCode'      : 30},
        'Load'              : {
            'action'        : 'attempting to pickle load object, ',
            'error'         : 'a PickleError occured.',
            'exitCode'      : 20}
    }

                    
    def __init__(self, **kwargs):
        '''
        Basic constructor. Checks on named input args, checks that files
        exist and creates directories.

        '''
        base.FNNDSC.__init__(self, **kwargs)

        self._lw                        = 60
        self._rw                        = 20
        self._l_subject                 = []
        
        self._str_subjectDir            = ''
        self._stageslist                = '12'
        
        for key, value in kwargs.iteritems():
            if key == 'subjectList':    self._l_subject         = value
            if key == 'stages':         self._stageslist        = value


    def initialize(self):
        '''
        This method provides some "post-constructor" initialization. It is
        typically called after the constructor and after other class flags
        have been set (or reset).
        
        '''

        # First, this script should only be run on cluster nodes.
        lst_clusterNodes = ['rc-drno', 'rc-russia', 'rc-thunderball',
                            'rc-goldfinger', 'rc-twice']
        str_hostname    = socket.gethostname()
        #if str_hostname not in lst_clusterNodes:
            #error.fatal(self, 'notClusterNode', 'Current hostname = %s' % str_hostname)

        # Set the stages
        self._pipeline.stages_canRun(False)
        lst_stages = list(self._stageslist)
        for index in lst_stages:
            stage = self._pipeline.stage_get(int(index))
            stage.canRun(True)

        # Check for FS env variable
        self._log('Checking on FREESURFER_HOME', debug=9, lw=self._lw)
        if not os.environ.get('FREESURFER_HOME'):
            error.fatal(self, 'noFreeSurferEnv')
        self._log('[ ok ]\n', debug=9, rw=self._rw, syslog=False)
            
        for str_subj in self._l_subject:
            self._log('Checking on subjectDir <%s>' % str_subj,
                        debug=9, lw=self._lw)
            if os.path.isdir(str_subj):
                self._log('[ ok ]\n', debug=9, rw=self._rw, syslog=False)
            else:
                self._log('[ not found ]\n', debug=9, rw=self._rw,
                            syslog=False)
                error.fatal(self, 'subjectDirnotExist')
                
    def run(self):
        '''
        The main 'engine' of the class.

        '''
        base.FNNDSC.run(self)
            

### Non-class methods
            
def synopsis(ab_shortOnly = False):
    shortSynopsis =  '''
    SYNOPSIS

            %s                                      \\
                            [--stages <stages>]                 \\
                            [-v|--verbosity <verboseLevel>]     \\
                            [--ADC <ADCvolFile>]                \\
                            [--ASL <ASLvolFile>]                \\
                            [--BO <B0volFile>]                  \\
                            [-o|--outDir <perSubjectOutDir>]    \\
                            <Subj1> <Subj2> ... <SubjN>
    ''' % scriptName
  
    description =  '''
    DESCRIPTION

        `%s' runs a comparison between high-deviation co-located regions
        of interest in ADC and ASL volumes.
        
    ARGS
      
        --ADC <ADCvolFile> --ASL <ASLvolFile> --B0 <B0volFile>
        The respective ADC, ASL, and B0 volume files to process. Note
        that these default to '%s', '%s', '%s' respectively.
        
        The script will automatically search for the respective files
        in each <subj> directory as it is being processed.
      
        --stages|-s <stages>
        The stages to execute. This is specified in a string, such as '1234'
        which would imply stages 1, 2, 3, and 4.

        The special keyword 'all' can be used to turn on all stages.

    PRECONDITIONS
    
        o Each subject directory should contain the following files:
        
            - b0.nii
            - asl.nii
            - adc.nii
    
    POSTCONDITIONS
    
        o A directory, <perSubjectOutDir> is created in each subject 
          directory and contains the result tree relevant to the analysis.

    EXAMPLES


    ''' % (scriptName, _str_adc, _str_asl, _str_b0)
    if ab_shortOnly:
        return shortSynopsis
    else:
        return shortSynopsis + description


def f_blockOnScheduledJobs(**kwargs):
    '''
    A simple wrapper around a stage.blockOnShellCmd(...)
    call.
    '''
    str_blockCondition  = 'mosq listall | wc -l'
    str_blockProcess    = 'undefined'
    str_blockUntil      = "0"
    timepoll            = 10
    for key, val in kwargs.iteritems():
        if key == 'obj':                stage                   = val
        if key == 'blockCondition':     str_blockCondition      = val
        if key == 'blockUntil':         str_blockUntil          = val
        if key == 'blockProcess':
            str_blockProcess            = val
            str_blockCondition          = 'mosq listall | grep %s | wc -l' % str_blockProcess
        if key == 'timepoll':           timepoll                = val
    str_blockMsg    = '''\n
    Postconditions are still running: multiple '%s' instances
    detected in MOSIX scheduler. Blocking until all scheduled jobs are
    completed. Block interval = %s seconds.
    \n''' % (str_blockProcess, timepoll)
    str_loopMsg     = 'Waiting for scheduled jobs to complete... ' +\
                      '(hit <ctrl>-c to kill this script).'
                        
    stage.blockOnShellCmd(  str_blockCondition, str_blockUntil,
                            str_blockMsg, str_loopMsg, timepoll)
    return True

        
#
# entry point
#
if __name__ == "__main__":


    # always show the help if no arguments were specified
    if len( sys.argv ) == 1:
        print synopsis()
        sys.exit( 1 )

    l_subj      = []
    b_query     = False
    verbosity   = 0

    parser = argparse.ArgumentParser(description = synopsis(True))
    
    parser.add_argument('l_subj',
                        metavar='SUBJECT', nargs='+',
                        help='SubjectIDs to process')
    parser.add_argument('--verbosity', '-v',
                        dest='verbosity', 
                        action='store',
                        default=0,
                        help='verbosity level')
    parser.add_argument('--stages', '-s',
                        dest='stages',
                        action='store',
                        default='01',
                        help='analysis stages')
    args = parser.parse_args()

    # First, define the container pipeline
    pipe_hypothermia = FNNDSC_hypothermia(
                        subjectList     = args.l_subj,
                        stages          = args.stages,
                        logTo           = 'hypothermia.log',
                        syslog          = True,
                        logTee          = True
                        )
    pipe_hypothermia.verbosity(args.verbosity)
    pipeline    = pipe_hypothermia.pipeline()
    pipeline.log()('INIT: %s %s\n' % (scriptName, ' '.join(sys.argv[1:])))
    pipeline.name('hypothermia')
    pipeline.poststdout(True)
    pipeline.poststderr(True)

    # Now define each stage...

    #
    # Stage 0
    # This is a callback stage, demonstrating how python logic is used
    # to create multiple cluster-based processing instances of the same
    # core FreeSurfer command, each with slightly different operating
    # flags.
    # 
    # In some ways, the stage0.def_stage(...) is vaguely reminiscent
    # of javascript, in as much as the f_stage0callback is a 
    # callback function.
    #
    # PRECONDITIONS:
    # o Check that script is running on a cluster node.
    # 
    stage0 = stage.Stage(
                        name            = 'bet',
                        fatalConditions = True,
                        syslog          = True,
                        logTo           = 'hypothermia-bet.log',
                        logTee          = True
                        )
    def f_stage0callback(**kwargs):
        str_cwd         =  os.getcwd()
        lst_subj        = []
        for key, val in kwargs.iteritems():
            if key == 'subj':   lst_subj        = val
            if key == 'obj':    stage           = val
        for subj in lst_subj:
            # find the relevant input files in each <subj> dir
            os.chdir(subj)
            l_B0        = misc.find(_str_b0)
            if not l_B0: error.fatal(pipe_hypothermia, 'noB0')
            _str_B0File = l_B0[0]
            _str_outDir = '%s/outDir' % os.getcwd()
            _str_outFile = 'b0Brain.nii'
            misc.mkdir(_str_outDir)
            str_prefixCmd = '( cd %s ; ' % (os.getcwd())
            log = stage.log()
            log('Scheduling brain extraction for "%s"...\n' % (subj))
            str_cmd = 'bet.py --input %s --output %s/%s' % (_str_B0File, 
                                                         _str_outDir, _str_outFile)
            #cluster = crun.crun_mosix(cmdPrefix=str_prefixCmd)
            #cluster = crun.crun_mosix()
            cluster = crun.crun()
            #str_ccmd = cluster.echo(True)
            #log(str_ccmd)
            cluster.echo(False)
            cluster.echoStdOut(False)
            cluster.detach(False)
            cluster(str_cmd, waitForChild=True, stdoutflush=True, stderrflush=True)
            if cluster.exitCode():
                error.fatal(pipe_hypothermia, 'stageExec', cluster.stderr())
            os.chdir(str_cwd)
        return True
    stage0.def_stage(f_stage0callback, subj=args.l_subj, obj=stage0)
    stage0.def_postconditions(f_blockOnScheduledJobs, obj=stage0,
                              blockProcess    = 'bet')
    
    #
    # Stage 1
    # This is a callback stage, creating multiple runs of 'coreg.py'
    # to co-register ASL->B0.
    #
    # PRECONDITIONS:
    # o 'mosq listall | grep bet.py | wc -l' evaluating to zero
    # 
    stage1 = stage.Stage(
                        name            = 'coreg',
                        fatalConditions = True,
                        syslog          = True,
                        logTo           = 'hypothermia-coreg.log',
                        logTee          = True
                        )
    stage1.def_preconditions(stage0.def_postconditions()[0], **stage0.def_postconditions()[1])
    def f_stage1callback(**kwargs):
        str_cwd         =  os.getcwd()
        lst_subj        = []
        for key, val in kwargs.iteritems():
            if key == 'subj':   lst_subj        = val
            if key == 'obj':    stage           = val
        for subj in lst_subj:
            # find the relevant input files in each <subj> dir
            os.chdir(subj)
            l_B0        = misc.find(_str_b0)
            l_ASL       = misc.find(_str_asl)
            if not l_B0:  error.fatal(pipe_hypothermia, 'noB0')
            if not l_ASL: error.fatal(pipe_hypothermia, 'noASL')
            _str_B0File  = l_B0[0]
            _str_ASLFile = l_ASL[0]
            _str_outDir  = 'outDir'
            _str_outFile = 'asl2b0.nii'
            misc.mkdir(_str_outDir)
            log = stage.log()
            log('Scheduling coreg for "%s: ASL->B0"...\n' % (subj))
            str_cmd = 'coreg.py --input %s --ref %s --out %s/%s' % (
                                                        _str_ASLFile,
                                                        _str_B0File, 
                                                        _str_outDir, 
                                                        _str_outFile)
            cluster = crun.crun()
            #str_ccmd = cluster.echo(True)
            #log(str_ccmd)
            cluster.echo(False)
            cluster.echoStdOut(False)
            cluster.detach(False)
            cluster(str_cmd, waitForChild=True, stdoutflush=True, stderrflush=True)
            if cluster.exitCode():
                error.fatal(pipe_hypothermia, 'stageExec', cluster.stderr())
            os.chdir(str_cwd)
        return True
    stage1.def_stage(f_stage1callback, subj=args.l_subj, obj=stage1)
    stage1.def_postconditions(f_blockOnScheduledJobs, obj=stage1,
                              blockProcess    = 'coreg.py')

    #
    # Stage 2
    # This is a callback stage, creating multiple runs of 'masconorm.py'
    #
    # PRECONDITIONS:
    # o 'mosq listall | grep coreg.py | wc -l' evaluating to zero
    #
    stage2 = stage.Stage(
                        name            = 'masconorm',
                        fatalConditions = True,
                        syslog          = True,
                        logTo           = 'hypothermia-masconorm-asl.log',
                        logTee          = True
                        )
    
    stage2.def_preconditions(stage1.def_postconditions()[0], **stage1.def_postconditions()[1])
    def f_stage2callback(**kwargs):
        str_cwd         =  os.getcwd()
        lst_subj        = []
        for key, val in kwargs.iteritems():
            if key == 'subj':   lst_subj        = val
            if key == 'obj':    stage           = val
        for subj in lst_subj:
            # find the relevant input files in each <subj> dir
            os.chdir(subj)
            l_B0        = misc.find(_str_b0)
            l_ASL       = misc.find(_str_asl)
            if not l_B0:  error.fatal(pipe_hypothermia, 'noB0')
            if not l_ASL: error.fatal(pipe_hypothermia, 'noASL')
            _str_B0File  = l_B0[0]
            _str_ASLFile = l_ASL[0]
            _str_outDir  = 'outDir'
            _str_outFile = 'asl2b0.nii'
            misc.mkdir(_str_outDir)
            log = stage.log()
            log('Scheduling masconorm for "%s: ASL"..\n' % (subj))
            str_cmd = 'masconorm.py --input %s/asl2b0.nii.gz --mask %s/b0Brain_mask.nii.gz --outStem %s/asl' % (
                                                        _str_outDir,
                                                        _str_outDir,
                                                        _str_outDir)
            #cluster = crun.crun_mosix()
            cluster = crun.crun()
            cluster.echo(False)
            cluster.echoStdOut(False)
            cluster.detach(False)
            cluster(str_cmd, waitForChild=True, stdoutflush=True, stderrflush=True)
            if cluster.exitCode():
                error.fatal(pipe_hypothermia, 'stageExec', cluster.stderr())
            os.chdir(str_cwd)
        return True
    stage2.def_stage(f_stage2callback, subj=args.l_subj, obj=stage2)
    stage2.def_postconditions(f_blockOnScheduledJobs, obj=stage2,
                              blockProcess    = 'masconorm.py')
                              
    #
    # Stage 3
    # This is a callback stage, creating multiple runs of 'masconorm.py'
    #
    # PRECONDITIONS:
    # o 'mosq listall | grep coreg.py | wc -l' evaluating to zero
    #
    stage3 = stage.Stage(
                        name            = 'masconorm',
                        fatalConditions = True,
                        syslog          = True,
                        logTo           = 'hypothermia-masconorm-adc.log',
                        logTee          = True
                        )
    
    stage3.def_preconditions(stage2.def_postconditions()[0], **stage2.def_postconditions()[1])
    def f_stage3callback(**kwargs):
        str_cwd         =  os.getcwd()
        lst_subj        = []
        for key, val in kwargs.iteritems():
            if key == 'subj':   lst_subj        = val
            if key == 'obj':    stage           = val
        for subj in lst_subj:
            # find the relevant input files in each <subj> dir
            os.chdir(subj)
            l_ADC       = misc.find(_str_adc)
            if not l_ADC: error.fatal(pipe_hypothermia, 'noADC')
            _str_ADCFile = l_ADC[0]
            _str_outDir  = 'outDir'
            misc.mkdir(_str_outDir)
            log = stage.log()
            log('Scheduling masconorm for "%s: ADC"..\n' % (subj))
            str_cmd = 'masconorm.py --input %s --mask %s/b0Brain_mask.nii.gz --outStem %s/adc' % (
                                                        _str_ADCFile,
                                                        _str_outDir,
                                                        _str_outDir)
            cluster = crun.crun()
            cluster.echo(False)
            cluster.echoStdOut(False)
            cluster.detach(False)
            cluster(str_cmd, waitForChild=True, stdoutflush=True, stderrflush=True)
            os.chdir(str_cwd)
        return True
    stage3.def_stage(f_stage3callback, subj=args.l_subj, obj=stage3)
    stage3.def_postconditions(f_blockOnScheduledJobs, obj=stage3,
                              blockProcess    = 'masconorm.py')
                              
    
    # Add all the stages to the pipeline  
    pipe_hypothermia.stage_add(stage0)
    pipe_hypothermia.stage_add(stage1)
    pipe_hypothermia.stage_add(stage2)
    pipe_hypothermia.stage_add(stage3)

    # Initialize the pipeline and ... run!
    pipe_hypothermia.initialize()
    pipe_hypothermia.run()
  
