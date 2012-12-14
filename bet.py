#!/usr/bin/env python

'''

    This "pipeline" demonstrates how to wrap around a single command line
    app in the FNNDSC structuring.
    
'''


import  os
import  sys
import  string
import  argparse
from    _common import systemMisc       as misc
from    _common import crun

import  error
import  message
import  stage

import  fnndsc  as base

_stageslist             = '0'
_str_inputB0            = 'b0.nii'
_str_outMaskStem        = 'b0_mask.nii'

class FNNDSC_bet(base.FNNDSC):
    '''
    This class is a specialization of the FNNDSC base and geared to running
    a single brain extraction tool instance.
    
    '''

    # 
    # Class member variables -- if declared here are shared
    # across all instances of this class
    #
    _dictErr = {
        'inputFileNotFound' : {
            'action'        : 'while checking on input files, ',
            'error'         : 'a required file could not be found.',
            'exitCode'      : 10},
        'noFreeSurferEnv'   : {
            'action'        : 'examining environment, ',
            'error'         : 'it seems that the FreeSurfer environment has not been sourced.',
            'exitCode'      : 11},
        'noStagePostConditions' : {
            'action'        : 'querying a stage for its exitCode, ',
            'error'         : 'it seems that the stagehas not been specified.',
            'exitCode'      : 12},
        'subjectDirnotExist': {
            'action'        : 'examining the <subjectDirectories>, ',
            'error'         : 'the directory does not exist.',
            'exitCode'      : 13},
        'Load'              : {
            'action'        : 'attempting to pickle load object, ',
            'error'         : 'a PickleError occured.',
            'exitCode'      : 14}
    }

                    
    def __init__(self, **kwargs):
        '''
        Basic constructor. Checks on named input args, checks that files
        exist and creates directories.

        '''
        base.FNNDSC.__init__(self, **kwargs)

        self._lw                        = 120
        self._rw                        = 20
        self._l_subject                 = []
        
        self._str_subjectDir            = ''
        self._stageslist                = _stageslist
        
        for key, value in kwargs.iteritems():
            if key == 'stages':         self._stageslist        = value


    def initialize(self):
        '''
        This method provides some "post-constructor" initialization. It is
        typically called after the constructor and after other class flags
        have been set (or reset).
        
        '''

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
            
            
def synopsis(ab_shortOnly = False):
    scriptName = os.path.basename(sys.argv[0])
    shortSynopsis =  '''
    SYNOPSIS

            %s                                          \\
                            [--stages <stages>]             \\
                            [-v|--verbosity <verboseLevel>] \\
                            --input <b0Image>               \\
                            --output <brainMaskImage>
    ''' % scriptName
  
    description =  '''
    DESCRIPTION

        `%s' is a thin pipeline/stage wrapper around the FSL 'bet' app.

    ARGS

       --stages <stages> (default: '%s')
       The stages to execute. In the 'bet.py' case, there is only a single
       stage, viz. the actual 'bet' process itself. This argument can 
       effectively be ignored.

       --input <b0Image> (default: '%s')
       The B0 image on which to run 'bet'.
       
       --output <brainMaskImage> (default: '%s')
       The name of the output masked image.


    EXAMPLES
    
        $>bet.py --input someB0Vol.nii --output B0VolMasked.nii
        
        In this case, two outputs will be generated:
        
            * B0VolMasked.nii.gz -- the brain mask
            * B0VolMasked_mask.nii.gz -- a binary brain mask


    ''' % (scriptName, _stageslist, _str_inputB0, _str_outMaskStem)
    if ab_shortOnly:
        return shortSynopsis
    else:
        return shortSynopsis + description

def f_fileCheck(**kwargs):
    '''
    A file exist checker.
    '''
    stage       = None
    l_file      = []
    for key, val in kwargs.iteritems():
        if key == 'obj':        stage   = val
        if key == 'files':      l_file  = val
    log = stage.log()
    b_allFilesExist = True
    for str_fileName in l_file:
        log('Checking on %s...' % str_fileName, lw=85)
        b_exists = misc.file_exists(str_fileName)
        b_allFilesExist &= b_exists
        log('[ %s ]\n' % b_exists, rw=20, syslog=False)
    return b_allFilesExist

def f_stageShellExitCode(**kwargs):
    '''
    A simple function that returns a conditional based on the
    exitCode of the passed stage object. It assumes global access
    to the <pipeline> object.

    **kwargs:

        obj=<stage>
        The stage to query for exitStatus.
    
    '''
    stage = None
    for key, val in kwargs.iteritems():
        if key == 'obj':                stage                   = val
    if not stage: error.fatal(pipeline, "noStagePostConditions")
    if not stage.callCount():   return True
    if stage.exitCode() == "0": return True
    else: return False

        
#
# entry point
#
if __name__ == "__main__":


    # always show the help if no arguments were specified
    if len( sys.argv ) == 1:
        print synopsis()
        sys.exit( 1 )

    verbosity   = 0

    parser = argparse.ArgumentParser(description = synopsis(True))
    
    parser.add_argument('--verbosity', '-v',
                        dest='verbosity', 
                        action='store',
                        default=0,
                        help='verbosity level')
    parser.add_argument('--stages', '-s',
                        dest='stages',
                        action='store',
                        default='0',
                        help='analysis stages')
    parser.add_argument('--input', '-i',
                        dest='inNifti',
                        action='store',
                        default=_str_inputB0,
                        help='input NIFTI format file')
    parser.add_argument('--output', '-o',
                        dest='outNifti',
                        action='store',
                        default=_str_outMaskStem,
                        help='output NIFTI format file')

    args = parser.parse_args()

    bet = FNNDSC_bet(    stages          = args.stages,
                         logTo           = 'bet.log',
                         syslog          = True,
                         logTee          = True)
    bet.name('FSL bet-wrapper')
    bet.verbosity(args.verbosity)
    pipeline    = bet.pipeline()
    pipeline.poststdout(True)
    pipeline.poststderr(True)
    pipeline.name('bet-subpipe')

    str_cmdArgs = " %s                                      \
                    %s                                      \
                    -m " % ( args.inNifti, args.outNifti)

    stage0 = stage.Stage_crun(
                        name            = 'bet',
                        fatalConditions = True,
                        syslog          = True,
                        logTo           = 'bet.log',
                        logTee          = True,
                        cmd             = 'bet ' + str_cmdArgs
                        )
    stage0.def_preconditions(f_fileCheck, obj=stage0, files=[args.inNifti])
    stage0.def_postconditions(f_stageShellExitCode, obj=stage0)

    betlog = bet.log()
    betlog('INIT: %s\n' % ' '.join(sys.argv))
    bet.stage_add(stage0)
    bet.initialize()

    bet.run()
  
