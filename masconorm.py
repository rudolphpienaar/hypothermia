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

_stageslist             = '0123'
_str_input              = 'input.nii'
_str_mask               = 'mask.nii'
_str_outStem            = 'out'

class FNNDSC_masconorm(base.FNNDSC):
    '''
    This class is a specialization of the FNNDSC base and geared to running
    some FreeSurfer 'mris_calc' and 'mri_convert' tools on registered and 
    masked data volumes. Essentially, for a given input and a mask, the
    pipeline masks, converts to float, normalizes and filters.
    
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
        self._stageslist                = '0'
        
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
                            --input <inputVol>              \\
                            --mask <maskVol>                \\
                            --outStem <outStem>
    ''' % scriptName
  
    description =  '''
    DESCRIPTION

        `%s' is a thin pipeline that masks, converts-to-float, and
        normalizes volumes.
        
    ARGS

       --stages <stages> (default: '%s')
       The stages to execute. In the 'bet.py' case, there is only a single
       stage, viz. the actual 'bet' process itself. This argument can 
       effectively be ignored.
       
       --input <inputVol> (default: '%s')
       The (registered-to-B0) input volume to process.
              
       --mask <maskVol> (default: '%s')
       The mask volume.

       --outStem <outStem> (default: '%s')
       The stem name to use for output processing.

    EXAMPLES
    
        %s --input asl.nii --mask mask.nii --stages 012 --outStem asl


    ''' % (scriptName, _stageslist, _str_input, _str_mask, _str_outStem, scriptName)
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
                        default=_stageslist,
                        help='analysis stages')
    parser.add_argument('--input', '-i',
                        dest='inFile',
                        action='store',
                        default=_str_input,
                        help='input NIFTI format file')
    parser.add_argument('--outStem', '-o',
                        dest='outStem',
                        action='store',
                        default=_str_outStem,
                        help='output stem filename')
    parser.add_argument('--mask', '-m',
                        dest='mask',
                        action='store',
                        default=_str_mask,
                        help='input mask NIFTI format file')

    args = parser.parse_args()

    mcn = FNNDSC_masconorm(    
                         stages          = args.stages,
                         logTo           = 'masconorm.log',
                         syslog          = True,
                         logTee          = True)
    mcn.name('Mask/Convert/Norm')
    mcn.verbosity(args.verbosity)
    pipeline    = mcn.pipeline()
    pipeline.poststdout(True)
    pipeline.poststderr(True)
    pipeline.name('mcn-subpipe')

    stage0 = stage.Stage_crun(
                        name            = 'mask',
                        fatalConditions = True,
                        syslog          = True,
                        logTo           = 'mask.log',
                        logTee          = True,
                        cmd             = 'mris_calc -o %sB0Mask.nii   \
                                            %s masked %s' % 
                                            (args.outStem, 
                                             args.inFile, args.mask)
                        )
    stage0.def_preconditions(f_fileCheck, obj=stage0, files=[args.inFile,args.mask])
    stage0.def_postconditions(f_stageShellExitCode, obj=stage0)

    stage1 = stage.Stage_crun(
                        name            = 'convert',
                        fatalConditions = True,
                        syslog          = True,
                        logTo           = 'convert.log',
                        logTee          = True,
                        cmd             = 'mri_convert -odt float %sB0Mask.nii   \
                                            %sB0Mask_float.nii' % 
                                            (args.outStem, args.outStem)
                        )
    stage1.def_preconditions(f_fileCheck, obj=stage1, files=['%sB0Mask.nii' % args.outStem])
    stage1.def_postconditions(f_stageShellExitCode, obj=stage1)

    stage2 = stage.Stage_crun(
                        name            = 'filter >= 0',
                        fatalConditions = True,
                        syslog          = True,
                        logTo           = 'fgte0.log',
                        logTee          = True,
                        cmd             = 'mris_calc -o %sB0Mask_float_gte0.nii  \
                                            %sB0Mask_float.nii gte 0' % 
                                            (args.outStem, args.outStem)
                        )
    stage2.def_preconditions(f_fileCheck, obj=stage2, files=['%sB0Mask_float.nii' % args.outStem])
    stage2.def_postconditions(f_stageShellExitCode, obj=stage2)

    stage3 = stage.Stage_crun(
                        name            = 'normalize',
                        fatalConditions = True,
                        syslog          = True,
                        logTo           = 'normalize.log',
                        logTee          = True,
                        cmd             = 'mris_calc -o %sB0Mask_float_gte0_norm.nii  \
                                            %sB0Mask_float_gte0.nii norm' % 
                                            (args.outStem, args.outStem)
                        )
    stage3.def_preconditions(f_fileCheck, obj=stage3, files=['%sB0Mask_float_gte0.nii' % args.outStem])
    stage3.def_postconditions(f_stageShellExitCode, obj=stage3)


    mcnlog = mcn.log()
    mcnlog('INIT: %s\n' % ' '.join(sys.argv))
    mcn.stage_add(stage0)
    mcn.stage_add(stage1)
    mcn.stage_add(stage2)
    mcn.stage_add(stage3)
    mcn.initialize()

    mcn.run()
  
