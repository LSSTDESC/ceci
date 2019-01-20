import os
import yaml
import json
import sys
import argparse
import cwltool.main
from cwltool.loghandler import _logger
from cwltool.context import LoadingContext, RuntimeContext
from . import Pipeline, PipelineStage
from . import sites
import parsl
from .configs import threads_config
from .command_line_tool import customMakeTool
from .argparser import arg_parser

def export_cwl_tools():
    """Exports pipeline tools"""
    parser = argparse.ArgumentParser(description='Export pipeline elements in CWL format')
    parser.add_argument('output_path', type=str, help='Path to export the CWL tools.')
    parser.add_argument('modules', type=str, nargs='+', help='Names of modules to export.')
    args = parser.parse_args()
    path = args.output_path
    modules = args.modules

    # Imports the modules to export
    for module in modules:
        __import__(module)

    # Export each pipeline stage as a CWL app
    for k in PipelineStage.pipeline_stages:
        tool = PipelineStage.pipeline_stages[k][0].generate_cwl()
        tool.export(f'{path}/{k}.cwl')


def ceci2cwl(pipeline_config, output_path):
    """ Exports entire workflow and associated configuration file"""

    path = output_path + '/cwl'
    if not os.path.exists(path):
        os.makedirs(path)

    # YAML input file.
    config = yaml.load(open(pipeline_config))

    # Python modules in which to search for pipeline stages
    modules = config['modules'].split()
    for module in modules:
        __import__(module)

    # Export each pipeline stage as a CWL app
    for k in PipelineStage.pipeline_stages:
        tool = PipelineStage.pipeline_stages[k][0].generate_cwl()
        tool.export(f'{path}/{k}.cwl')

    stages = config['stages']
    inputs = config['inputs']

    pipeline = Pipeline(stages)
    cwl_wf = pipeline.generate_cwl(inputs)
    cwl_wf.export(f'{path}/workflow.cwl')

    # Now export configuration file
    job_config = {}
    inputs['config'] = config['config']
    for inp in cwl_wf.inputs:
        job_config[inp.id] = {"class": "File",
                         "format": inp.format,
                         "path": os.path.abspath(inputs[inp.id])}
    with open(f'{path}/job.json', 'w') as outfile:
        json.dump(job_config, outfile, indent=4, sort_keys=True)
    return f'{path}/workflow.cwl', f'{path}/job.json'

def main():
    """ Main ceci executable, runs the pipeline """
    # Use default cwltool parser to read any additional flags
    parser = arg_parser()
    parsed_args = parser.parse_args(sys.argv[1:])

    # Read configuration and export CWL definition and config file
    config = yaml.load(open(parsed_args.ceci_configuration))

    # Export cwl files and job config to output directory
    worklow, job = ceci2cwl(parsed_args.ceci_configuration, config['work_dir'])
    setattr(parsed_args, "workflow", worklow)
    setattr(parsed_args, "job_order", [job])

    # Load the requested parsl configuration
    if config['parsl_config'] == 'threads':
        parsl.load(threads_config)
    else:
        raise NotImplementedError

    # Adds additional arguments
    setattr(parsed_args, "outdir", config['output_dir'])
    setattr(parsed_args, "basedir", config['work_dir'])

    rc = RuntimeContext(vars(parsed_args))
    rc.shifter = False
    parsed_args.__dict__['parallel'] = True
    rc.basedir = './'
    rc.tmpdir_prefix = rc.basedir+'/tmp/tmp'
    rc.tmp_outdir_prefix = rc.basedir+'/out/out' # type: Text
    # if parsed_args.shifter:
    #     rc.shifter = True
    #     rc.docker_outdir='/spooldir'
    #     rc.docker_stagedir=rc.basedir+'/stage'
    #     rc.docker_tmpdir='/tmpdir'

    lc = LoadingContext(vars(parsed_args))
    lc.construct_tool_object = customMakeTool

    sys.exit(cwltool.main.main(
             args=parsed_args,
             loadingContext=lc,
             runtimeContext=rc))

if __name__ == '__main__':
    main()
