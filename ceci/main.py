import os
import yaml
import sys
import argparse
import cwltool.main
from cwltool.loghandler import _logger
from cwltool.context import LoadingContext, RuntimeContext
from cwltool.argparser import arg_parser
from . import Pipeline, PipelineStage
from . import sites
import parsl
from .configs import threads_config
from .command_line_tool import customMakeTool

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

def export_cwl_workflow():
    """ Exports entire workflow """
    parser = argparse.ArgumentParser(description='Export ceci workflow to path')
    parser.add_argument('pipeline_config', type=str, help='Ceci config file.')
    parser.add_argument('output_path', type=str, help='Path to export the workflow.')
    args = parser.parse_args()
    path = args.output_path

    if not os.path.exists(path):
        os.makedirs(path)

    # YAML input file.
    config = yaml.load(open(args.pipeline_config))

    # Python modules in which to search for pipeline stages
    modules = config['modules'].split()
    for module in modules:
        __import__(module)

    # Export each pipeline stage as a CWL app
    for k in PipelineStage.pipeline_stages:
        tool = PipelineStage.pipeline_stages[k][0].generate_cwl()
        tool.export(f'{path}/{k}.cwl')

    stages = config['stages']

    # Exports the pipeline itself
    launcher = config.get("launcher", "local")
    if launcher == "local":
        launcher_config = sites.local.make_launcher(stages)
    elif launcher == "cori":
        launcher_config = sites.cori.make_launcher(stages)
    else:
        raise ValueError(f"Unknown launcher {launcher}")

    inputs = config['inputs']

    pipeline = Pipeline(launcher_config, stages)
    cwl_wf = pipeline.generate_cwl(inputs)
    cwl_wf.export(f'{path}/pipeline.cwl')

def main():
    """ Main ceci executable, runs the pipeline """
    #TODO: Extract the site configuration from commandline or Workflow itself
    parser = arg_parser()
    parsed_args = parser.parse_args(sys.argv[1:])

    # Load the requested parsl configuration
    parsl.load(threads_config)

    # Trigger the argparse message if the cwl file is missing
    # Otherwise cwltool will use the default argparser
    # if not parsed_args.workflow:
    #     if os.path.isfile("CWLFile"):
    #         setattr(parsed_args, "workflow", "CWLFile")
    #     else:
    #         _logger.error("")
    #         _logger.error("CWL document required, no input file was provided")
    #         parser.print_help()
    #         sys.exit(1)
    # elif not parsed_args.basedir:
    #     _logger.error("")
    #     _logger.error("Basedir is required for storing itermediate results")
    #     parser.print_help()
    #     sys.exit(1)

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
