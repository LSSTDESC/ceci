import os
import yaml
import sys
import parsl
import argparse
from . import Pipeline, PipelineStage
from . import sites
from .executor import ParslExecutor

def export_cwl():
    """Exports pipeline tools"""
    parser = argparse.ArgumentParser(description='Export pipeline elements in CWL format')
    parser.add_argument('output_path', type=str, help='Path to export the CWL tools.')
    parser.add_argument('module', type=str, help='Names of modules to export.')
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

def main():
    """ Main ceci executable, runs the pipeline """
    #TODO: Extract the site configuration from commandline or Workflow itself
    sys.exit(cwltool.main.main(sys.argv[1:], executor=ParslExecutor()))

if __name__ == '__main__':
    main()
