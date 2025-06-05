import ceci
import yaml
import argparse
import networkx


def print_ancestors(pipeline_config_file, target):
    with open(pipeline_config_file) as f:
        pipe_config = yaml.safe_load(f)

    # need to manually switch off resume mode because it
    # would stop jobs from being properly in the DAG.
    pipe_config['resume'] = False

    pipe = ceci.Pipeline.create(pipe_config)

    for ancestor in networkx.ancestors(pipe.graph, target):
        if pipe.graph.nodes[ancestor]['type'] == 'stage':
            print(ancestor)

parser = argparse.ArgumentParser()
parser.add_argument('pipeline_config_file')
parser.add_argument('stage_name_or_output_tag')


def main():
    args = parser.parse_args()
    print_ancestors(args.pipeline_config_file, args.stage_name_or_output_tag)

if __name__ == '__main__':
    main()