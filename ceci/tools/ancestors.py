import ceci
import yaml
import argparse

def get_ancestors(dag, job):
    for parent in dag[job]:
        yield parent
        yield from get_ancestors(dag, parent)


def print_ancestors(pipeline_config_file, target):
    with open(pipeline_config_file) as f:
        pipe_config = yaml.safe_load(f)

    # need to manually switch off resume mode because it
    # would stop jobs from being properly in the DAG.
    pipe_config['resume'] = False

    with ceci.prepare_for_pipeline(pipe_config):        
        pipe = ceci.Pipeline.create(pipe_config)

    jobs = pipe.run_info[0]
    dag = pipe.build_dag(jobs)

    if target in jobs:
        # in this case the target is the name of a stage.
        job = jobs[target]
    else:
        # otherwise it must be the name of an output tag
        for stage in pipe.stages:
            if target in stage.output_tags():
                job = jobs[stage.instance_name]
                break
        else:
            raise ValueError(f"Could not find job or output tag {target}")

    for ancestor in get_ancestors(dag, job):
        print(ancestor.name)

parser = argparse.ArgumentParser()
parser.add_argument('pipeline_config_file')
parser.add_argument('stage_name_or_output_tag')


def main():
    args = parser.parse_args()
    print_ancestors(args.pipeline_config_file, args.stage_name_or_output_tag)

if __name__ == '__main__':
    main()