import ruamel.yaml
import os
import sys
import collections


def is_pipeline_file(info):
    required = ["config", "stages", "launcher", "site", "inputs"]
    return all(r in info for r in required)


def update_pipeline_file(pipeline_info, config_info):
    for stage_info in pipeline_info["stages"]:
        name = stage_info["name"]

        stage_config = config_info.get(name, None)

        if stage_config is None:
            continue

        aliases = stage_config.get("aliases", None)

        if aliases is not None:
            aliases = {k:v.strip() for k, v in aliases.items()}
            stage_info["aliases"] = aliases

def update_config_file(config_info):
    for stage_info in config_info.values():
        r = stage_info.pop("aliases", None)


def update_pipeline_file_group(pipeline_files):
    # configure yaml - these are the approximate
    # settings we have mostly used in TXPipe
    yaml = ruamel.yaml.YAML()
    yaml.indent(sequence=4, offset=2, mapping=4)
    yaml.width = 4096
    yaml.allow_duplicate_keys = True

    # Read all the pipeline files
    pipeline_infos = []
    for pipeline_file in pipeline_files:

        with open(pipeline_file) as f:
            yaml_str = f.read()

        pipeline_info = yaml.load(yaml_str)
        config_file = pipeline_info["config"]
        pipeline_infos.append(pipeline_info)

    # Check that all the pipeline files use the same config file
    for pipeline_info in pipeline_infos:
        if not pipeline_info["config"] == config_file:
            raise ValueError("All pipeline files supplied to this script should use the same config file. Run the script multiple times on different files otherwise.")

    # Read the config file
    with open(config_file) as f:
        yaml_str = f.read()
    config_info = yaml.load(yaml_str)

    # Update all the pipeline files.
    for pipeline_info in pipeline_infos:
        update_pipeline_file(pipeline_info, config_info)
 
    # Only now can we delete the alias information
    update_config_file(config_info)

    # Update all the files in-place
    for pipeline_file, pipeline_info in zip(pipeline_files, pipeline_infos):
        with open(pipeline_file, "w") as f:
            yaml.dump(pipeline_info, f)

    with open(config_file, "w") as f:
        yaml.dump(config_info, f)


def scan_directory_and_update(base_dir):
    groups = collections.defaultdict(list)
    yaml = ruamel.yaml.YAML()
    yaml.allow_duplicate_keys = True
    for dirpath, _, filenames in os.walk(base_dir):
        # just process yaml files
        for filename in filenames:
            if not (filename.endswith(".yaml") or filename.endswith(".yml")):
                continue
            filepath = os.path.join(dirpath, filename)
            with open(filepath) as f:
                yaml_str = f.read()
            try:
                info = yaml.load(yaml_str)
            except:
                print("# Could not read yaml file:", filepath)
                continue

            if is_pipeline_file(info):
                config = info["config"]
                groups[config].append(filepath)

    for config_filename, group in groups.items():
        print("Updating group:", group)
        try:
            with open(config_filename) as f:
                yaml_str = f.read()
        except FileNotFoundError:
            print('# missing', config_filename)
            continue
        if not "alias" in yaml_str:
            continue

        update_pipeline_file_group(group)


def main():
    if len(sys.argv) != 2:
        raise ValueError("Please supply a base directory to work on")
    scan_directory_and_update(sys.argv[1])

if __name__ == "__main__":
    main()
