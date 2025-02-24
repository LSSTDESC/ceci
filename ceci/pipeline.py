"""Module with core pipeline functionality """

import os
import sys
import graphlib
import yaml
import shutil
from abc import abstractmethod
import collections

from .stage import PipelineStage
from . import minirunner
from .sites import load, get_default_site
from .utils import embolden, extra_paths

RESUME_MODE_RESUME = "resume"
RESUME_MODE_RESTART = "restart"
RESUME_MODE_REFUSE = "refuse"



def override_config(config, extra):
    """Override configuration parameters with extra parameters provided on command line

    Parameters
    ----------
    config : `Mapping`
        The original configuration, will be updated
    extra : `str`
        The additional arguments, in key=value pairs
    """
    print("Over-riding config parameters from command line:")

    for arg in extra:
        key, value = arg.split("=", 1)
        item = key.split(".")
        p = config
        print(f"    {key}: {value}")

        for x in item[:-1]:
            if x in p:
                p = p[x]
            else:
                p[x] = {}
                p = p[x]
        p[item[-1]] = yaml.safe_load(value)


class StageExecutionConfig:
    """
    This class stores information describing how an individual job is to be executed,
    for example how many cores it is run on, and where.  It does not store the
    job input or output information or anything like that.

    TODO: Consider attaching this object to the stage itself.

    Attributes
    ----------

    name: str
        The name of the stage
    class_name: str
        The name of the class of the stage
    module_name: str
        The name of the module for the stage
    site: Site object
        (default the global default) The site this stage is run on
    nprocess: int
        (default 1) The number of (usually MPI) processes to use for this task
    nodes: int
        (default 1) The number of nodes the processes should be spread over
    threads_per_process: int
        (default 1) The number of (usually OpenMP) threads to use per process.
    mem_per_process: float
        (defaut 2GB) The amount of memory in GB required for the job
    image: str
        (default is the site default) A docker image name to use for this task
    volume: str
        (default is the site default) Any volume mappings in the form
        /path/on/host:/path/on/container that the job needs
    stage_class : type
        (default is None) the class of the associated PipelineStage
    stage_obj : stage_class
        (default is None) and instance of stage_class, used to store configuration
    """

    def __init__(self, info):
        """Constructor, build from a dictionary used to set attributes

        Parameters
        ----------
        info : `Mapping'
            Dictionary used to initialize class, see class description
            for class attributes
        """
        # Core attributes - mandatory
        self.name = info["name"]
        self.class_name = info.get("classname", self.name)
        self.module_name = info.get("module_name")
        self.site = info.get("site", get_default_site())

        # Parallelism attributes - optional
        self.nprocess = info.get("nprocess", 1)
        self.nodes = info.get("nodes", 1)
        self.threads_per_process = info.get("threads_per_process", 1)  #
        self.mem_per_process = info.get("mem_per_process", 2)

        # Alias attributes - optional
        self.aliases = info.get("aliases", {})

        # Container attributes - optional.
        # There may be a default container for the entire site the
        # stage is run on, in which case use that if it is not overridden.
        self.image = info.get("image", self.site.config.get("image"))
        self.volume = info.get("volume", self.site.config.get("volume"))

        self.stage_class = None
        self.stage_obj = None

    @classmethod
    def create(cls, stage, **kwargs):
        """Construction method that builds a StageExecutionConfig
        from an existing PipelineStage object

        This is useful when building a Pipeline interactively

        Parameters
        ----------
        stage : `PipelineStage`
            The stage in question

        Keywords
        --------
        The keyword arguments are passed as a dictionary to the class constructor

        Returns
        -------
        sec : `StageExecutionConfig`
            The newly built object
        """
        info = kwargs.copy()
        info["name"] = stage.instance_name
        info["classname"] = stage.name
        info["module_name"] = stage.get_module()
        info["aliases"] = stage.get_aliases()
        sec = cls(info)
        sec.set_stage_obj(stage)
        return sec

    def set_stage_obj(self, stage_obj):
        """Set the stage_obj attribute to a particular object

        Parameters
        ----------
        stage_obj : `PipelineClass`
            The object in question

        Raises
        ------
        TypeError : if stage_obj is not and instance of self.stage_class as
            determined by the self.class_name attribute
        """
        self.stage_class = PipelineStage.get_stage(self.class_name, self.module_name)
        if not isinstance(stage_obj, self.stage_class):  # pragma: no cover
            raise TypeError(f"{str(stage_obj)} is not a {str(self.stage_class)}")
        self.stage_obj = stage_obj

    def build_stage_class(self):
        """Set the stage_class attribute by finding
        self.class_name in the dictionary of classes from `Pipeline_stage`
        """
        self.stage_class = PipelineStage.get_stage(self.class_name, self.module_name)
        return self.stage_class

    def build_stage_object(self, args):
        """Build an instance of the PipelineStage by looking up the
        correct type and passing args to the constructor

        Parameters
        ----------
        args : `Mapping`
            Arguments passed to the constructor of self.stage_class

        Returns
        -------
        obj : `PipelineClass`
            The newly constructed object
        """
        if self.stage_class is None:  # pragma: no cover
            self.stage_class = PipelineStage.get_stage(
                self.class_name, self.module_name
            )
        self.stage_obj = self.stage_class(args, aliases=self.aliases)
        return self.stage_obj

    def generate_full_command(self, inputs, outputs, config):
        """Generate the full command needed to run this stage

        Parameters
        ----------
        inputs : `Mapping`
            Mapping of tags to paths for the stage inputs
        outputs : `Mapping`
            Mapping of tags to paths for the stage outputs
        config : `str`
            Path to file with stage configuration

        Returns
        -------
        command : str
            The command in question
        """
        if self.stage_class is None:
            self.build_stage_class()  # pragma: no cover
        core = self.stage_class.generate_command(
            inputs, config, outputs, self.aliases, self.name
        )
        return self.site.command(core, self)


class FileManager(dict):
    """Small class to manage files within a particular Pipeline

    This is a dict which is used for tag to path mapping,
    but has a couple of additional dicts to manage the reverse mapping and the
    tag to type mapping.


    The tag to path mapping is the thing that the Pipeline uses to set the
    input paths for downstream stages that use the outputs of earlier stages,
    i.e., everything in the pipeline can refer to a particular file by tag.

    The tag defaults to the input or output tag as define in the stage class attributes.
    However, in the case that we want multiple stages of the same class in a pipeline we
    have to alias the tags so that each stage can write to its own location (and so that
    downstream stages can pick up that location correctly)
    """

    def __init__(self):
        """Constructor, makes empty dictionaries"""
        self._tag_to_type = {}
        self._path_to_tag = {}
        dict.__init__(self)

    def __setitem__(self, tag, path):
        """Override dict.__setitem__() to also insert the reverse mapping"""
        dict.__setitem__(self, tag, path)
        self._path_to_tag[path] = tag

    def insert(self, tag, path=None, ftype=None):
        """Insert a file, including the path and the file type

        Parameters
        ----------
        tag : str
            The tag by which this file will be identified
        path : str
            The path to this file
        ftype : type
            The file type for this file
        """
        if path is not None:
            self[tag] = path
            self._path_to_tag[path] = tag
        if tag not in self._tag_to_type:
            self._tag_to_type[tag] = ftype

    def get_type(self, tag):
        """Return the file type associated to a given tag"""
        return self._tag_to_type[tag]

    def get_path(self, tag):
        """Return the path associated to a give tag"""
        return self[tag]

    def get_tag(self, path):
        """Return the tag associated to a given path"""
        return self._path_to_tag[path]

    def insert_paths(self, path_dict):
        """Insert a set of paths from a dict that has tag, path pairs"""
        for key, val in path_dict.items():
            self.insert(key, path=val)

    def insert_outputs(self, stage, outdir):
        """Insert a set of tags and associated paths and file types that are output by a stage"""
        stage_outputs = stage.find_outputs(outdir)
        for tag, ftype in stage.outputs:
            aliased_tag = stage.get_aliased_tag(tag)
            path = stage_outputs[aliased_tag]
            self.insert(aliased_tag, path=path, ftype=ftype)
        return stage_outputs


class Pipeline:
    """
    The Pipeline base class models the shared information and behaviour
    that pipelines need, no matter which workflow manager runs them.

    This includes constructing the pipeline in the first place from
    stages, checking that it is a valid pipeline, finding overall
    inputs and outputs for the pipeline, and constructing the command
    line for a given stage.

    Sub-classes run the pipeline using a given workflow manager.
    """

    def __init__(self, stages, launcher_config, **kwargs):
        """Construct a pipeline using configuraion information.

        Parameters
        ----------
        stages: list[dict]
            Information used to construct each stage and how it is run
        launcher_config: dict
            Any additional configuration that will be needed by the workflow
            management.  The base class does not use this.

        Keywords
        --------
        overall_inputs : Mapping or None
            The global inputs to the pipeline, as Mapping of tag:url
        modules : str
            Space seperated path of modules loaded for this pipeline
        """
        self.launcher_config = launcher_config
        self.data_registry = None

        self.overall_inputs = {}
        self.modules = kwargs.get("modules", "")

        # These are populated as we add stages below
        self.stage_execution_config = {}
        self.stage_names = []

        self.run_info = None
        self.run_config = None
        self.stages = None
        self.pipeline_files = FileManager()
        self.pipeline_outputs = None
        self.stages_config = None
        self.stage_config_data = None
        self.global_config = {}

        # Store the individual stage informaton
        for info in stages:
            self.add_stage(info)

    @staticmethod
    def create(pipe_config):
        """Create a Pipeline of a particular type, using the configuration provided

        Parameters
        ----------
        pipe_config : `Mapping`
            Dictionary of configuration parameters

        Returns
        -------
        pipeline : `Pipeline`
            The newly created pipeline
        """
        launcher_config = pipe_config.get("launcher")
        launcher_name = launcher_config["name"]
        stages_config = pipe_config["config"]
        stages = pipe_config["stages"]
        inputs = pipe_config["inputs"]
        modules = pipe_config["modules"]
        run_config = {
            "output_dir": pipe_config.get("output_dir", "."),
            "log_dir": pipe_config.get("log_dir", "."),
            "resume": pipe_config.get("resume", RESUME_MODE_RESUME),
            "flow_chart": pipe_config.get("flow_chart", ""),
            "registry": pipe_config.get("registry", None),
        }

        if run_config["resume"] is True:
            run_config["resume"] = RESUME_MODE_RESUME
        elif run_config["resume"] is False:
            run_config["resume"] = RESUME_MODE_RESTART

        launcher_dict = dict(cwl=CWLPipeline, parsl=ParslPipeline, mini=MiniPipeline)

        if pipe_config.get("flow_chart", False):
            pipeline_class = FlowChartPipeline
        elif pipe_config.get("dry_run", False):
            pipeline_class = DryRunPipeline
        else:
            try:
                pipeline_class = launcher_dict[launcher_name]
            except KeyError as msg:  # pragma: no cover
                raise KeyError(
                    f"Unknown pipeline launcher {launcher_name}, options are {list(launcher_dict.keys())}"
                ) from msg

        p = pipeline_class(
            stages, launcher_config, overall_inputs=inputs, modules=modules
        )
        p.initialize(inputs, run_config, stages_config)
        return p

    @staticmethod
    def interactive():
        """Build and return a pipeline specifically intended for interactive use"""
        launcher_config = dict(name="mini")
        return MiniPipeline([], launcher_config)

    @staticmethod
    def build_config(
        pipeline_config_filename, extra_config=None, dry_run=False, flow_chart=None
    ):
        """Build a configuration dictionary from a yaml file and extra optional parameters

        Parameters
        ----------
        pipeline_config_filename : str
            The path to the input yaml file
        extra_config : str
            A string with extra parameters in key=value pairs
        dry_run : bool
            A specfic flag to build a pipeline only from dry-runs

        Returns
        -------
        pipe_config : dict
            The resulting configuration
        """

        # YAML input file.
        # Load the text and then expand any environment variables
        with open(pipeline_config_filename) as config_file:
            raw_config_text = config_file.read()
        config_text = os.path.expandvars(raw_config_text)
        # Then parse with YAML
        pipe_config = yaml.safe_load(config_text)

        if extra_config:
            override_config(pipe_config, extra_config)

        # parsl execution/launcher configuration information
        launcher_config = pipe_config.setdefault("launcher", {"name": "mini"})

        # Launchers may need to know if this is a dry-run
        launcher_config["dry_run"] = dry_run
        pipe_config["dry_run"] = dry_run
        launcher_config["flow_chart"] = flow_chart
        pipe_config["flow_chart"] = flow_chart
        return pipe_config

    def __getitem__(self, name):
        """Get a particular stage by name"""
        try:
            return self.stage_execution_config[name].stage_obj
        except Exception as msg:  # pragma: no cover
            raise AttributeError(f"Pipeline does not have stage {name}") from msg

    def __getattr__(self, name):
        """Get a particular stage by name"""
        return self.__getitem__(name)

    def print_stages(self, stream=sys.stdout):
        """Print the list of stages in this pipeline to a stream"""
        for stage in self.stages:
            stream.write(f"{stage.instance_name:20}: {str(stage)}")
            stream.write("\n")

    def setup_data_registry(self, registry_config): #pragma: no cover
        """
        Set up the data registry.

        # TODO: interactive version

        Parameters
        ----------
        registry_config : dict
            A dictionary with information about the data registry to use
        """
        from dataregistry import DataRegistry

        # Establish a connection to the data registry. If the config_file is
        # None the dataregistry will assume the users config file is in the
        # default location (~/.config_reg_access).
        registry = DataRegistry(config_file=registry_config.get("config", None),
                owner_type=registry_config.get("owner_type", "user"),
                owner=registry_config.get("owner", None),
                root_dir=registry_config.get("root_dir", None))

        #if not os.environ.get("NERSC_HOST"):
        #    warnings.warn("The Data Registry is only available on NERSC: not setting it up now.")
        #    return None

        # Save the things that may be useful.
        return {
            "registry": registry,
            "config": registry_config,
        }


    def data_registry_lookup(self, info): #pragma: no cover
        """
        Look up a dataset in the data registry

        Parameters
        ----------
        info : dict
            A dictionary with information about the dataset to look up. Must contain
            either an id, and alias, or a name
        """
        if self.data_registry is None:
            raise ValueError("No data registry configured")

        registry = self.data_registry["registry"]

        # We have various ways of looking up a dataset
        # 1. By the `dataset_id`
        # 2. By the dataset `name`
        # 3. By a dataset alias `name`
        if "id" in info:
            return registry.Query.get_dataset_absolute_path(info["id"])
        elif "name" in info:
            filter = registry.Query.gen_filter("dataset.name", "==", info["name"])
        elif "alias" in info:
            raise NotImplementedError("Alias lookup not yet implemented")
        else:
            raise ValueError("Must specify either id or name in registry lookup")

        # Whatever the lookup, we always require a dataset which has not beed deleted or replaced
        status_filter = registry.Query.gen_filter("dataset.status", "==", 1)

        # Main finder method
        results = registry.Query.find_datasets(["dataset.dataset_id"], [filter, status_filter])

        # Check that we find exactly one dataset matching the query
        if not results:
            raise ValueError(f"Could not find any dataset matching {info} in registry")

        results = results['dataset.dataset_id']

        if len(results) > 1:
            raise ValueError(f"Found multiple datasets matching {info} in registry")

        # Get the absolute path
        return registry.Query.get_dataset_absolute_path(results[0])


    def process_overall_inputs(self, inputs):
        """
        Find the correct paths for the overall inputs to the pipeline.

        Paths may be explicit strings, or may be looked up in the data registry.

        Parameters
        ----------
        inputs : dict
            A dictionary of inputs to the pipeline
        """
        paths = {}
        for tag, value in inputs.items():
            # Case 1, explicit lookup (the original version)
            if isinstance(value, str):
                paths[tag] = value
            # Case 2, dictionary with lookup method
            elif isinstance(value, dict):  #pragma: no cover
                # This means that we will look up a path
                # using the data registry
                paths[tag] = self.data_registry_lookup(value)
            elif value is None:
                paths[tag] = None
            else:
                raise ValueError(f"Unknown input type {type(value)}")
        return paths

    @classmethod
    def read(cls, pipeline_config_filename, extra_config=None, dry_run=False):
        """Create a pipeline for a configuration dictionary from a yaml file and extra optional parameters

        Parameters
        ----------
        pipeline_config_filename : str
            The path to the input yaml file
        extra_config : str
            A string with extra parameters in key=value pairs
        dry_run : bool
            A specfic flag to build a pipeline only from dry-runs

        Returns
        -------
        pipeline : Pipeline
            The newly built pipeline
        """
        pipe_config = cls.build_config(pipeline_config_filename, extra_config, dry_run)
        paths = pipe_config.get("python_paths", [])
        if isinstance(paths, str):  # pragma: no cover
            paths = paths.split()

        modules = pipe_config["modules"].split()
        launcher_config = pipe_config.setdefault("launcher", {"name": "mini"})
        site_config = dict(name="local")
        site_config.update(**pipe_config.get("site"))
        # Pass the paths along to the site
        site_config["python_paths"] = paths
        load(launcher_config, [site_config])

        with extra_paths(paths):
            for module in modules:
                __import__(module)
        return cls.create(pipe_config)

    def add_stage(self, stage_info):
        """Add a stage to the pipeline.

        To begin with this stage is not connected to any others -
        that is determined later.

        There are two ways to add a stage.

        1) Passing in configuration information about the stage, see below

        2) Passing in an instance of a pipeline stage.  Typically this is done when
        building a pipeline interactively.

        The configuration info for this stage must contain at least
        the name of the stage and the name of the site where it is
        to be run.  It can also contain information for the
        StageExecutionConfig above describing parallelism
        and container usage.

        The stage_info can contain the following parameters:
            site: Site object
                The site this stage is run on
            nprocess: int
                (default 1) The number of (usually MPI) processes to use for this task
            nodes: int
                (default 1) The number of nodes the processes should be spread over
            threads_per_process: int
                (default 1) The number of (usually OpenMP) threads to use per process.
            mem_per_process: float
                (defaut 2GB) The amount of memory in GB required for the job
            aliases: dict
                (default {}) A dictionary of aliases mapping tags to new tags
            image: str
                (default is the site default) A docker image name to use for this task
            volume: str
                (default is the site default) Any volume mappings in the form
                /path/on/host:/path/on/container that the job needs

        Parameters
        ----------
        stage_info: dict or PipelineStage
            Configuration information for this stage. See docstring for info.
        """
        if isinstance(stage_info, PipelineStage):
            sec = StageExecutionConfig.create(stage_info)
        else:
            sec = StageExecutionConfig(stage_info)
        self.stage_execution_config[sec.name] = sec
        self.stage_names.append(sec.name)
        if sec.stage_obj is None:
            return {}
        return self.pipeline_files.insert_outputs(sec.stage_obj, ".")

    def build_stage(self, stage_class, **kwargs):
        """Build a stage and add it to the pipeline

        Parameters
        ----------
        stage_class: type
            A subtype of `PipelineStage`, the class of the stage being build

        Returns
        -------
        stage_outputs: `dict`
            The names of the output files

        Notes
        -----
        The keyword arguments will be based to the `stage_class` constructor

        The output files produced by this stage will be added to the
        `Pipeline.pipeline_files` data member, so that they are available to later stages
        """
        kwcopy = kwargs.copy()
        aliases = kwcopy.pop("aliases", {})
        comm = kwcopy.pop("comm", None)
        kwcopy.update(**self.pipeline_files)

        stage = stage_class(kwcopy, comm=comm, aliases=aliases)
        return self.add_stage(stage)

    def remove_stage(self, name):
        """Delete a stage from the pipeline

        Parameters
        ----------
        name: str
            The name of the stage to remove.

        """
        self.stage_names.remove(name)
        del self.stage_execution_config[name]

    def ordered_stages(self, overall_inputs):
        """Produce a linear ordering for the stages.

        Some stages within the pipeline might be runnable in parallel; this
        method does not analyze this, since different workflow managers will
        treat this differently.

        The stages in the pipeline are also checked for consistency, to avoid
        circular pipelines (A->B->C->A) and to ensure that all overall inputs
        needed in the pipeline are supplied from the overall inputs.

        The naive ordering algorithm used is faster when the stages are in
        the correct order to start with.  This won't matter unless you have
        a large number of stages.

        Parameters
        ----------
        overall_inputs: dict{str: str}
            Any inputs that do not need to be generated by the pipeline but are
            instead already supplied at the start.  Mapping is from tag -> path.

        Returns
        -------
        ordered_stages: list[PipelineStage]
            The pipeline stages in an order that can be run.

        """
        stage_names = self.stage_names[:]

        # First pass, get the classes for all the stages
        stage_classes = []
        for stage_name in stage_names:
            sec = self.stage_execution_config[stage_name]
            stage_classes.append(sec.build_stage_class())

        n = len(stage_names)

        # Check for a pipeline output that is already given as an input
        for stage_name in stage_names:
            sec = self.stage_execution_config[stage_name]
            stage_aliases = sec.aliases
            stage_class = sec.stage_class
            for tag in stage_class.output_tags():
                aliased_tag = stage_aliases.get(tag, tag)
                if aliased_tag in overall_inputs:
                    raise ValueError(
                        f"Pipeline stage {stage_name} "
                        f"generates output {aliased_tag}, but "
                        "it is already an overall input"
                    )

        # Now check that the stage names are unique
        stage_set = set(stage_names)
        if len(stage_set) < len(stage_classes):
            raise ValueError("Some stages are included twice in your pipeline")

        # Make a dict finding which stage makes each tag
        creators = {}
        for stage_name in stage_names:
            stage_class = self.stage_execution_config[stage_name].stage_class
            stage_aliases = self.stage_execution_config[stage_name].aliases
            for tag in stage_class.output_tags():
                aliased_tag = stage_aliases.get(tag, tag)
                if aliased_tag in creators:
                    creator1 = creators[aliased_tag]
                    raise ValueError(f"Tag {aliased_tag} is created by both {creator1} and {stage_name}")
                creators[aliased_tag] = stage_name

        # graphlib is new in python3.9 but available in backports for older versions.
        # it provides a topological sort class that we use here.
        graph = graphlib.TopologicalSorter()

        # Go through all the stages and add them to the graph with any stages
        # that they depend on
        for stage_name in stage_names:
            stage_class = self.stage_execution_config[stage_name].stage_class
            stage_aliases = self.stage_execution_config[stage_name].aliases

            # All stages can be added to the graph, then calling add
            # again below will add the dependencies
            graph.add(stage_name)

            for tag in stage_class.input_tags():
                aliased_tag = stage_aliases.get(tag, tag)
                creator = creators.get(aliased_tag)

                # If the tag is in the overall inputs then there is no dependency
                if aliased_tag in overall_inputs:
                    pass
                # If nothing creates the tag then it is an error
                elif creator is None:
                    raise ValueError(f"Tag {aliased_tag} is not created by any stage and is needed by stage {stage_name}")
                # If the tag is created by another stage then add that as a dependency
                else:
                    graph.add(stage_name, creator)

        return list(graph.static_order())

    def initialize_stages(self, stage_names, overall_inputs, stages_config):
        """Initialize the stages in the pipeline

        Parameters
        ----------
        stage_names : list[str]
            The ordered names of the stages to initialize
        overall_inputs : dict{str: str}
            Any inputs that do not need to be generated by the pipeline but are
            instead already supplied at the start.  Mapping is from tag -> path.
        stages_config: str, dict, or None
            The configuration file for the stages, or the configuration dictionary

        Returns
        -------
        stages : list[PipelineStage]
        """
        if isinstance(stages_config, str):
            with open(stages_config) as f:
                stages_config_data = yaml.safe_load(f)
        elif isinstance(stages_config, dict):
            stages_config_data = stages_config
        elif stages_config is None:
            stages_config_data = {}
        else:
            raise ValueError("stages_config must be a filename, a dict, or None")

        stages = []
        all_inputs = overall_inputs.copy()
        for stage_name in stage_names:
            # Find the stage class and execution config
            sec = self.stage_execution_config[stage_name]
            stage_aliases = sec.aliases
            stage_class = sec.stage_class

            # In interactive use the stage object might already be created
            # otherwise we need to create it
            if sec.stage_obj is not None:
                orig_stage_config = stages_config_data.get(stage_name, {}).copy()
                stage = sec.stage_obj
                stage.config.update(**orig_stage_config)
            else:
                # Find the inputs for this stage and set up the arguments
                # to the stage init method
                args = stages_config_data.get(stage_name, {})
                for tag in stage_class.input_tags():
                    aliased_tag = stage_aliases.get(tag, tag)
                    args[aliased_tag] = all_inputs[aliased_tag]
                args["config"] = stages_config

                # Make the stage object and find the outputs,
                # keeping track of them for the next stages
                stage = sec.build_stage_object(args)
                stage_outputs = stage.find_outputs(".")
                for tag in stage_class.output_tags():
                    aliased_tag = stage_aliases.get(tag, tag)
                    all_inputs[aliased_tag] = stage_outputs[aliased_tag]

            stages.append(stage)
        return stages


    def initialize(self, overall_inputs, run_config, stages_config):
        """Load the configuation for this pipeline

        Parameters
        ----------
        overall_inputs : `Mapping`
            A mapping from tag to path for all of the overall inputs needed by this pipeline
        run_config : `Mapping`
            Configuration parameters for how to run the pipeline
        stages_config: `str`
            File with stage configuration parameters

        Returns
        -------
        self.run_info : information on how to run the pipeline, as provided by sub-class `initiate_run` method
        self.run_config : copy of configuration parameters on how to run the pipeline
        """

        # Set up paths to our overall input files
        if run_config.get("registry") is not None:
            self.data_registry = self.setup_data_registry(run_config["registry"])
        self.overall_inputs = self.process_overall_inputs(overall_inputs)
        self.pipeline_files.insert_paths(self.overall_inputs)

        # Make a copy, since we maybe be modifying these
        self.run_config = run_config.copy()

        self.stages_config = stages_config
        if self.stages_config is not None:
            with open(self.stages_config) as stage_config_file:
                self.stage_config_data = yaml.safe_load(stage_config_file)
        else:  # pragma: no cover
            self.stage_config_data = {}
        self.global_config = self.stage_config_data.pop("global", {})
        for v in self.stage_config_data.values():
            v.update(self.global_config)

        # Get the stages in the order we need.
        ordered_stages = self.ordered_stages(self.overall_inputs)
        self.stages = self.initialize_stages(ordered_stages, self.overall_inputs, self.stages_config)

        # Initiate the run.
        # This is an implementation detail for the different subclasses to store
        # necessary information about the run if necessary.
        # Usually, the arguments are ignored, but they are provided in case a class needs to
        # do something special with any of them.
        self.run_info = self.initiate_run(self.overall_inputs)

        # make sure output directories exist
        os.makedirs(run_config["output_dir"], exist_ok=True)
        os.makedirs(run_config["log_dir"], exist_ok=True)

        for stage in self.stages:
            # If we are in "resume" mode and the pipeline has already been run
            # then we re-use any existing outputs.  User is responsible for making
            # sure they are complete!

            if self.should_skip_stage(stage):
                stage.already_finished()
                self.pipeline_files.insert_outputs(stage, run_config["output_dir"])

            # Otherwise, run the pipeline and register any outputs from the
            # pipe element.
            else:
                stage_outputs = self.enqueue_job(stage, self.pipeline_files)
                self.pipeline_files.insert_paths(stage_outputs)

        return self.run_info, self.run_config

    def run(self):
        """Run the pipeline are return the execution status"""
        status = self.run_jobs()
        # When the
        self.pipeline_outputs = self.find_all_outputs()
        return status

    def find_all_outputs(self):
        """Find all the outputs associated to this pipeline

        Returns
        -------
        outputs : dict
            A dictionary of tag : path pairs will all of this Pipeline's outputs
        """
        outputs = {}
        for stage in self.stages:
            stage_outputs = stage.find_outputs(self.run_config["output_dir"])
            outputs.update(stage_outputs)
        return outputs

    @abstractmethod
    def initiate_run(self, overall_inputs):  # pragma: no cover
        """Setup the run and return any global information about how to run the pipeline"""
        raise NotImplementedError()

    @abstractmethod
    def enqueue_job(self, stage, pipeline_files):  # pragma: no cover
        """Setup the job for a single stage, and return stage specific information"""
        raise NotImplementedError()

    @abstractmethod
    def run_jobs(self):  # pragma: no cover
        """Actually run all the jobs and return the execution status"""
        raise NotImplementedError()

    def should_skip_stage(self, stage):
        """Return true if we should skip a stage because it is finished and we are in resume mode"""
        resume_mode = self.run_config["resume"]
        if resume_mode is True:
            resume_mode = "resume"
        elif resume_mode is False:
            resume_mode = "restart"

        if resume_mode == "restart":
            return False

        outputs = stage.find_outputs(self.run_config["output_dir"]).values()

        if resume_mode == "resume":
            return all(os.path.exists(output) for output in outputs)
        elif resume_mode == "refuse":
            if any(os.path.exists(output) for output in outputs):
                raise RuntimeError(f"Output files already exist for stage {stage} and resume mode is 'refuse'")
        else:
            raise ValueError(f"Unknown resume mode: {resume_mode}")

    def save(self, pipefile, stagefile=None, reduce_config=False, **kwargs):
        """Save this pipeline state to a yaml file

        Paramaeters
        -----------
        pipeline: str
            Path to the file were we save this
        stagefile: str
            Optional path to where we save the configuration file
        reduce_config: bool
            If true, reduce the configuration by parsing out the inputs, outputs and global params

        
        Keywords
        --------
        site_name: str
            Used to override site name
        """
        pipe_dict = {}
        stage_dict = {}
        pipe_info_list = []
        if stagefile is None:
            stagefile = os.path.splitext(pipefile)[0] + "_config.yml"
        if self.run_config is not None:
            pipe_dict.update(**self.run_config)
        pipe_dict["config"] = stagefile
        if reduce_config:
            stage_dict["global"] = self.global_config
        site = None
        for key, val in self.stage_execution_config.items():
            if val.stage_obj is None:  # pragma: no cover
                raise ValueError(f"Stage {key} has not been built, can not save")
            if site is None:
                site = val.site.config
            pipe_stage_info = dict(
                name=val.name,
                classname=val.class_name,
                nprocess=val.nprocess,
                module_name=val.module_name,
                aliases=val.aliases,
            )

            if val.threads_per_process != 1:
                pipe_stage_info["threads_per_process"] = val.threads_per_process
            pipe_info_list.append(pipe_stage_info)
            stage_dict[key] = val.stage_obj.get_config_dict(
                self.global_config, reduce_config=reduce_config
            )

        module_list = list(
            {
                val[0].get_module().split(".")[0]
                for val in PipelineStage.pipeline_stages.values()
            }
        )
        not_first = False
        if self.modules:
            not_first = True
        for m_ in module_list:
            if not_first:
                self.modules += " "
            self.modules += f"{m_}"
            not_first = True
        pipe_dict["modules"] = self.modules
        pipe_dict["inputs"] = self.overall_inputs
        pipe_dict["stages"] = pipe_info_list
        pipe_dict["site"] = site
        pipe_dict["site"]["name"] = kwargs.get('site_name', 'local')
        with open(pipefile, "w") as outfile:
            try:
                yaml.dump(pipe_dict, outfile, sort_keys=False)
            except Exception as msg:  # pragma: no cover
                print(f"Failed to save {str(pipe_dict)} because {msg}")
        with open(stagefile, "w") as outfile:
            try:
                yaml.dump(stage_dict, outfile, sort_keys=False)
            except Exception as msg:  # pragma: no cover
                print(f"Failed to save {str(stage_dict)} because {msg}")

    def make_flow_chart(self, filename):
        import pygraphviz

        # Make a graph object
        graph = pygraphviz.AGraph(directed=True)

        # Nodes we have already added
        seen = set()

        # Dictionary to track nodes by their inputs and outputs
        node_groups = {}


        # Add overall pipeline inputs
        for inp in self.overall_inputs.keys():
            graph.add_node(inp, shape="box", color="gold", style="filled")
            seen.add(inp)

        for stage in self.stages:
            # add the stage itself
            graph.add_node(
                stage.instance_name, shape="ellipse", color="orangered", style="filled"
            )
            # connect that stage to its inputs
            for inp, _ in stage.inputs:
                inp = stage.get_aliased_tag(inp)
                if inp not in seen:  # pragma: no cover
                    graph.add_node(inp, shape="box", color="skyblue", style="filled")
                    seen.add(inp)
                graph.add_edge(inp, stage.instance_name, color="black")
            # and to its outputs
            for out, _ in stage.outputs:
                out = stage.get_aliased_tag(out)
                if out not in seen:
                    graph.add_node(out, shape="box", color="skyblue", style="filled")
                    seen.add(out)
                graph.add_edge(stage.instance_name, out, color="black")

        # We want to group together all the files that all created
        # by the same stage and also all used by the same stages, to
        # reduce the number of nodes in the graph and make it more readable.
        # First we find that grouping.
        node_groups = collections.defaultdict(list)
        for node in graph.nodes_iter():
            # only affect the nodes representing files
            if node.attr['color'] != "skyblue":
                continue
            # Find the stage node that created this file,
            # and all the stage nodes that make use of it
            edge_in = graph.in_edges(node)[0]
            creator = edge_in[0]
            users = []
            for edge in graph.out_edges(node):
                users.append(edge[1])
            key = (creator, tuple(users))
            node_groups[key].append(node)

        # Now we remove all the groups of nodes with more than one in
        # and replace them with a single node
        for key, nodes in node_groups.items():
            if len(nodes) > 1:
                if len(nodes) > 4:
                    # make a string with two nodes per line
                    node_names = []
                    for i in range(0, len(nodes), 2):
                        node_names.append(",  ".join(nodes[i:i+2]))
                    new_node = "\n".join(node_names)
                else:
                    new_node = "\n".join(nodes)
                graph.remove_nodes_from(nodes)
                graph.add_node(new_node, shape="box", color="skyblue", style="filled")
                graph.add_edge(key[0], new_node, color="black")
                for user in key[1]:
                    graph.add_edge(new_node, user, color="black")

        # finally, output the stage to file
        if filename.endswith(".dot"):
            graph.write(filename)
        else:
            graph.draw(filename, prog="dot")

    def generate_stage_command(self, stage_name, **kwargs):
        """Generate the command to run one stage in this pipeline

        Paramaeters
        -----------
        stage_name: str
            The name of the stage
        kwargs: dict        
            Used to override pipeline inputs
        """
        try:
            sec = self.stage_execution_config[stage_name]
        except KeyError as msg:
            raise KeyError(f'Failed to find stage named {stage_name} in {list(self.stage_execution_config.keys())}') from msg

        if sec.stage_obj is not None:
            the_stage = sec.stage_obj
        else:            
            for i, stage_name_ in self.stage_names:
                if stage_name == stage_name_:
                    idx = i
                    break
            if idx is None:
                raise KeyError(f'Failed to find stage named {stage_name} in {self.stage_names}')
            the_stage = self.stages[idx]

        all_inputs = self.pipeline_files.copy()
        all_inputs.update(**kwargs)

        outputs = the_stage.find_outputs(self.run_config["output_dir"])
        return sec.generate_full_command(all_inputs, outputs, self.stages_config)
    

class DryRunPipeline(Pipeline):
    """A pipeline subclass which just does a dry-run, showing which commands
    would be executed.

    See the base class for almost all behaviour.

    No additional attributes.
    """

    def initiate_run(self, overall_inputs):
        return []

    def should_skip_stage(self, stage):
        return False

    def enqueue_job(self, stage, pipeline_files):
        outputs = stage.find_outputs(self.run_config["output_dir"])
        sec = self.stage_execution_config[stage.instance_name]

        cmd = sec.generate_full_command(pipeline_files, outputs, self.stages_config)

        # Replace the first instance of the stage name with bold
        # text, but only if we are printing to screen. This helps the
        # eye pick out the stage you want to run.
        if sys.stdout.isatty():
            cmd = cmd.replace(stage.instance_name, embolden(stage.instance_name), 1)

        self.run_info.append(cmd)
        return outputs

    def run_jobs(self):
        for cmd in self.run_info:
            print(cmd)
            print("\n")
        return 0

    def find_all_outputs(self):
        return {}


class FlowChartPipeline(DryRunPipeline):
    def run_jobs(self):
        filename = self.run_config["flow_chart"]
        self.make_flow_chart(filename)
        return 0


class ParslPipeline(Pipeline):
    """A subclass of Pipeline that uses Parsl to manage workflow."""

    def initiate_run(self, overall_inputs):
        return []  # list of futures

    def enqueue_job(self, stage, pipeline_files):

        from parsl.data_provider.files import File

        # log_dir = self.run_config["log_dir"]
        # convert the command into an app
        app = self.generate_app(stage, self.run_config)

        # Convert the dicts of inputs/outputs to the list that
        # parsl wants.
        # The inputs that exist already need to be converted into Parsl File objects.
        # The ones that don't stay as data futures
        inputs1 = stage.find_inputs(pipeline_files)
        inputs = [
            File(val) if isinstance(val, str) else val for val in inputs1.values()
        ]
        inputs.append(File(self.stages_config))
        # The outputs are just strings.  python dicts are now ordered,
        # so this works okay.
        outputs = [
            File(f) for f in stage.find_outputs(self.run_config["output_dir"]).values()
        ]

        # have parsl queue the app
        future = app(inputs=inputs, outputs=outputs)
        self.run_info.append((stage.instance_name, future))
        return {stage.get_aliased_tag(tag): future.outputs[i] for i, tag in enumerate(stage.output_tags())}

    def run_jobs(self):
        from parsl.app.errors import BashExitFailure

        log_dir = self.run_config["log_dir"]
        # Wait for the final results, from all files
        for stage_name, future in self.run_info:
            try:
                # This waits for b/g pipeline completion.
                future.result()
            # Parsl emits this on any non-zero status code.
            except BashExitFailure:
                stdout_file = f"{log_dir}/{stage_name}.err"
                stderr_file = f"{log_dir}/{stage_name}.out"
                sys.stderr.write(
                    f"""
*************************************************
Error running pipeline stage {stage_name}.

Standard output and error streams below.

*************************************************

Standard output:
----------------

"""
                )
                if os.path.exists(stdout_file):
                    with open(stdout_file) as _stdout:
                        sys.stderr.write(_stdout.read())
                else:  # pragma: no cover
                    sys.stderr.write("STDOUT MISSING!\n\n")

                sys.stderr.write(
                    """
*************************************************

Standard error:
----------------

"""
                )

                if os.path.exists(stderr_file):
                    with open(stderr_file) as _stderr:
                        sys.stderr.write(_stderr.read())
                else:  # pragma: no cover
                    sys.stderr.write("STDERR MISSING!\n\n")
                return 1
        return 0

    def generate_app(self, stage, run_config):
        """Build a parsl app that wraps this pipeline stage.

        This object is passed onto parsl for execution.

        Parameters
        ----------
        stage: PipelineStage
            The stage to wrap

        log_dir: str
            The directory to put the logs in.

        sec: StageExecutionConfig
            The execution info for this stage.

        Returns
        -------
        app: App
            A parsl app object

        """
        import parsl

        module = stage.get_module()
        module = module.split(".")[0]

        inputs = {}
        outputs = {}

        # Parsl wants a function with the inputs and outputs
        # extracted from lists.  That function should iself return
        # a string representing the bash cmd line.
        # We build up all these components here.

        # Parsl wants our functions to take their input/output paths
        # from inputs[0], inputs[1], etc.
        for i, inp in enumerate(stage.input_tags()):
            inp = stage.get_aliased_tag(inp)
            inputs[inp] = f"{{inputs[{i}]}}"
        for i, out in enumerate(stage.output_tags()):
            out = stage.get_aliased_tag(out)
            outputs[out] = f"{{outputs[{i}]}}"

        # The last input file is always the config file
        config_index = len(stage.input_tags())
        config = f"{{inputs[{config_index}]}}"

        # This includes all the "mpirun" stuff.
        sec = self.stage_execution_config[stage.instance_name]
        executor = sec.site.info["executor"]

        # Construct the command line call
        core = sec.generate_full_command(inputs, outputs, config)
        cmd1 = sec.site.command(core, sec)
        log_dir = run_config["log_dir"]

        # We will be exec'ing this here.  We can't just define it inline
        # because it just gets too painful with all the i/o names, so instead
        # we build and exec a string.
        template = f"""
@parsl.app.app.bash_app(executors=[executor])
def {stage.instance_name}(inputs, outputs, stdout='{log_dir}/{stage.instance_name}.out', stderr='{log_dir}/{stage.instance_name}.err'):
    cmd = '{cmd1}'.format(inputs=inputs, outputs=outputs)
    print("Launching command:")
    print(cmd, " 2> {log_dir}/{stage.instance_name}.err 1> {log_dir}/{stage.instance_name}.out")
    return cmd
"""
        print(template)

        # local variables for creating this function.
        d = {"executor": executor.label, "cmd1": cmd1}
        exec(template, {"parsl": parsl}, d)  # pylint: disable=exec-used

        # Return the function itself.
        return d[stage.instance_name]


class MiniPipeline(Pipeline):
    """A pipeline subclass that uses Minirunner, a sub-module
    of ceci, to run.

    Minirununer is a small tool I wrote suitable for interactive
    jobs on cori.  It launches jobs locally (not through a batch system),
    which parsl can also do, but it has a simple and clearer (to me at least)
    understanding of available nodes and cores.

    """

    def __init__(self, *args, **kwargs):
        """Create a MiniRunner Pipeline

        In addition to parent initialization parameters (see the
        Pipeline base class), this subclass can take these optional
        keywords.

        Parameters
        ----------
        callback: function(event_type: str, event_info: dict)
            A function called when jobs launch, complete, or fail,
            and when the pipeline aborts.  Can be used for tracing
            execution.  Default=None.

        sleep: function(t: float)
            A function to replace time.sleep called in the pipeline
            to wait until the next time to check process completion
            Most normal usage will not need this.  Default=None.
        """
        self.callback = kwargs.pop("callback", None)
        self.sleep = kwargs.pop("sleep", None)
        super().__init__(*args, **kwargs)

    def build_dag(self, jobs):
        """Build a directed acyclic graph of a set of stages.

        The DAG is represented by a list of jobs that each other job
        depends on.  If all a job's dependencies are complete
        then it can be run.

        Fun fact: the word "dag" is also Australian slang for a
        "dung-caked lock of wool around the hindquarters of a sheep".
        and is used as a semi-affectionate insult.

        Parameters
        ----------

        stages: list[PipelineStage]
            A list of stages to generate the DAG for

        """
        depend = {}

        # for each stage in our pipeline ...
        for stage in self.stages[:]:
            if stage.instance_name not in jobs:
                continue
            job = jobs[stage.instance_name]
            depend[job] = []
            # check for each of the inputs for that stage ...
            for tag in stage.input_tags():
                aliased_tag = stage.get_aliased_tag(tag)
                for potential_parent in self.stages[:]:
                    # if that stage is supplied by another pipeline stage
                    if potential_parent.instance_name not in jobs:  # pragma: no cover
                        continue
                    potential_parent_tags = [
                        potential_parent.get_aliased_tag(tag_)
                        for tag_ in potential_parent.output_tags()
                    ]
                    if aliased_tag in potential_parent_tags:
                        depend[job].append(jobs[potential_parent.instance_name])
        return depend

    def initiate_run(self, overall_inputs):
        jobs = {}
        stages = []
        return jobs, stages

    def enqueue_job(self, stage, pipeline_files):
        sec = self.stage_execution_config[stage.instance_name]
        outputs = stage.find_outputs(self.run_config["output_dir"])
        cmd = sec.generate_full_command(pipeline_files, outputs, self.stages_config)
        job = minirunner.Job(
            stage.instance_name,
            cmd,
            cores=sec.threads_per_process * sec.nprocess,
            nodes=sec.nodes,
        )
        self.run_info[0][stage.instance_name] = job
        self.run_info[1].append(stage)
        return outputs

    def run_jobs(self):
        jobs, _ = self.run_info
        # Now the jobs have all been queued, build them into a graph
        graph = self.build_dag(jobs)
        nodes = get_default_site().info["nodes"]
        log_dir = self.run_config["log_dir"]

        # This pipeline, being mainly for testing, can only
        # run at a single site, so we can assume here that the
        # sites are all the same
        sec = self.stage_execution_config[self.stage_names[0]]
        nodes = sec.site.info["nodes"]

        # Run under minirununer
        runner = minirunner.Runner(
            nodes, graph, log_dir, callback=self.callback, sleep=self.sleep
        )
        interval = self.launcher_config.get("interval", 3)
        try:
            runner.run(interval)
        except minirunner.FailedJob as error:
            sys.stderr.write(
                f"""
*************************************************
Error running pipeline stage {error.job_name}.
Failed after {error.run_time}.

Standard output and error streams in {log_dir}/{error.job_name}.out
*************************************************
"""
            )
            return 1

        return 0


class CWLPipeline(Pipeline):
    """Export the pipeline as Common Workflow Language files and optionally run it
    with cwltool or another CWL-aware runner.

    """

    @staticmethod
    def make_inputs_file(
        stages, overall_inputs, stages_config, inputs_file
    ):  # pylint: disable=missing-function-docstring

        # find out the class of the file objects.  This is a bit ugly,
        # but the only way we record this now it in the stages.
        input_types = {}
        for stage in stages:
            for tag, ftype in stage.inputs:
                if (tag in overall_inputs) and (tag not in input_types):
                    input_types[tag] = ftype

        inputs = {}
        # First we build up the file inputs, each with this dictionary
        # information.
        for tag, filepath in overall_inputs.items():
            ftype = input_types[tag]
            filepath = os.path.abspath(filepath)
            d = {"class": "File", "path": filepath, "format": ftype.format}
            inputs[tag] = d

        # CWL also wants the config information passed through in an inputs
        # file, so it is all collected together.
        with open(stages_config) as _stages_config_file:
            stage_config_data = yaml.safe_load(_stages_config_file)
        global_config = stage_config_data.pop("global", {})

        # For each stage, we check if any of its config information
        # is set in the config file
        for stage in stages:
            # There might be nothing if no options are needed.
            this_stage_config = stage_config_data.get(stage.instance_name, {})
            # Record only keys that have been set.  If any are missing
            # it is an error that will be noticed later.
            for key in stage.config_options:
                val = this_stage_config.get(key, global_config.get(key))
                if val is not None:
                    inputs[f"{key}@{stage.instance_name}"] = val

        inputs["config"] = {
            "class": "File",
            "path": os.path.abspath(stages_config),
            # YAML file indicator:
            "format": "http://edamontology.org/format_3750",
        }

        # Save to the inputs file
        with open(inputs_file, "w") as f:
            yaml.dump(inputs, f)

    def initiate_run(self, overall_inputs):
        from cwl_utils.parser.cwl_v1_0 import Workflow

        wf = Workflow([], [], [], cwlVersion="v1.0")

        cwl_dir = self.launcher_config["dir"]
        os.makedirs(cwl_dir, exist_ok=True)

        # Write the inputs files
        inputs_file = f"{cwl_dir}/cwl_inputs.yml"
        self.make_inputs_file(
            self.stages, overall_inputs, self.stages_config, inputs_file
        )

        # CWL treats overall inputs differently, and requires
        # that we include the config file in there too
        self.overall_inputs["config"] = self.stages_config

        return {
            "workflow": wf,
            "cwl_dir": cwl_dir,
            "inputs_file": inputs_file,
            # keeps track of overall pipeline inputs we have already found
            "workflow_inputs": set(),
            "workflow_outputs": {},
        }

    def enqueue_job(self, stage, pipeline_files):
        from cwl_utils.parser.cwl_v1_0 import WorkflowStep, WorkflowStepInput
        from cwl_utils.parser.cwl_v1_0 import WorkflowOutputParameter, InputParameter

        cwl_dir = self.run_info["cwl_dir"]
        workflow = self.run_info["workflow"]
        log_dir = self.run_config["log_dir"]

        # Create a CWL representation of this step
        cwl_tool = stage.generate_cwl(log_dir)
        with open(f"{cwl_dir}/{stage.instance_name}.cwl", "w") as f:
            yaml.dump(cwl_tool.save(), f)

        # Load that representation again and add it to the pipeline
        step = WorkflowStep(stage.instance_name, [], [], run=f"{cwl_tool.id}.cwl")

        # For CWL these inputs are a mix of file and config inputs,
        # so not he same as the pipeline_files we usually see
        for inp in cwl_tool.inputs:

            if inp.id in self.overall_inputs:
                name = inp.id
            # If this input is an putput from an earlier stage
            # then it takes its name based on that
            elif inp.id in pipeline_files:
                name = pipeline_files[inp.id] + "/" + inp.id
            # otherwise if it's a config option we mangle
            # it to avod clashes
            elif inp.id in stage.config_options:
                name = f"{inp.id}@{cwl_tool.id}"
            # otherwise just leave it as-is
            else:  # pragma: no cover
                name = inp.id

            # If it's an overall input to the entire pipeline we
            # record that.  Or a configuration option.
            # And we don't want things that we've already recorded.
            if (
                (inp.id in self.overall_inputs) or (inp.id in stage.config_options)
            ) and (name not in self.run_info["workflow_inputs"]):
                self.run_info["workflow_inputs"].add(name)
                # These are the overall inputs to the enture pipeline.
                # Convert them to CWL input parameters
                cwl_inp = InputParameter(
                    name, label=inp.label, type=inp.type, format=inp.format
                )
                cwl_inp.default = inp.default

                # Bypassing cwlgen type check in case of arrays
                if isinstance(inp.type, dict):
                    cwl_inp.type = inp.type

                # record that these are overall pipeline inputs
                workflow.inputs.append(cwl_inp)

            # Record that thisis an input to the step.
            step.in_.append(WorkflowStepInput(id=inp.id, source=name))

        # Also record that we want all the pipeline outputs
        for tag, ftype in stage.outputs:
            # Record the expected output for this tag
            step.out.append(tag)

            # Also record that each file is an output to the entire pipeline
            cwl_out = WorkflowOutputParameter(
                tag,
                outputSource=f"{step.id}/{tag}",
                type="File",
                format=ftype.__name__,
            )
            workflow.outputs.append(cwl_out)

        # Also capture stdout and stderr as outputs
        cwl_out = WorkflowOutputParameter(
            f"{step.id}@stdout",
            outputSource=f"{step.id}/{step.id}@stdout",
            label="stdout",
            type="File",
        )
        step.out.append(f"{step.id}@stdout")
        workflow.outputs.append(cwl_out)

        cwl_out = WorkflowOutputParameter(
            f"{step.id}@stderr",
            outputSource=f"{step.id}/{step.id}@stderr",
            type="File",
        )
        step.out.append(f"{step.id}@stderr")
        workflow.outputs.append(cwl_out)

        # This step is now ready - add it to the list
        workflow.steps.append(step)

        # In CWL our data elemnts dict just records which step each
        # output is made in
        return {tag: step.id for tag in stage.output_tags()}

    def run_jobs(self):
        workflow = self.run_info["workflow"]
        cwl_dir = self.run_info["cwl_dir"]
        output_dir = self.run_config["output_dir"]
        log_dir = self.run_config["log_dir"]
        inputs_file = self.run_info["inputs_file"]

        with open(f"{cwl_dir}/pipeline.cwl", "w") as f:
            yaml.dump(workflow.save(), f)

        # If 'launcher' is defined, use that
        launcher = self.launcher_config.get(
            "launch",
            f"cwltool --outdir {output_dir} " "--preserve-environment PYTHONPATH",
        )
        if launcher == "cwltool":  # pragma: no cover
            launcher = (
                f"cwltool --outdir {output_dir} " "--preserve-environment PYTHONPATH"
            )

        if launcher:
            # need to include the CWD on the path for CWL as it
            # runs in an isolated directory
            pypath = os.environ.get("PYTHONPATH", "")
            os.environ["PYTHONPATH"] = pypath + ":" + os.getcwd()
            cmd = f"{launcher} {cwl_dir}/pipeline.cwl {inputs_file}"
            print(cmd)
            status = os.system(cmd)
            if pypath:  # pragma: no cover
                os.environ["PYTHONPATH"] = pypath
            else:
                del os.environ["PYTHONPATH"]

        # Parsl insists on putting everything in the same output directory,
        # both logs and file outputs.
        # we need to move those

        if status == 0:
            for step in self.run_info["workflow"].steps:
                shutil.move(f"{output_dir}/{step.id}.out", f"{log_dir}/{step.id}.out")
                shutil.move(f"{output_dir}/{step.id}.err", f"{log_dir}/{step.id}.err")

        return status
