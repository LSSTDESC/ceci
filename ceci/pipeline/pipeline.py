import os
import sys
import contextlib
import networkx
import jinja2.meta
import yaml
from abc import abstractmethod

from ..stage import PipelineStage
from ..sites import load, set_default_site, get_default_site
from ..utils import extra_paths
from .graph import build_graph, get_static_ordering, trim_pipeline_graph
from .file_manager import FileManager
from .sec import StageExecutionConfig
from .templates import read_and_apply_template


RESUME_MODE_RESUME = "resume"
RESUME_MODE_RESTART = "restart"
RESUME_MODE_REFUSE = "refuse"



@contextlib.contextmanager
def prepare_for_pipeline(pipe_config):
    """
    Prepare the paths and launcher needed to read and run a pipeline.
    """

    # Later we will add these paths to sys.path for running here,
    # but we will also need to pass them to the sites below so that
    # they can be added within any containers or other launchers
    # that we use
    paths = pipe_config.get("python_paths", [])
    if isinstance(paths, str):  # pragma: no cover
        paths = paths.split()

    # Get information (maybe the default) on the launcher we may be using
    launcher_config = pipe_config.setdefault("launcher", {"name": "mini"})
    site_config = pipe_config.get("site", {"name": "local"})

    # Pass the paths along to the site
    site_config["python_paths"] = paths
    load(launcher_config, [site_config])

    # Python modules in which to search for pipeline stages
    modules = pipe_config.get("modules", "").split()

    # This helps with testing
    default_site = get_default_site()

    # temporarily add the paths to sys.path,
    # but remove them at the end
    with extra_paths(paths):

        # Import modules. We have to do this because the definitions
        # of the stages can be inside.
        for module in modules:
            __import__(module)

        try:
            yield
        finally:
            set_default_site(default_site)



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
        self.graph = None
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
        from .parsl import ParslPipeline
        from .mini import MiniPipeline
        from .flow_chart import FlowChartPipeline
        from .dry_run import DryRunPipeline

        with prepare_for_pipeline(pipe_config):
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
                "to": pipe_config.get("to", None),
                "from": pipe_config.get("from", None),
            }

            if run_config["resume"] is True:
                run_config["resume"] = RESUME_MODE_RESUME
            elif run_config["resume"] is False:
                run_config["resume"] = RESUME_MODE_RESTART

            launcher_dict = dict(parsl=ParslPipeline, mini=MiniPipeline)

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
        from .mini import MiniPipeline
        launcher_config = dict(name="mini", interval=0.5)
        return MiniPipeline([], launcher_config)

    @staticmethod
    def build_config(
        pipeline_config_filename, extra_config=None, dry_run=False, flow_chart=None,
        template_parameters=None,
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
        parameters: dict
            A dictionary of parameters that are used to complete template
            variables in the pipeline configuration file.

        Returns
        -------
        pipe_config : dict
            The resulting configuration
        """
        # Read the configuration file, expanding any jinja2 template variables
        config_text = read_and_apply_template(pipeline_config_filename, template_parameters)

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
    def read(cls, pipeline_config_filename, extra_config=None, dry_run=False, template_parameters=None):
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
        pipe_config = cls.build_config(pipeline_config_filename, extra_config, dry_run, template_parameters)
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

        pipeline = cls.create(pipe_config)

        return pipeline

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
        sec.build_stage_class()
        self.stage_execution_config[sec.name] = sec
        self.stage_names.append(sec.name)

        # If we are adding a pre-built stage then we insert the output files now.
        # Otherwise this happens later.  Why does this happen now? Probably so that
        # subsequent stages can refer to the outputs of this stage when constructed
        # interactively.
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


    def configure_stages(self, stages_config):
        """Initialize and configure the stages in the pipeline

        Returns
        -------
        stages : list[PipelineStage]
        """
        # First read all the configuration information for the stages,
        # and do some minor pre-processing.
        self.read_stages_config(stages_config)

        stages = []
        all_inputs = self.overall_inputs.copy()
        for stage_name in self.stage_names:
            # Find the stage class and execution config
            sec = self.stage_execution_config[stage_name]
            stage_aliases = sec.aliases
            stage_class = sec.stage_class

            # In interactive use the stage object might already be created
            # otherwise we need to create it
            if sec.stage_obj is not None:
                orig_stage_config = self.stage_config_data.get(stage_name, {}).copy()
                stage = sec.stage_obj
                stage.config.update(**orig_stage_config)
            else:
                # Find the inputs for this stage and set up the arguments
                # to the stage init method
                args = self.stage_config_data.get(stage_name, {})
                for tag in stage_class.input_tags():
                    aliased_tag = stage_aliases.get(tag, tag)
                    args[aliased_tag] = all_inputs[aliased_tag]
                args["config"] = self.stages_config

                # Make the stage object and find the outputs,
                # keeping track of them for the next stages
                stage = sec.build_stage_object(args)
                stage_outputs = stage.find_outputs(".")
                for tag in stage_class.output_tags():
                    aliased_tag = stage_aliases.get(tag, tag)
                    all_inputs[aliased_tag] = stage_outputs[aliased_tag]

            stages.append(stage)
        return stages

    
    def construct_pipeline_graph(self, overall_inputs, run_config):
        """
        Connect together the pipeline stages, finding all the inputs
        and outputs for each. Build the graph object that encodes the
        pipeline structure and use it to determine a linear ordering in
        which to run the pipeline.

        This method is run automatically when creating a pipeline,
        or calling the `initialize` method.

        Parameters
        ---------
        overall_inputs : `Mapping`
            A mapping from tag to path for all of the overall inputs needed by this pipeline
        run_config : `Mapping`
            Configuration parameters for how to run the pipeline

        """

        # Make copies, since we may be be modifying these
        self.run_config = run_config.copy()

        # Set up paths to our overall input files
        registry_info = self.run_config.get("registry")
        if (registry_info is not None) and (self.data_registry is None):
            self.data_registry = self.setup_data_registry(registry_info)
        self.overall_inputs = self.process_overall_inputs(overall_inputs)

        # Get the stages in the order we need.
        # First build the graph that includes both the stages and the
        # input and output files for the pipeline.
        self.graph = build_graph(
            self.stage_names, 
            [self.stage_execution_config[name].stage_class for name in self.stage_names],
            [self.stage_execution_config[name].aliases for name in self.stage_names],
            self.overall_inputs
        )

        if "to" in self.run_config or "from" in self.run_config:
            to_ = self.run_config.get("to")
            from_ = self.run_config.get("from")

            self.graph, converted_inputs = trim_pipeline_graph(self.graph, from_, to_)

            # converted_inputs contains the file names that were previously
            # outputs from the pipeline but are now converted to being inputs
            # to it. We need them to exist already if this is not a dry-run.
            output_dir = self.run_config["output_dir"]
            converted_inputs = {
                tag: os.path.join(output_dir, path)
                for tag,path in converted_inputs.items()
            }
            if converted_inputs:
                print("The pipeline was trimmed using the 'to' and/or 'from' options " \
                      "in the pipeline file.\n" \
                      "The following files were to be generated in the full pipeline" \
                      "but are now expected to exist already from a previous run")
                for t, path in converted_inputs.items():
                    print(f"    - {t}: {path}")
                    self.overall_inputs[t] = path


        # Re-order the pipeline stages in a static order
        self.stage_names = get_static_ordering(self.graph)

        # This is also a convenient place to record the location
        # of the overall inputs in the file manager
        self.pipeline_files.insert_paths(self.overall_inputs)


        return self.stage_names

    def read_stages_config(self, stages_config):
        """Read the configuration for the individual stages
        
        This also copies the global configuration into each stage
        
        Parameters
        ----------
        stages_config : str or None
            The path to the file with the stage configuration, or None if no
            configuration is provided.  If None, then the default configuration
            is used for each stage.
        """

        # Now we configure the individual stages
        self.stages_config = stages_config

        if isinstance(self.stages_config, str):
            with open(self.stages_config) as stage_config_file:
                self.stage_config_data = yaml.safe_load(stage_config_file)
        elif isinstance(self.stages_config, dict):
            # In interactive mode we may have a dictionary with the configuration
            # information for each stage pre-read.
            self.stage_config_data = self.stages_config
        elif self.stages_config is None:
            # There may be no information on the stages if all the defaults are
            # being used
            self.stage_config_data = {}
        else:
            raise ValueError("stages_config must be a filename, a dict, or None")

        # Copy the global configuration into each stage separately.
        self.global_config = self.stage_config_data.pop("global", {}) 
        for v in self.stage_config_data.values():
            v.update(self.global_config)

    def enqueue_jobs(self):
        """
        Queue up all the pipeline to be run.
        """
        # make sure output directories exist
        os.makedirs(self.run_config["output_dir"], exist_ok=True)
        os.makedirs(self.run_config["log_dir"], exist_ok=True)

        # Initiate the run.
        # This is an implementation detail for the different subclasses to store
        # necessary information about the run if necessary.
        # Usually, the arguments are ignored, but they are provided in case a class needs to
        # do something special with any of them.
        self.run_info = self.initiate_run(self.overall_inputs)

        # Now we can queue up all the jobs. The subclass decides what this actually means.
        for stage in self.stages:
            # If we are in "resume" mode and the pipeline has already been run
            # then we re-use any existing outputs.  User is responsible for making
            # sure they are complete!

            if self.should_skip_stage(stage):
                stage.already_finished()
                self.pipeline_files.insert_outputs(stage, self.run_config["output_dir"])

            # Otherwise, run the pipeline and register any outputs from the
            # pipe element.
            else:
                stage_outputs = self.enqueue_job(stage, self.pipeline_files)
                self.pipeline_files.insert_paths(stage_outputs)
        

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
        # This first part sets up the pipeline structure, figuring
        # out how each stage connects to the others
        self.construct_pipeline_graph(overall_inputs, run_config)

        # Configure the individual stages. Reads the config information for them
        # and creates PipelineStage objects for each one.
        self.stages = self.configure_stages(stages_config)

        # Tell the subclass to preapre to run the pipeline by queueing up
        # the jobs. This does a few other miscellaneous things too.
        self.enqueue_jobs()

        return self.run_info, self.run_config

    def run(self):
        """Run the pipeline are return the execution status"""
        status = self.run_jobs()
        # When the pipeline completes we collect all the outputs
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
        pipe_info_list = []
        if self.run_config is not None:
            pipe_dict.update(**self.run_config)

        # Prepare the configuration file for the stages.
        # For this to work the pipeline stages must have been configured.
        if stagefile is None:
            stagefile = os.path.splitext(pipefile)[0] + "_config.yml"
        pipe_dict["config"] = stagefile
        stage_dict = {}
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

    def make_flow_chart(self, filename=None):
        """Make a flow chart of the pipeline stages and write it to a file

        Parameters
        ----------
        filename: str or None
            The name of the file to write the flow chart to.  If it ends in .dot
            then a dot file is written, otherwise it should be an image format.
            If None then the flow chart is not written to a file, but the graphvix
            object is still returned.

        Returns
        -------
        graphviz.AGraph
            The graphviz object representing the pipeline stages
        """
        # generate a graphviz object from the networkx graph
        graph = networkx.nx_agraph.to_agraph(self.graph)

        # set the colours and styles for the boxes
        for node in graph.nodes_iter():
            node_type = node.attr['type']
            if node_type == "input":
                node.attr.update(shape="box", color="gold", style="filled")
            elif node_type == "stage":
                node.attr.update(shape="ellipse", color="orangered", style="filled")
            elif node_type == "output":
                node.attr.update(shape="box", color="skyblue", style="filled")

        # optionally output the stage to file
        if filename is None:
            pass
        elif filename.endswith(".dot"):
            graph.write(filename)
        else:
            graph.draw(filename, prog="dot")

        return graph

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
    


