from ..sites import get_default_site
from ..stage import PipelineStage

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
