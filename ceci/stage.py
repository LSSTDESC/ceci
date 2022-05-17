"""Module with core functionality for a single pipeline stage """

import pathlib
import os
import sys
from textwrap import dedent
import shutil
import cProfile
import pdb
import datetime

from abc import abstractmethod
from . import errors
from .monitor import MemoryMonitor
from .config import StageConfig, cast_to_streamable

SERIAL = "serial"
MPI_PARALLEL = "mpi"
DASK_PARALLEL = "dask"

IN_PROGRESS_PREFIX = "inprogress_"


class PipelineStage:
    """A PipelineStage implements a single calculation step within a wider pipeline.

    Each different type of analysis stage is represented by a subclass of this
    base class.  The base class handles the connection between different pipeline
    stages, and the execution of the stages within a workflow system (parsl),
    potentially in parallel (MPI).

    An instance of one of these classes represents an actual run of the stage,
    with the required inputs, outputs, and configuration specified.

    See documentation pages for more details.

    """

    parallel = True
    dask_parallel = False
    config_options = {}
    doc = ""
    allow_reload = False

    def __init__(self, args, comm=None):
        """Construct a pipeline stage, specifying the inputs, outputs, and configuration for it.

        The constructor needs a dict or namespace. It should include:
        - input paths (required)
        - config path (required)
        - output paths (optional but usual)
        - additional configuration (required if not specified elsewhere)

        Input and output paths should map tags to paths.
        Tags are strings, and the first elements in each item in the subclass's
        "inputs" and "output" attributes.
        e.g. for a subclass with:
            inputs = [('eggs', TextFile)]
            outputs = [('spam', TextFile)]
        the args could contain:
            {'eggs': 'inputs/eggs.txt',
             'spam': 'outputs/spam.txt' }
        If spam is not specified it will default to "./spam.txt"

        }

        The config should map "config" to a path where a YAML config file
        is located, e.g. {'config':'/path/to/config.yml'}

        Any config variables that are specified in the class's config attribute
        will be searched for first in args, then in the config file, and then
        by looking at any default value they have been given.
        If they have no default value (and just a type, like int, is listed), then
        it's an error if they are not specified somewhere.

        The execute method can instantiate and run the class together, with added bonuses
        like profiling and debugging tools.

        Parameters
        ----------
        args: dict or namespace
            Specification of input and output paths and any missing config options
        comm: MPI communicator
            (default is None) An MPI comm object to use in preference to COMM_WORLD
        """
        self._configs = StageConfig(**self.config_options)
        self._inputs = None
        self._outputs = None
        self._parallel = SERIAL
        self._comm = None
        self._size = 1
        self._rank = 0
        self._io_checked = False
        self.dask_client = None

        self.load_configs(args)
        if comm is not None:
            self.setup_mpi(comm)

    @classmethod
    def make_stage(cls, **kwargs):
        """Make a stage of a particular type"""
        kwcopy = kwargs.copy()
        kwcopy.setdefault("config", None)
        comm = kwcopy.pop("comm", None)
        name = kwcopy.get("name", None)
        aliases = kwcopy.pop("aliases", {})
        for input_ in cls.inputs:
            kwcopy.setdefault(input_[0], "None")
        if name is not None:
            for output_ in cls.outputs:  # pylint: disable=no-member
                outtag = output_[0]
                aliases[outtag] = f"{outtag}_{name}"
        kwcopy["aliases"] = aliases
        return cls(kwcopy, comm=comm)

    def get_aliases(self):
        """Returns the dictionary of aliases used to remap inputs and outputs
        in the case that we want to have multiple instance of this class in the pipeline"""
        return self.config.get("aliases", None)

    def get_aliased_tag(self, tag):
        """Returns the possibly remapped value for an input or output tag

        Parameter
        ---------
        tag : `str`
            The input or output tag we are checking

        Returns
        -------
        aliased_tag : `str`
            The aliases version of the tag
        """
        aliases = self.get_aliases()
        if aliases is None:
            return tag
        return aliases.get(tag, tag)

    @abstractmethod
    def run(self):  # pragma: no cover
        """Run the stage and return the execution status"""
        raise NotImplementedError("run")

    def load_configs(self, args):
        """
        Load the configuraiton

        Parameters
        ----------
        args: dict or namespace
            Specification of input and output paths and any missing config options
        """
        if not isinstance(args, dict):
            args = vars(args)

        # We alwys assume the config arg exists, whether it is in input_tags or not
        if "config" not in args:  # pragma: no cover
            raise ValueError("The argument --config was missing on the command line.")

        _name = args.get("name")
        if _name is not None:
            self._configs.name = _name

        # First, we extract configuration information from a combination of
        # command line arguments and optional 'config' file
        self._inputs = dict(config=args["config"])
        self.read_config(args)
        self.check_io(args)

    def check_io(self, args=None):
        """
        Check the inputs and outputs.
        This function is seperate so that when Stages are configured interactively after
        construction then can invove this

        Parameters
        ----------
        args: dict or namespace
            Specification of input and output paths and any missing config options
        """

        # We first check for missing input files, that's a show stopper
        if self._io_checked:  # pragma: no cover
            return
        if args is None:  # pragma: no cover
            args = self.config

        missing_inputs = []
        for x in self.input_tags():
            val = args.get(x)
            aliased_tag = self.get_aliased_tag(x)

            if val is None:
                val = args.get(aliased_tag)

            if val is None:  # pragma: no cover
                missing_inputs.append(f"--{x}")
            else:
                self._inputs[aliased_tag] = val
        if missing_inputs:  # pragma: no cover
            missing_inputs = "  ".join(missing_inputs)
            raise ValueError(
                f"""

{self.instance_name} Missing these names on the command line:
    Input names: {missing_inputs}"""
            )

        # We prefer to receive explicit filenames for the outputs but will
        # tolerate missing output filenames and will default to tag name in
        # current folder (this is for CWL compliance)
        self._outputs = {}
        for i, x in enumerate(self.output_tags()):
            aliased_tag = self.get_aliased_tag(x)
            if args.get(x) is None:
                ftype = self.outputs[i][1]  # pylint: disable=no-member
                self._outputs[aliased_tag] = ftype.make_name(aliased_tag)
            else:
                self._outputs[aliased_tag] = args[x]
        self._io_checked = True

    def setup_mpi(self, comm=None):
        """
        Setup the MPI interface

        Parameters
        ----------
        comm: MPI communicator
            (default is None) An MPI comm object to use in preference to COMM_WORLD
        """
        mpi = self.config.get("mpi", False)

        if mpi:  # pragma: no cover
            try:
                # This isn't a ceci dependency, so give a sensible error message if not installed.
                import mpi4py.MPI
            except ImportError:
                print("ERROR: Using --mpi option requires mpi4py to be installed.")
                raise

        # For scripting and testing we allow an MPI communicator or anything
        # with the same API to be passed in directly, overriding the --mpi
        # flag.
        if comm is not None:
            self._parallel = MPI_PARALLEL
            self._comm = comm
            self._size = self._comm.Get_size()
            self._rank = self._comm.Get_rank()
        elif mpi:  # pragma: no cover
            self._parallel = MPI_PARALLEL
            self._comm = mpi4py.MPI.COMM_WORLD
            self._size = self._comm.Get_size()
            self._rank = self._comm.Get_rank()
        else:
            self._parallel = SERIAL
            self._comm = None
            self._size = 1
            self._rank = 0

        # If we are running under MPI but this subclass has enabled dask
        # then we note that here. It stops various MPI-specific things happening
        # later
        if (self._parallel == MPI_PARALLEL) and self.dask_parallel:
            self._parallel = DASK_PARALLEL

    pipeline_stages = {}
    incomplete_pipeline_stages = {}

    def __init_subclass__(cls, **kwargs):
        """
        Python 3.6+ provides a facility to automatically
        call a method (this one) whenever a new subclass
        is defined.  In this case we use that feature to keep
        track of all available pipeline stages, each of which is
        defined by a class.

        """
        super().__init_subclass__(**kwargs)

        # This is a hacky way of finding the file
        # where our stage was defined
        filename = sys.modules[cls.__module__].__file__

        stage_is_complete = (
            hasattr(cls, "inputs")
            and hasattr(cls, "outputs")
            and not getattr(cls.run, "__isabstractmethod__", False)
        )

        # If there isn't an explicit name already then set it here.
        # by default use the class name.
        if not hasattr(cls, "name"):  # pragma: no cover
            cls.name = cls.__name__
        if cls.name is None:  # pragma: no cover
            cls.name = cls.__name__

        if stage_is_complete:
            # Deal with duplicated class names
            if cls.name in cls.pipeline_stages and not cls.allow_reload:
                other = cls.pipeline_stages[cls.name][1]
                raise errors.DuplicateStageName(
                    "You created two pipeline stages with the"
                    f"name {cls.name}.\nOne was in {filename}\nand the "
                    f"other in {other}\nYou can either change the class "
                    "name or explicitly put a variable 'name' in the top"
                    "level of the class."
                )

            # Check for "config" in the inputs list - this is implicit
            for name, _ in cls.inputs:
                if name == "config":
                    raise errors.ReservedNameError(
                        "An input called 'config' is implicit in each pipeline "
                        "stage and should not be added explicitly.  Please update "
                        f"your pipeline stage called {cls.name} to remove/rename "
                        "the input called 'config'."
                    )

        # Check if user has over-written the config variable.
        # Quite a common error I make myself.
        if not isinstance(cls.config, property):
            raise errors.ReservedNameError(
                "You have a class variable called 'config', which "
                "is reserved in ceci for its own configuration. "
                "You may have meant to specify config_options?"
            )
        # Find the absolute path to the class defining the file
        path = pathlib.Path(filename).resolve()

        # Register the class
        if stage_is_complete:
            cls.pipeline_stages[cls.name] = (cls, path)
        else:
            cls.incomplete_pipeline_stages[cls.__name__] = (cls, path)

    #############################################
    # Life cycle-related methods and properties.
    #############################################

    @classmethod
    def get_stage(cls, name):
        """
        Return the PipelineStage subclass with the given name.

        This is used so that we do not need a new entry point __main__ function
        for each new stage - instead we can just use a single one which can query
        which class it should be using based on the name.

        Returns
        -------
        cls: class
            The corresponding subclass
        """
        stage = cls.pipeline_stages.get(name)

        # If not found, then check for incomplete stages
        if stage is None:
            if name in cls.incomplete_pipeline_stages:
                raise errors.IncompleteStage(
                    f"The stage {name} is not completely written. "
                    "Stages must specify 'inputs', 'outputs' as class variables "
                    f"and a 'run' method.\n{name} might be unfinished, or it might "
                    "be intended as a base for other classes and not to be run."
                )
            raise errors.StageNotFound(f"Unknown stage '{name}'")
        return stage[0]

    @classmethod
    def get_module(cls):
        """
        Return the path to the python package containing the current sub-class

        If we have a PipelineStage subclass defined in a module called "bar", in
        a package called "foo" e.g.:
        /path/to/foo/bar.py  <--   contains subclass "Baz"

        Then calling Baz.get_module() will return "foo.bar".

        We use this later to construct command lines like "python -m foo Baz"

        Returns
        -------
        module: str
            The module containing this class.
        """
        return cls.pipeline_stages[cls.name][0].__module__

    @classmethod
    def usage(cls):  # pragma: no cover
        """
        Print a usage message.
        """
        names = []
        docs = []
        for name, (stage, _) in cls.pipeline_stages.items():
            # find the first non-empty doc line, if there is one.
            try:
                doc_lines = [s.strip() for s in stage.__doc__.split("\n")]
                doc_lines = [d for d in doc_lines if d]
                doc = doc_lines[0]
            except (AttributeError, IndexError):
                doc = ""
            # cut off any very long lines
            if len(doc) > 100:
                doc = doc[:100] + " ..."
            # print the text
            names.append(name)
            docs.append(doc)

        # Make it look like a nice table by finding the maximum
        # length of the names, so that all the docs line up
        n = max(len(name) for name in names) + 1
        stage_texts = [f"- {name:{n}} - {d}" for name, d in zip(names, docs)]
        stage_text = "\n".join(stage_texts)

        try:
            module = cls.get_module().split(".")[0]
        except:  # pylint: disable=bare-except
            module = "<module_name>"
        sys.stderr.write(
            f"""
Usage: python -m {module} <stage_name> <stage_arguments>

If no stage_arguments are given then usage information
for the chosen stage will be given.

I currently know about these stages:

{stage_text}
"""
        )

    @classmethod
    def main(cls):
        """
        Create an instance of this stage and execute it with
        inputs and outputs taken from the command line
        """
        try:
            stage_name = sys.argv[1]
        except IndexError:  # pragma: no cover
            cls.usage()
            return 1
        if stage_name in ["--help", "-h"] and len(sys.argv) == 2:  # pragma: no cover
            cls.usage()
            return 1
        stage = cls.get_stage(stage_name)
        args = stage.parse_command_line()
        stage.execute(args)
        return 0

    @classmethod
    def parse_command_line(cls, cmd=None):
        """Set up and argument parser and parse the command line

        Parameters
        ----------
        cmd : str or None
            The command line to part (if None this will use the system arguments)

        Returns
        -------
        args : Namespace
            The resulting Mapping of arguement to values
        """
        import argparse

        parser = argparse.ArgumentParser(description=f"Run pipeline stage {cls.name}")
        parser.add_argument("stage_name")
        for conf, def_val in cls.config_options.items():
            opt_type = def_val if isinstance(def_val, type) else type(def_val)

            if opt_type == bool:
                parser.add_argument(f"--{conf}", action="store_const", const=True)
                parser.add_argument(
                    f"--no-{conf}", dest=conf, action="store_const", const=False
                )
            elif opt_type == list:
                out_type = (
                    def_val[0] if isinstance(def_val[0], type) else type(def_val[0])
                )
                if out_type is str:  # pragma: no cover
                    parser.add_argument(
                        f"--{conf}", type=lambda string: string.split(",")
                    )
                elif out_type is int:  # pragma: no cover
                    parser.add_argument(
                        f"--{conf}",
                        type=lambda string: [int(i) for i in string.split(",")],
                    )
                elif out_type is float:
                    parser.add_argument(
                        f"--{conf}",
                        type=lambda string: [float(i) for i in string.split(",")],
                    )
                else:  # pragma: no cover
                    raise NotImplementedError(
                        "Only handles str, int and float list arguments"
                    )
            else:  # pragma: no cover
                parser.add_argument(f"--{conf}", type=opt_type)
        for inp in cls.input_tags():
            parser.add_argument(f"--{inp}")
        for out in cls.output_tags():
            parser.add_argument(f"--{out}")
        parser.add_argument(
            "--name",
            action="store",
            default=cls.name,
            type=str,
            help="Rename the stage",
        )
        parser.add_argument("--config")

        if cls.parallel:
            parser.add_argument(
                "--mpi", action="store_true", help="Set up MPI parallelism"
            )
        parser.add_argument(
            "--pdb", action="store_true", help="Run under the python debugger"
        )
        parser.add_argument(
            "--cprofile",
            action="store",
            default="",
            type=str,
            help="Profile the stage using the python cProfile tool",
        )
        parser.add_argument(
            "--memmon",
            type=int,
            default=0,
            help="Report memory use. Argument gives interval in seconds between reports",
        )

        if cmd is None:
            ret_args = parser.parse_args()
        else:
            ret_args = parser.parse_args(cmd)

        return ret_args

    @classmethod
    def execute(cls, args, comm=None):
        """
        Create an instance of this stage and run it
        with the specified inputs and outputs.

        This is calld by the main method.

        Parameters
        ----------
        args: namespace
            The argparse namespace for this subclass.
        """

        # Create the stage instance.  Running under dask this only
        # actually needs to happen for one process, but it's not a major
        # overhead and lets us do a whole bunch of other setup above
        stage = cls(args)
        stage.setup_mpi(comm)

        # This happens before dask is initialized
        start_time = datetime.datetime.now()
        if stage.rank == 0:
            start_time_text = start_time.isoformat(" ")
            print(f"Executing stage: {cls.name} @ {start_time_text}")

        if stage.is_dask():
            is_client = stage.start_dask()
            # worker and scheduler stages do not execute the
            # run method under dask
            if not is_client:
                return

        if args.cprofile:  # pragma: no cover
            profile = cProfile.Profile()
            profile.enable()

        if args.memmon:  # pragma: no cover
            monitor = MemoryMonitor.start_in_thread(interval=args.memmon)

        try:
            stage.run()
        except Exception as error:  # pragma: no cover
            if args.pdb:
                print(
                    "There was an exception - starting python debugger because you ran with --pdb"
                )
                print(error)
                pdb.post_mortem()
            else:
                if stage.rank == 0:
                    end_time = datetime.datetime.now()
                    end_time_text = end_time.isoformat(" ")
                    minutes = (end_time - start_time).total_seconds() / 60
                    print(
                        f"Stage failed: {cls.name} @ {end_time_text} after {minutes:.2f} minutes"
                    )
                raise
        finally:
            if args.memmon:  # pragma: no cover
                monitor.stop()
            if stage.is_dask():
                stage.stop_dask()

        # The default finalization renames any output files to their
        # final location, but subclasses can override to do other things too
        try:
            stage.finalize()
        except Exception as error:  # pragma: no cover
            if args.pdb:
                print(
                    "There was an exception in the finalization - starting python debugger because you ran with --pdb"
                )
                print(error)
                pdb.post_mortem()
            else:
                raise
        if args.cprofile:  # pragma: no cover
            profile.disable()
            profile.dump_stats(args.cprofile)
            profile.print_stats("cumtime")

        # Under dask the
        # the root process has gone off to become the scheduler,
        # and process 1 becomes the client which runs this code
        # and gets to this point
        if stage.rank == 0 or stage.is_dask():
            end_time = datetime.datetime.now()
            end_time_text = end_time.isoformat(" ")
            minutes = (end_time - start_time).total_seconds() / 60
            print(
                f"Stage complete: {cls.name} @ {end_time_text} took {minutes:.2f} minutes"
            )

    def finalize(self):
        """Finalize the stage, moving all its outputs to their final locations."""
        # Synchronize files so that everything is closed
        if self.is_mpi():  # pragma: no cover
            self.comm.Barrier()

        # Move files to their final path
        # Only the root process moves things, except under dask it is
        # process 1, which is the only process that reaches this point
        # (as noted above)
        if (self.rank == 0) or self.is_dask():
            for tag in self.output_tags():
                # find the old and new names
                self._finalize_tag(tag)

    def _finalize_tag(self, tag):
        """Finalize the data for a particular tag.

        This can be overridden by sub-classes for more complicated behavior
        """
        aliased_tag = self.get_aliased_tag(tag)
        temp_name = self.get_output(aliased_tag)
        final_name = self.get_output(aliased_tag, final_name=True)

        # it's not an error here if the path does not exist,
        # because that will be handled later.
        if pathlib.Path(temp_name).exists():
            # replace directories, rather than nesting more results
            if pathlib.Path(final_name).is_dir():  # pragma: no cover
                shutil.rmtree(final_name)
            shutil.move(temp_name, final_name)
        else:  # pragma: no cover
            sys.stderr.write(
                f"NOTE/WARNING: Expected output file {final_name} was not generated.\n"
            )
        return final_name

    #############################################
    # Parallelism-related methods and properties.
    #############################################
    @property
    def rank(self):
        """The rank of this process under MPI (0 if not running under MPI)"""
        return self._rank

    @property
    def size(self):
        """The number or processes under MPI (1 if not running under MPI)"""
        return self._size

    @property
    def comm(self):
        """The MPI communicator object (None if not running under MPI)"""
        return self._comm

    def is_parallel(self):
        """
        Returns True if the code is being run in parallel.
        Right now is_parallel() will return the same value as is_mpi(),
        but that may change in future if we implement other forms of
        parallelization.
        """
        return self._parallel != SERIAL

    def is_mpi(self):
        """
        Returns True if the stage is being run under MPI.
        """
        return self._parallel == MPI_PARALLEL

    def is_dask(self):
        """
        Returns True if the stage is being run in parallel with Dask.
        """
        return self._parallel == DASK_PARALLEL

    def start_dask(self):
        """
        Prepare dask to run under MPI. After calling this method
        only a single process, MPI rank 1 will continue to exeute code
        """

        # using the programmatic dask configuration system
        # does not seem to work. Presumably the loggers have already
        # been created by the time we modify the config. Doing it with
        # env vars seems to work. If the user has already set this then
        # we use that value. Otherwise we only want error logs
        key = "DASK_LOGGING__DISTRIBUTED"
        os.environ[key] = os.environ.get(key, "error")
        try:
            import dask
            import dask_mpi
            import dask.distributed
        except ImportError:  # pragma: no cover
            print(
                "ERROR: Using --mpi option on stages that use dask requires "
                "dask[distributed] and dask_mpi to be installed."
            )
            raise

        if self.size < 3:  # pragma: no cover
            raise ValueError(
                "Dask requires at least three processes. One becomes a scheduler "
                "process, one is a client that runs the code, and more are required "
                "as worker processes."
            )

        # This requires my fork until/unless they merge the PR, to allow
        # us to pass in these two arguments. In vanilla dask-mpi sys.exit
        # is called at the end of the event loop without returning to us.
        # After this point only a single process, MPI rank 1,
        # should continue to exeute code. The others enter an event
        # loop and return with is_client=False, which we return here
        # to tell the caller that they should not run everything.
        is_client = dask_mpi.initialize(comm=self.comm, exit=False)

        if is_client:
            # Connect this local process to remote workers.
            self.dask_client = dask.distributed.Client()
            # I don't yet know how to see this dashboard link at nersc
            print(f"Started dask. Diagnostics at {self.dask_client.dashboard_link}")

        return is_client

    @staticmethod
    def stop_dask():
        """
        End the dask event loop
        """
        from dask_mpi import send_close_signal

        send_close_signal()

    def split_tasks_by_rank(self, tasks):
        """Iterate through a list of items, yielding ones this process is responsible for/

        Tasks are allocated in a round-robin way.

        Parameters
        ----------
        tasks: iterable
            Tasks to split up

        """
        for i, task in enumerate(tasks):
            if i % self.size == self.rank:
                yield task

    def data_ranges_by_rank(self, n_rows, chunk_rows, parallel=True):
        """Split a number of rows by process.

        Given a total number of rows to read and a chunk size, yield
        the ranges within them that this process should handle.

        Parameters
        ----------
        n_rows: int
            Total number of rows to split up

        chunk_rows: int
            Size of each chunk to be read.

        Parallel: bool
            Whether to split data by rank or just give all procs all data.
            Default=True
        """
        n_chunks = n_rows // chunk_rows
        if n_chunks * chunk_rows < n_rows:  # pragma: no cover
            n_chunks += 1
        if parallel:
            it = self.split_tasks_by_rank(range(n_chunks))
        else:
            it = range(n_chunks)
        for i in it:
            start = i * chunk_rows
            end = min((i + 1) * chunk_rows, n_rows)
            yield start, end

    ##################################################
    # Input and output-related methods and properties.
    ##################################################

    def get_input(self, tag):
        """Return the path of an input file with the given tag"""
        return self._inputs[tag]

    def get_output(self, tag, final_name=False):
        """Return the path of an output file with the given tag

        If final_name is False then use a temporary name - file will
        be moved to its final name at the end
        """
        path = self._outputs[tag]

        # If not the final version, add a tag at the start of the filename
        if not final_name:
            p = pathlib.Path(path)
            p = p.parent / (IN_PROGRESS_PREFIX + p.name)
            path = str(p)
        return path

    def open_input(self, tag, wrapper=False, **kwargs):
        """
        Find and open an input file with the given tag, in read-only mode.

        For general files this will simply return a standard
        python file object.

        For specialized file types like FITS or HDF5 it will return
        a more specific object - see the types.py file for more info.

        """
        aliased_tag = self.get_aliased_tag(tag)
        path = self.get_input(aliased_tag)
        input_class = self.get_input_type(tag)
        obj = input_class(path, "r", **kwargs)

        if wrapper:  # pragma: no cover
            return obj
        return obj.file

    def open_output(
        self, tag, wrapper=False, final_name=False, **kwargs
    ):  # pragma: no cover
        """
        Find and open an output file with the given tag, in write mode.

        If final_name is True then they will be opened using their final
        target output name.  Otherwise we will prepend "inprogress_" to their
        file name. This means we know that if the final file exists then it
        is completed.

        If wrapper is True this will return an instance of the class
        of the file as specified in the cls.outputs.  Otherwise it will
        return an open file object (standard python one or something more
        specialized).

        Parameters
        ----------

        tag: str
            Tag as listed in self.outputs

        wrapper: bool
            Default=False.  Whether to return a wrapped file

        final_name: bool
            Default=False. Whether to save to

        **kwargs:
            Extra args are passed on to the file's class constructor.

        """
        aliased_tag = self.get_aliased_tag(tag)
        path = self.get_output(aliased_tag, final_name=final_name)
        output_class = self.get_output_type(tag)

        # HDF files can be opened for parallel writing
        # under MPI.  This checks if:
        # - we have been told to open in parallel
        # - we are actually running under MPI
        # and adds the flags required if all these are true
        run_parallel = kwargs.pop("parallel", False) and self.is_mpi()
        if run_parallel:
            kwargs["driver"] = "mpio"
            kwargs["comm"] = self.comm

            # XXX: This is also not a dependency, but it should be.
            #      Or even better would be to make it a dependency of descformats where it
            #      is actually used.
            import h5py

            if not h5py.get_config().mpi:
                print(
                    dedent(
                        """\
                Your h5py installation is not MPI-enabled.
                Options include:
                  1) Set nprocess to 1 for all stages
                  2) Upgrade h5py to use mpi.  See instructions here:
                     http://docs.h5py.org/en/latest/build.html#custom-installation
                Note: If using conda, the most straightforward way is to enable it is
                    conda install -c spectraldns h5py-parallel
                """
                    )
                )
                raise RuntimeError("h5py module is not MPI-enabled.")

        # Return an opened object representing the file
        obj = output_class(path, "w", **kwargs)
        if wrapper:
            return obj
        return obj.file

    @classmethod
    def inputs_(cls):
        """
        Return the dict of inputs
        """
        return cls.inputs  # pylint: disable=no-member

    @classmethod
    def outputs_(cls):
        """
        Return the dict of inputs
        """
        return cls.outputs  # pylint: disable=no-member

    @classmethod
    def output_tags(cls):
        """
        Return the list of output tags required by this stage
        """
        return [tag for tag, _ in cls.outputs_()]

    @classmethod
    def input_tags(cls):
        """
        Return the list of input tags required by this stage
        """
        return [tag for tag, _ in cls.inputs_()]

    def get_input_type(self, tag):
        """Return the file type class of an input file with the given tag."""
        for t, dt in self.inputs_():
            if t == tag:
                return dt
        raise ValueError(f"Tag {tag} is not a known input")  # pragma: no cover

    def get_output_type(self, tag):
        """Return the file type class of an output file with the given tag."""
        for t, dt in self.outputs_():
            if t == tag:
                return dt
        raise ValueError(f"Tag {tag} is not a known output")  # pragma: no cover

    ##################################################
    # Configuration-related methods and properties.
    ##################################################

    @property
    def instance_name(self):
        """Return the name associated to this particular instance of this stage"""
        return self._configs.get("name", self.name)

    @property
    def config(self):
        """
        Returns the configuration dictionary for this stage, aggregating command
        line options and optional configuration file.
        """
        return self._configs

    def read_config(self, args):
        """
        This function looks for the arguments of the pipeline stage using a
        combination of default values, command line options and separate
        configuration file.

        The order for resolving config options is first looking for a default
        value, then looking for a

        In case a mandatory argument (argument with no default) is missing,
        an exception is raised.

        Note that we recognize arguments with no default as the ones where
        self.config_options holds a type instead of a value.
        """
        # Try to load configuration file if provided
        import yaml

        config_file = self.get_input("config")

        # This is all the config information in the file, including
        # things for other stages
        if config_file is not None:
            with open(config_file) as _config_file:
                overall_config = yaml.safe_load(_config_file)
        else:
            overall_config = {}

        # The user can define global options that are inherited by
        # all the other sections if not already specified there.
        input_config = overall_config.get("global", {})

        # This is just the config info in the file for this stage.
        # It may be incomplete - there may be things specified on the
        # command line instead, or just using their default values
        stage_config = overall_config.get(self.instance_name, {})
        input_config.update(stage_config)

        self._configs.set_config(input_config, args)

    def get_config_dict(self, ignore=None, reduce_config=False):
        """Write the current configuration to a dict

        Parameters
        ----------
        ignore : dict or None
            Global parameters not to write
        reduce_config : bool
            If true, reduce the configuration by parsing out the inputs, outputs and global params

        Returns
        -------
        out_dict : dict
            The configuration
        """
        out_dict = {}
        if reduce_config:
            ignore_keys = self.input_tags() + self.output_tags() + ["config"]
        else:
            ignore_keys = []
        ignore = ignore or {}
        for key, val in self.config.items():
            if reduce_config:
                if key in ignore:
                    if ignore[key] == val:
                        continue
                if key in ignore_keys:
                    continue
            out_dict[key] = cast_to_streamable(val)
        return out_dict

    def find_inputs(self, pipeline_files):
        """Find and retrun all the inputs associated to this stage in the FileManager

        These are returned as a dictionary of tag : path pairs
        """
        ret_dict = {}
        for tag, _ in self.inputs_():
            aliased_tag = self.get_aliased_tag(tag)
            ret_dict[aliased_tag] = pipeline_files[aliased_tag]
        return ret_dict

    def find_outputs(self, outdir):
        """Find and retrun all the outputs associated to this stage

        These are returned as a dictionary of tag : path pairs
        """
        ret_dict = {}
        for tag, ftype in self.outputs_():
            aliased_tag = self.get_aliased_tag(tag)
            if not aliased_tag in self._outputs.keys(): # pragma: no cover
                self._outputs[aliased_tag]=ftype.make_name(aliased_tag)
            ret_dict[aliased_tag] = f"{outdir}/{self._outputs[aliased_tag]}"
        return ret_dict

    def print_io(self, stream=sys.stdout):
        """Print out the tags, paths and types for all the inputs and outputs of this stage"""
        stream.write("Inputs--------\n")
        for tag, ftype in self.inputs_():
            aliased_tag = self.get_aliased_tag(tag)
            stream.write(
                f"{tag:20} : {aliased_tag:20} :{str(ftype):20} : {self._inputs[tag]}\n"
            )
        stream.write("Outputs--------\n")
        for tag, ftype in self.outputs_():
            aliased_tag = self.get_aliased_tag(tag)
            stream.write(
                f"{tag:20} : {aliased_tag:20} :{str(ftype):20} : {self._outputs[aliased_tag]}\n"
            )

    def should_skip(self, run_config):
        """Return true if we should skip a stage b/c it's outputs already exist and we are in resume mode"""
        outputs = self.find_outputs(run_config["output_dir"]).values()
        already_run_stage = all(os.path.exists(output) for output in outputs)
        return already_run_stage and run_config["resume"]

    def already_finished(self):
        """Print a warning that a stage is being skipped"""
        print(f"Skipping stage {self.instance_name} because its outputs exist already")

    def iterate_fits(
        self, tag, hdunum, cols, chunk_rows, parallel=True
    ):  # pragma: no cover
        """
        Loop through chunks of the input data from a FITS file with the given tag

        TODO: add ceci tests of this functions
        Parameters
        ----------
        tag: str
            The tag from the inputs list to use

        hdunum: int
            The extension number to read

        cols: list
            The columns to read

        chunk_rows: int
            Number of columns to read and return at once

        parallel: bool
            Whether to split up data among processes (parallel=True) or give
            all processes all data (parallel=False).  Default = True.

        Returns
        -------
        it: iterator
            Iterator yielding (int, int, array) tuples of (start, end, data)
            data is a structured array.
        """
        fits = self.open_input(tag)
        ext = fits[hdunum]
        n = ext.get_nrows()
        for start, end in self.data_ranges_by_rank(n, chunk_rows, parallel=parallel):
            data = ext.read_columns(cols, rows=range(start, end))
            yield start, end, data

    def iterate_hdf(
        self, tag, group_name, cols, chunk_rows, parallel=True, longest=False
    ):
        """
        Loop through chunks of the input data from an HDF5 file with the given tag.

        All the selected columns must have the same length.

        Parameters
        ----------
        tag: str
            The tag from the inputs list to use

        group: str
            The group within the HDF5 file to use, looked up as
            file[group]

        cols: list
            The columns to read

        chunk_rows: int
            Number of columns to read and return at once

        parallel: bool
            Whether to split up data among processes (parallel=True) or give
            all processes all data (parallel=False).  Default = True.

        longest: bool
            Whether to allow mixed length arrays and keep going until the longest
            array is completed, returning empty arrays for shorter ones


        Returns
        -------
        it: iterator
            Iterator yielding (int, int, dict) tuples of (start, end, data)
        """
        import numpy as np

        hdf = self.open_input(tag)
        group = hdf[group_name]

        # Check all the columns are the same length
        N = [len(group[col]) for col in cols]
        n = max(N)
        if not longest:
            if not np.equal(N, n).all():
                raise ValueError(
                    f"Different columns among {cols} in file {tag} group {group_name}"
                    "are different sizes - if this is acceptable set longest=True"
                )

        # Iterate through the data providing chunks
        for start, end in self.data_ranges_by_rank(n, chunk_rows, parallel=parallel):
            data = {col: group[col][start:end] for col in cols}
            yield start, end, data

    ################################
    # Pipeline-related methods
    ################################

    @classmethod
    def generate_command(
        cls, inputs, config, outputs, aliases=None, instance_name=None
    ):
        """
        Generate a command line that will run the stage
        """
        module = cls.get_module()
        module = module.split(".")[0]

        flags = [cls.name]
        aliases = aliases or {}

        for tag, _ in cls.inputs_():
            aliased_tag = aliases.get(tag, tag)
            try:
                fpath = inputs[aliased_tag]
            except KeyError as msg:  # pragma: no cover
                raise ValueError(
                    f"Missing input location {aliased_tag} {str(inputs)}"
                ) from msg
            flags.append(f"--{tag}={fpath}")

        if instance_name is not None and instance_name != cls.name:
            flags.append(f"--name={instance_name}")

        flags.append(f"--config={config}")

        for tag, _ in cls.outputs_():
            aliased_tag = aliases.get(tag, tag)
            try:
                fpath = outputs[aliased_tag]
            except KeyError as msg:  # pragma: no cover
                raise ValueError(
                    f"Missing output location {aliased_tag} {str(outputs)}"
                ) from msg
            flags.append(f"--{tag}={fpath}")

        flags = "   ".join(flags)

        # We just return this, instead of wrapping it in a
        # parsl job
        cmd = f"python3 -m {module} {flags}"
        return cmd

    @classmethod
    def generate_cwl(cls, log_dir=None):
        """
        Produces a CWL App object which can then be exported to yaml
        """
        import cwlgen

        module = cls.get_module()
        module = module.split(".")[0]

        # Basic definition of the tool
        cwl_tool = cwlgen.CommandLineTool(
            tool_id=cls.name,
            label=cls.name,
            base_command="python3",
            cwl_version="v1.0",
            doc=cls.__doc__,
        )
        if log_dir is not None:
            cwl_tool.stdout = f"{cls.name}.out"
            cwl_tool.stderr = f"{cls.name}.err"

        # Adds the first input binding with the name of the module and pipeline stage
        input_arg = cwlgen.CommandLineBinding(position=-1, value_from=f"-m{module}")
        cwl_tool.arguments.append(input_arg)
        input_arg = cwlgen.CommandLineBinding(position=0, value_from=f"{cls.name}")
        cwl_tool.arguments.append(input_arg)

        type_dict = {int: "int", float: "float", str: "string", bool: "boolean"}
        # Adds the parameters of the tool
        for opt, def_val in cls.config_options.items():

            # Handles special case of lists:
            if isinstance(def_val, list):
                v = def_val[0]
                param_type = {
                    "type": "array",
                    "items": type_dict[v]
                    if isinstance(v, type)
                    else type_dict[type(v)],
                }
                default = def_val if not isinstance(v, type) else None
                input_binding = cwlgen.CommandLineBinding(
                    prefix=f"--{opt}=", item_separator=",", separate=False
                )
            else:
                param_type = (
                    type_dict[def_val]
                    if isinstance(def_val, type)
                    else type_dict[type(def_val)]
                )
                default = def_val if not isinstance(def_val, type) else None
                if param_type == "boolean":
                    input_binding = cwlgen.CommandLineBinding(prefix=f"--{opt}")
                else:  # pragma: no cover
                    input_binding = cwlgen.CommandLineBinding(
                        prefix=f"--{opt}=", separate=False
                    )

            input_param = cwlgen.CommandInputParameter(
                opt,
                label=opt,
                param_type=param_type,
                input_binding=input_binding,
                default=default,
                doc="Some documentation about this parameter",
            )

            # We are bypassing the cwlgen builtin type check for the special case
            # of arrays until that gets added to the standard
            if isinstance(def_val, list):
                input_param.type = param_type

            cwl_tool.inputs.append(input_param)

        # Add the inputs of the tool
        for i, inp in enumerate(cls.input_tags()):
            input_binding = cwlgen.CommandLineBinding(prefix=f"--{inp}")
            input_param = cwlgen.CommandInputParameter(
                inp,
                label=inp,
                param_type="File",
                param_format=cls.inputs[i][1].format,  # pylint: disable=no-member
                input_binding=input_binding,
                doc="Some documentation about the input",
            )
            cwl_tool.inputs.append(input_param)

        # Adds the overall configuration file
        input_binding = cwlgen.CommandLineBinding(prefix="--config")
        input_param = cwlgen.CommandInputParameter(
            "config",
            label="config",
            param_type="File",
            param_format="http://edamontology.org/format_3750",
            input_binding=input_binding,
            doc="Configuration file",
        )
        cwl_tool.inputs.append(input_param)

        # Add the definition of the outputs
        for i, out in enumerate(cls.output_tags()):
            output_name = cls.outputs[i][1].make_name(out)  # pylint: disable=no-member
            output_binding = cwlgen.CommandOutputBinding(glob=output_name)
            output = cwlgen.CommandOutputParameter(
                out,
                label=out,
                param_type="File",
                output_binding=output_binding,
                param_format=cls.outputs[i][1].format,  # pylint: disable=no-member
                doc="Some results produced by the pipeline element",
            )
            cwl_tool.outputs.append(output)

        if log_dir is not None:
            output = cwlgen.CommandOutputParameter(
                f"{cls.name}@stdout",
                label="stdout",
                param_type="stdout",
                doc="Pipeline elements standard output",
            )
            cwl_tool.outputs.append(output)
            error = cwlgen.CommandOutputParameter(
                f"{cls.name}@stderr",
                label="stderr",
                param_type="stderr",
                doc="Pipeline elements standard output",
            )
            cwl_tool.outputs.append(error)

        # Potentially add more metadata
        # This requires a schema however...
        # metadata = {'name': cls.name,
        #         'about': 'Some additional info',
        #         'publication': [{'id': 'one_doi'}, {'id': 'another_doi'}],
        #         'license': ['MIT']}
        # cwl_tool.metadata = cwlgen.Metadata(**metadata)

        return cwl_tool
