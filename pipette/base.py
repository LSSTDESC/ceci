import parsl
from parsl.data_provider.files import File

import pathlib
import sys

SERIAL = 'serial'
MPI_PARALLEL = 'mpi'

class PipelineStage:
    # By default all stages are assumed to be able to be run
    # in parallel
    parallel = True
    def __init__(self, args):
        args = vars(args)
        self._inputs = {x:args[x] for x in self.inputs}
        self._outputs = {x:args[x] for x in self.outputs}
        self.memory_limit = args['mem']
        self._setup_parallelism(args)

    def _setup_parallelism(self, args):
        if args['mpi']:
            import mpi4py.MPI
            self._parallel = MPI_PARALLEL
            self._comm = mpi4py.MPI.COMM_WORLD
        else:
            self._parallel = SERIAL


    def iter_fits_chunks(self, hdr, cols):
        n = hdr.get_nrows()
        ncol = len(cols)
        meg_per_row = ncol * 8 * 1000 * 1000.0
        self.ncore = self.get_nprocess()
        nrow_max = self.memory_limit / meg_per_row
        

    def get_nprocess(self):
        if self._parallel == MPI_PARALLEL:
            comm = self.get_communicator()
            return comm.Get_size()
        else:
            return 1

    def get_communicator(self):
        if self._parallel == MPI_PARALLEL:
            return self._comm
        else:
            raise ValueError("Not running in MPI mode")

    def is_mpi(self):
        return self._parallel == MPI_PARALLEL

    def get_input(self, tag):
        return self._inputs[tag]

    def get_output(self, tag):
        return self._outputs[tag]

    pipeline_stages = {}
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

        # Make sure the pipeline stage has a name
        if not hasattr(cls, 'name'):
            raise ValueError(f"Pipeline stage defined in {filename} must be given a name.")
        if not hasattr(cls, 'outputs'):
            raise ValueError(f"Pipeline stage {cls.name} defined in {filename} must be given a list of outputs.")
        if not hasattr(cls, 'inputs'):
            raise ValueError(f"Pipeline stage {cls.name} defined in {filename} must be given a list of inputs.")

        # Check for two stages with the same name.
        # Not sure if we actually do want to allow this?
        if cls.name in cls.pipeline_stages:
            raise ValueError(f"Pipeline stage {name} already defined")
        
        # Find the absolute path to the class defining the file
        path = pathlib.Path(filename).resolve()
        cls.pipeline_stages[cls.name] = (cls, path)

    @classmethod
    def get_stage(cls, name):
        return cls.pipeline_stages[name][0]
    
    @classmethod
    def get_executable(cls):
        """
        Return the path to the executable code for this pipeline stage.
        """
        return cls.pipeline_stages[cls.name][1]

    @classmethod
    def main(cls):
        """
        Create an instance of this stage and run it with 
        inputs and outputs taken from the command line
        """
        stage_name = sys.argv[1]
        stage = cls.get_stage(stage_name)
        args = stage._parse_command_line()
        print(f"running stage: {stage.name}")
        stage.execute(args)

    @classmethod
    def _parse_command_line(cls):
        cmd = " ".join(sys.argv[:])
        print(f"Executing command line: {cmd}")
        import argparse
        parser = argparse.ArgumentParser(description="Run a stage or something")
        parser.add_argument("stage_name")
        for inp in cls.inputs:
            parser.add_argument('--{}'.format(inp))
        for out in cls.outputs:
            parser.add_argument('--{}'.format(out))
        parser.add_argument('--mpi', action='store_true', help="Set up MPI parallelism")
        parser.add_argument('--mem', type=float, default=2.0, help="Max size of data to read in GB")
        args = parser.parse_args()
        return args

    @classmethod
    def execute(cls, args):
        """
        Create an instance of this stage and run it 
        with the specified inputs and outputs
        """
        stage = cls(args)
        stage.run()

    @classmethod
    def _generate(cls, template, dfk):
        # dfk needs to be an argument here because it is
        # referenced in the template that is exec'd.
        d = locals().copy()
        exec(template, globals(), d)
        function = d['function']
        function.__name__ = cls.name
        return function


    @classmethod
    def parsl_outputs(cls, future):
        outputs = {tag: future[i] for i,tag in enumerate(self.outputs)}


    @classmethod
    def generate(cls, dfk, launcher=None, nprocess=0):
        """
        Build a parsl bash app that executes this pipeline stage
        """
        path = cls.get_executable()

        flags = [cls.name]
        for i,inp in enumerate(cls.inputs):
            flag = '--{}={{inputs[{}]}}'.format(inp,i)
            flags.append(flag)
        for i,out in enumerate(cls.outputs):
            flag = '--{}={{outputs[{}]}}'.format(out,i)
            flags.append(flag)
        flags = "   ".join(flags)

        # Parallelism - simple for now
        if launcher is None:
            launcher = ""
            mpi_flag = ""
        else:
            launcher = f"mpirun -n {nprocess}"
            mpi_flag = "--mpi"

        template = f"""
@parsl.App('bash', dfk)
def function(inputs, outputs):
    return '{launcher} python3 {path} {mpi_flag} {flags}'.format(inputs=inputs,outputs=outputs)
"""
        return cls._generate(template, dfk)

    @classmethod
    def find_inputs(cls, data_elements):
        inputs = []
        for inp in cls.inputs:
            item = data_elements[inp]
            if isinstance(item,str):
                item = File(item)
            inputs.append(item)
        return inputs
