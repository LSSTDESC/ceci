import parsl
import pathlib
import sys

SERIAL = 'serial'
MPI_PARALLEL = 'mpi'

class PipelineStage:
    """A PipelineStage implements a single calculation step within a wider pipeline.

    Each different type of analysis stge is represented by a subclass of this
    base class.  The base class handles the connection between different pipeline
    stages, and the execution of the stages within a workflow system (parsl),
    potentially in parallel (MPI).

    See documentation pages for more details.

    """
    parallel = True
    config_options = {}

    def __init__(self, args):
        args = vars(args)
        self._inputs = {}
        self._outputs = {}
        self._configs = {}
        missing_inputs = []
        missing_outputs = []
        missing_configs =[]
        # if the input contains a configuration file, we don't raise the alarm
        # just yet and will try to read missing configuration from file
        if 'config' not in self.input_tags():
            for x in self.config_options:
                val = args[x]
                if (val is None) and (config_options[x] is None):
                    missing_configs.append(f'--{x}')
        for x in self.input_tags():
            val = args[x]
            if val is None:
                missing_inputs.append(f'--{x}')
        for x in self.output_tags():
            val = args[x]
            if val is None:
                missing_outputs.append(f'--{x}')
        if missing_inputs or missing_outputs:
            missing_configs = '  '.join(missing_configs)
            missing_inputs = '  '.join(missing_inputs)
            missing_outputs = '  '.join(missing_outputs)
            raise ValueError(f"""

Missing these names on the command line:
    Config names: {missing_configs}
    Input names: {missing_inputs}
    Output names: {missing_outputs}""")

        self._inputs = {x:args[x] for x in self.input_tags()}
        self._outputs = {x:args[x] for x in self.output_tags()}
        self._configs = {x:args[x] if x in args else self.config_options[x] for x in self.config_options}
        if 'config' in self.input_tags():
            self._read_config()

        if args.get('mpi', False):
            import mpi4py.MPI
            self._parallel = MPI_PARALLEL
            self._comm = mpi4py.MPI.COMM_WORLD
            self._size = self._comm.Get_size()
            self._rank = self._comm.Get_rank()
        else:
            self._parallel = SERIAL
            self._comm = None
            self._size = 1
            self._rank = 0

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

    def get_input(self, tag):
        """Return the path of an input file with the given tag"""
        return self._inputs[tag]

    def get_output(self, tag):
        """Return the path of an output file with the given tag"""
        return self._outputs[tag]

    def open_input(self, tag, wrapper=False, **kwargs):
        """
        Find and open an input file with the given tag, in read-only mode.

        For general files this will simply return a standard
        python file object.

        For specialized file types like FITS or HDF5 it will return
        a more specific object - see the types.py file for more info.

        """
        path = self.get_input(tag)
        input_class = self.get_input_type(tag)
        obj = input_class(path, 'r', **kwargs)

        if wrapper:
            return obj
        else:
            return obj.file

    def open_output(self, tag, wrapper=False, **kwargs):
        """
        Find and open an output file with the given tag, in write mode.

        For general files this will simply return a standard
        python file object.

        For specialized file types like FITS or HDF5 it will return
        a more specific object - see the types.py file for more info.

        """
        path = self.get_output(tag)
        output_class = self.get_output_type(tag)

        # HDF files can be opened for parallel writing
        # under MPI.  This checks if:
        # - we have been told to open in parallel
        # - we are actually running under MPI
        # and adds the flags required if all these are true
        run_parallel = kwargs.pop('parallel', False) and self.is_mpi()
        if run_parallel:
            kwargs['driver'] = 'mpio'
            kwargs['comm'] = self.comm

        # Return an opened object representing the file
        obj = output_class(path, 'w', **kwargs)
        if wrapper:
            return obj
        else:
            return obj.file

    @property
    def config(self):
        """
        Returns the configuration directory for this stage, aggregating command
        line option and optional configuration file.
        """
        return self._configs

    def _read_config(self):
        """
        Read the file that has the tag "config".
        Find the section within that file with the same name as the stage,
        and return the contents.

        This method also checks that any options in the "config_options"
        dictionary that the class can define are present in the config.
        If they are not, then if they have a default value set there (not None)
        then this value is filled in instead.
        """
        import yaml
        input_config = yaml.load(open(self.get_input('config')))
        my_config = input_config[self.name]
        for opt, default in self._configs.items():
            if opt not in my_config:
                if default is None:
                    raise ValueError(f"Missing configuration option {opt} for stage {self.name}")
                my_config[opt] = default
        return my_config

    @classmethod
    def generate_cwl(cls):
        """
        Produces a CWL App object which can then be exported to yaml
        """
        import cwlgen
        module = cls.get_module()
        # Basic definition of the tool
        cwl_tool = cwlgen.CommandLineTool(tool_id=cls.name,
                                          label=cls.name,
                                          base_command=f'python3 -m {module}')

        #TODO: Add documentation in ceci elements
        cwl_tool.doc = "Pipeline element from ceci"

        # Add the inputs of the tool
        for i,inp in enumerate(cls.input_tags()):
            input_binding = cwlgen.CommandLineBinding(position=(i+1))
            input_param   = cwlgen.CommandInputParameter(inp,
                                                         param_type='File',
                                                         input_binding=input_binding,
                                                         doc='Some documentation about the input')
            cwl_tool.inputs.append(input_param)

        # Add the definition of the outputs
        for i,out in enumerate(cls.output_tags()):
            output_binding = cwlgen.CommandOutputBinding(glob=out)
            output = cwlgen.CommandOutputParameter(out, param_type='File',
                                            output_binding=output_binding,
                                            param_format='http://edamontology.org/format_2330',
                                            doc='Some results produced by the pipeline element')
            cwl_tool.outputs.append(output)

        # Potentially add more metadata
        metadata = {'name': cls.name,
                'about': 'I let you guess',
                'publication': [{'id': 'one_doi'}, {'id': 'another_doi'}],
                'license': ['MIT']}
        cwl_tool.metadata = cwlgen.Metadata(**metadata)

        return cwl_tool

    def iterate_fits(self, tag, hdunum, cols, chunk_rows):
        """
        Loop through chunks of the input data from a FITS file with the given tag
        """
        fits = self.open_input(tag)
        ext = fits[hdunum]
        n = ext.get_nrows()
        for start,end in self.data_ranges_by_rank(n, chunk_rows):
            data = ext.read_columns(cols, rows=range(start, end))
            yield start, end, data

    def iterate_hdf(self, tag, group_name, cols, chunk_rows):
        """
        Loop through chunks of the input data from an HDF5 file with the given tag.

        All the selected columns must have the same length.

        """
        import numpy as np
        hdf = self.open_input(tag)
        group = hdf[group_name]

        # Check all the columns are the same length
        N = [len(group[col]) for col in cols]
        n = N[0]
        if not np.equal(N,n).all():
            raise ValueError(f"Different columns among {cols} in file {tag}\
            group {group_name} are different sizes - cannot use iterate_hdf")

        # Iterate through the data providing chunks
        for start, end in self.data_ranges_by_rank(n, chunk_rows):
            data = {col: group[col][start:end] for col in cols}
            yield start, end, data




    def get_input_type(self, tag):
        """Return the file type class of an input file with the given tag."""
        for t,dt in self.inputs:
            if t==tag:
                return dt
        raise ValueError(f"Tag {tag} is not a known input")

    def get_output_type(self, tag):
        """Return the file type class of an output file with the given tag."""
        for t,dt in self.outputs:
            if t==tag:
                return dt
        raise ValueError(f"Tag {tag} is not a known output")



    def split_tasks_by_rank(self, n_chunks):
        for i in range(n_chunks):
            if i%self.size==self.rank:
                yield i

    def data_ranges_by_rank(self, n_rows, chunk_rows):
        n_chunks = n_rows//chunk_rows
        if n_chunks*chunk_rows<n_rows:
            n_chunks += 1
        for i in self.split_tasks_by_rank(n_chunks):
            start = i*chunk_rows
            end = min((i+1)*chunk_rows, n_rows)
            yield start, end



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
            raise ValueError(f"Pipeline stage {cls.name} already defined")

        # Find the absolute path to the class defining the file
        path = pathlib.Path(filename).resolve()
        cls.pipeline_stages[cls.name] = (cls, path)

    @classmethod
    def output_tags(cls):
        """
        Return the list of output tags required by this stage
        """
        return [tag for tag,_ in cls.outputs]

    @classmethod
    def input_tags(cls):
        """
        Return the list of input tags required by this stage
        """
        return [tag for tag,_ in cls.inputs]

    @classmethod
    def get_stage(cls, name):
        """
        Return the PipelineStage subclass with the given name.
        """
        return cls.pipeline_stages[name][0]

    @classmethod
    def get_executable(cls):
        """
        Return the path to the executable code for this pipeline stage.
        """
        return cls.pipeline_stages[cls.name][1]

    @classmethod
    def get_module(cls):
        """
        Return the path to the executable code for this pipeline stage.
        """
        return cls.pipeline_stages[cls.name][0].__module__


    @classmethod
    def main(cls):
        """
        Create an instance of this stage and run it with
        inputs and outputs taken from the command line
        """
        stage_name = sys.argv[1]
        stage = cls.get_stage(stage_name)
        args = stage._parse_command_line()
        stage.execute(args)

    @classmethod
    def export(cls, all_stages=True):
        """
        Export a dictionary representation of known stages
        (if all==True, the default) or just this stage (all==False).


        """

    @classmethod
    def export_yaml(cls, filename, all_stages=True):
        """
        Export a YAML file representation of known stages
        (if all==True, the default) or just this stage (all==False).
        """
        outfile = open(filename, 'w')
        d = cls.export(all_stages=all_stages)
        yaml.dump(d, outfile)
        outfile.close()


    @classmethod
    def _parse_command_line(cls):
        cmd = " ".join(sys.argv[:])
        print(f"Executing stage: {cls.name}")
        import argparse
        parser = argparse.ArgumentParser(description="Run a stage or something")
        parser.add_argument("stage_name")
        for conf in cls.config_options:
            parser.add_argument(f'--{conf}')
        for inp in cls.input_tags():
            parser.add_argument(f'--{inp}')
        for out in cls.output_tags():
            parser.add_argument(f'--{out}')
        parser.add_argument('--mpi', action='store_true', help="Set up MPI parallelism")
        parser.add_argument('--pdb', action='store_true', help="Run under the python debugger")
        args = parser.parse_args()
        return args

    @classmethod
    def execute(cls, args):
        """
        Create an instance of this stage and run it
        with the specified inputs and outputs
        """
        stage = cls(args)
        try:
            stage.run()
        except Exception as error:
            if args.pdb:
                print("There was an exception - starting python debugger because you ran with --pdb")
                print(error)
                pdb.post_mortem()
            else:
                raise



    @classmethod
    def _generate(cls, template, dfk):
        # dfk needs to be an argument here because it is
        # referenced in the template that is exec'd.
        d = locals().copy()
        exec(template, globals(), d)
        function = d[cls.name]
        return function


    @classmethod
    def generate(cls, dfk, nprocess, config, log_dir, mpi_command='mpirun -n'):
        """
        Build a parsl bash app that executes this pipeline stage
        """
        module = cls.get_module()
        module = module.split('.')[0]

        flags = [cls.name]
        # Adds non default options to the command line
        if config is not None:
            for opt in config:
                flag = '--{}={}'.format(opt, config[opt])
                flags.append(flag)
        for i,inp in enumerate(cls.input_tags()):
            flag = '--{}={{inputs[{}]}}'.format(inp,i)
            flags.append(flag)

        for i,out in enumerate(cls.output_tags()):
            flag = '--{}={{outputs[{}]}}'.format(out,i)
            flags.append(flag)
        flags = "   ".join(flags)

        # Parallelism - simple for now
        if nprocess > 1:
            launcher = f"{mpi_command} {nprocess}"
            mpi_flag = "--mpi"
        else:
            launcher = ""
            mpi_flag = ""

        template = f"""
@parsl.App('bash', dfk)
def {cls.name}(inputs, outputs, stdout='{log_dir}/{cls.name}.out', stderr='{log_dir}/{cls.name}.err'):
    cmd = '{launcher} python3 -m {module} {flags} {mpi_flag}'.format(inputs=inputs,outputs=outputs)
    print("Compiling command:")
    print(cmd)
    return cmd
"""
        return cls._generate(template, dfk)
