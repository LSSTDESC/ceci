import parsl
import pathlib
import sys
from textwrap import dedent

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
    doc=""

    def __init__(self, args):
        if not isinstance(args, dict):
            args = vars(args)

        # We first check for missing input files, that's a show stopper
        missing_inputs = []
        for x in self.input_tags():
            val = args[x]
            if val is None:
                missing_inputs.append(f'--{x}')
        if missing_inputs:
            missing_inputs = '  '.join(missing_inputs)
            raise ValueError(f"""

Missing these names on the command line:
    Input names: {missing_inputs}""")


        self._inputs = {x:args[x] for x in self.input_tags()}
        # We alwys assume the config arg exists, whether it is in input_tags or not
        self._inputs["config"] = args['config']

        # We prefer to receive explicit filenames for the outputs but will
        # tolerate missing output filenames and will default to tag name in
        # current folder (this is for CWL compliance)
        self._outputs = {x:args[x] if args[x] is not None else f'{x}.{self.outputs[i][1].suffix}' for i,x in enumerate(self.output_tags())}

        # Finally, we extract configuration information from a combination of
        # command line arguments and optional 'config' file
        self._configs = self.read_config(args)

        use_mpi = args.get('mpi', False)
        if use_mpi:
            try:
                # This isn't a ceci dependency, so give a sensible error message if not installed.
                import mpi4py.MPI
            except ImportError:
                print('ERROR: Using --mpi option requires mpi4py to be installed.')
                raise

        if use_mpi:
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

            # XXX: This is also not a dependency, but it should be.
            #      Or even better would be to make it a dependency of descformats where it
            #      is actually used.
            import h5py
            if not h5py.get_config().mpi:
                print(dedent("""\
                Your h5py installation is not MPI-enabled.
                Options include:
                  1) Set nprocess to 1 for all stages
                  2) Upgrade h5py to use mpi.  See instructions here:
                     http://docs.h5py.org/en/latest/build.html#custom-installation
                Note: If using conda, the most straightforward way is to enable it is
                    conda install -c spectraldns h5py-parallel
                """))
                raise RuntimeError("h5py module is not MPI-enabled.")

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

        # This is all the config information in the file, including
        # things for other stages
        overall_config = yaml.load(open(self.get_input('config')))

        # The user can define global options that are inherited by
        # all the other sections if not already specified there.
        input_config = overall_config.get('global', {})

        # This is just the config info in the file for this stage.
        # It may be incomplete - there may be things specified on the
        # command line instead, or just using their default values
        stage_config = overall_config.get(self.name, {})
        input_config.update(stage_config)


        # Here we build up the actual configuration we use on this
        # run from all these sources
        my_config = {}

        # Loop over the options of the pipeline stage
        for x in self.config_options:
            opt = None
            opt_type = None

            # First look for a default value,
            # if a type (like int) is provided as the default it indicates that
            # this option doesn't have a default (i.e. is mandatory) and should
            # be explicitly provided with the specified type
            if type(self.config_options[x]) is type:
                opt_type = self.config_options[x]

            elif type(self.config_options[x]) is list:
                v = self.config_options[x][0]
                if type(v) is type:
                    opt_type = v
                else:
                    opt = self.config_options[x]
                    opt_type = type(v)
            else:
                opt = self.config_options[x]
                opt_type = type(opt)

            # Second, look for the option in the configuration file and override
            # default if provided TODO: Check types
            if x in input_config:
                opt = input_config[x]

            # Finally check for command line option that would override the value
            # in the configuration file. Note that the argument parser should
            # already have taken care of type
            if args[x] is not None:
                opt = args[x]

            # Finally, check that we got at least some value for this option
            if opt is None:
                raise ValueError(f"Missing configuration option {x} for stage {self.name}")

            my_config[x] = opt

        # Unspecified parameters can also be copied over.
        # This will be needed for parameters that are more complicated, such
        # as dictionaries or other more structured parameter information.
        for x,val in input_config.items():
            # Omit things we've already dealt with above
            if x in self.config_options:
                continue
            # copy over everything else
            else:
                my_config[x] = val



        return my_config

    @classmethod
    def generate_cwl(cls):
        """
        Produces a CWL App object which can then be exported to yaml
        """
        import cwlgen
        module = cls.get_module()
        module = module.split('.')[0]

        # Basic definition of the tool
        cwl_tool = cwlgen.CommandLineTool(tool_id=cls.name,
                                          label=cls.name,
                                          base_command='python3',
                                          cwl_version='v1.0',
                                          doc=cls.__doc__)

        # Adds the first input binding with the name of the module and pipeline stage
        input_arg = cwlgen.CommandLineBinding(position=-1, value_from=f'-m{module}')
        cwl_tool.arguments.append(input_arg)
        input_arg = cwlgen.CommandLineBinding(position=0, value_from=f'{cls.name}')
        cwl_tool.arguments.append(input_arg)

        type_dict={int: 'int', float:'float', str:'string', bool:'boolean'}
        # Adds the parameters of the tool
        for opt in cls.config_options:
            def_val = cls.config_options[opt]

            # Handles special case of lists:
            if type(def_val) is list:
                v = def_val[0]
                param_type = {'type':'array', 'items': type_dict[v] if type(v) == type else type_dict[type(v)] }
                default = def_val if type(v) != type else None
                input_binding = cwlgen.CommandLineBinding(prefix='--{}='.format(opt), item_separator=',', separate=False)
            else:
                param_type=type_dict[def_val] if type(def_val) == type else type_dict[type(def_val)]
                default=def_val if type(def_val) != type else None
                if param_type is 'boolean':
                    input_binding = cwlgen.CommandLineBinding(prefix='--{}'.format(opt))
                else:
                    input_binding = cwlgen.CommandLineBinding(prefix='--{}='.format(opt), separate=False)

            input_param = cwlgen.CommandInputParameter(opt,
                                                       label=opt,
                                                       param_type=param_type,
                                                       input_binding=input_binding,
                                                       default=default,
                                                       doc='Some documentation about this parameter')

            # We are bypassing the cwlgen builtin type check for the special case
            # of arrays until that gets added to the standard
            if type(def_val) is list:
                input_param.type = param_type

            cwl_tool.inputs.append(input_param)


        # Add the inputs of the tool
        for i,inp in enumerate(cls.input_tags()):
            input_binding = cwlgen.CommandLineBinding(prefix='--{}'.format(inp))
            input_param   = cwlgen.CommandInputParameter(inp,
                                                         label=inp,
                                                         param_type='File',
                                                         param_format=cls.inputs[i][1].__name__,
                                                         input_binding=input_binding,
                                                         doc='Some documentation about the input')
            cwl_tool.inputs.append(input_param)

        # Adds the overall configuration file
        input_binding = cwlgen.CommandLineBinding(prefix='--config')
        input_param   = cwlgen.CommandInputParameter('config',
                                                     label='config',
                                                     param_type='File',
                                                     param_format='YamlFile',
                                                     input_binding=input_binding,
                                                     doc='Configuration file')
        cwl_tool.inputs.append(input_param)


        # Add the definition of the outputs
        for i,out in enumerate(cls.output_tags()):
            output_name = f'{out}.{cls.outputs[i][1].suffix}'
            output_binding = cwlgen.CommandOutputBinding(glob=output_name)
            output = cwlgen.CommandOutputParameter(out,
                                            label=out,
                                            param_type='File',
                                            output_binding=output_binding,
                                            param_format=cls.outputs[i][1].__name__,
                                            doc='Some results produced by the pipeline element')
            cwl_tool.outputs.append(output)

        # Potentially add more metadata
        # This requires a schema however...
        # metadata = {'name': cls.name,
        #         'about': 'Some additional info',
        #         'publication': [{'id': 'one_doi'}, {'id': 'another_doi'}],
        #         'license': ['MIT']}
        # cwl_tool.metadata = cwlgen.Metadata(**metadata)

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



    def split_tasks_by_rank(self, tasks):
        for i,task in enumerate(tasks):
            if i%self.size==self.rank:
                yield task

    def data_ranges_by_rank(self, n_rows, chunk_rows):
        n_chunks = n_rows//chunk_rows
        if n_chunks*chunk_rows<n_rows:
            n_chunks += 1
        for i in self.split_tasks_by_rank(range(n_chunks)):
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

        # Check for "config" in the inputs list - this is now implicit
        for name, _ in cls.inputs:
            if name=='config':
                raise ValueError(f"""An input called 'config' is now implicit in each pipeline stage
and should not be added explicitly.  Please update your pipeline stage called {cls.name} to remove
the input called 'config'.
""")

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
    def usage(cls):
        stage_names = "\n- ".join(cls.pipeline_stages.keys())
        sys.stderr.write(f"""
Usage: python -m txpipe <stage_name> <stage_arguments>

If no stage_arguments are given then usage information
for the chosen stage will be given.

I currently know about these stages:
- {stage_names}
""")

    @classmethod
    def main(cls):
        """
        Create an instance of this stage and run it with
        inputs and outputs taken from the command line
        """
        try:
            stage_name = sys.argv[1]
        except IndexError:
            cls.usage()
            return 1
        if stage_name in ['--help', '-h'] and len(sys.argv)==2:
            cls.usage()
            return 1
        stage = cls.get_stage(stage_name)
        args = stage._parse_command_line()
        stage.execute(args)
        return 0

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
        import argparse
        parser = argparse.ArgumentParser(description=f"Run pipeline stage {cls.name}")
        parser.add_argument("stage_name")
        for conf in cls.config_options:
            def_val = cls.config_options[conf]
            opt_type = def_val if type(def_val) == type else type(def_val)

            if opt_type == bool:
                parser.add_argument(f'--{conf}', action='store_const', const=True)
            elif opt_type == list:
                out_type = def_val[0] if type(def_val[0]) == type else type(def_val[0])
                if out_type is str:
                    parser.add_argument(f'--{conf}', type=lambda string: string.split(',') )
                elif out_type is int:
                    parser.add_argument(f'--{conf}', type=lambda string: [int(i) for i in string.split(',')])
                elif out_type is float:
                    parser.add_argument(f'--{conf}', type=lambda string: [float(i) for i in string.split(',')])
                else:
                    raise NotImplementedError("Only handles str, int and float list arguments")
            else:
                parser.add_argument(f'--{conf}', type=opt_type)
        for inp in cls.input_tags():
            parser.add_argument(f'--{inp}')
        for out in cls.output_tags():
            parser.add_argument(f'--{out}')
        parser.add_argument('--config')
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
        import pdb
        stage = cls(args)
        if stage.rank==0:
            print(f"Executing stage: {cls.name}")

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
    def generate_command(cls, external_inputs, config, outdir, nprocess=1, mpi_command='mpirun -n'):
        """
        Generate a command line that will run the stage
        """
        module = cls.get_module()
        module = module.split('.')[0]

        # Collect flags.
        # This is a bit different from the case within the
        # parsl pipeline because of where we find the inputs,
        # even if the end result is usually the same
        flags = [cls.name]
        for tag,ftype in cls.inputs + cls.outputs:
            if tag in external_inputs:
                fpath = external_inputs[tag]
            else:
                fpath = f'{outdir}/{tag}.{ftype.suffix}'
            flag = f'--{tag}={fpath}'
            flags.append(flag)

        flags.append(f'--config={config}')
        flags = "   ".join(flags)

        # This is identical to the parsl case however
        if nprocess > 1:
            launcher = f"{mpi_command} {nprocess}"
            mpi_flag = "--mpi"
        else:
            launcher = ""
            mpi_flag = ""

        # We just return this, instead of wrapping it in a
        # parsl job
        cmd = f'{launcher} python3 -m {module} {flags} {mpi_flag}'
        return cmd

    @classmethod
    def generate(cls, dfk, nprocess, site_name, log_dir, mpi_command='mpirun -n'):
        """
        Build a parsl bash app that executes this pipeline stage
        """
        module = cls.get_module()
        module = module.split('.')[0]

        flags = [cls.name]

        for i,inp in enumerate(cls.input_tags()):
            flag = '--{}={{inputs[{}]}}'.format(inp,i)
            flags.append(flag)

        config_index = len(cls.input_tags())
        flags.append(f'--config={{inputs[{config_index}]}}')

        for i,out in enumerate(cls.output_tags()):
            flag = '--{}={{outputs[{}]}}'.format(out,i)
            flags.append(flag)

        flags = "   ".join(flags)

        # The last input file is always the config


        # Parallelism - simple for now
        if nprocess > 1:
            launcher = f"{mpi_command} {nprocess}"
            mpi_flag = "--mpi"
        else:
            launcher = ""
            mpi_flag = ""

        template = f"""
@parsl.App('bash', dfk, sites=['{site_name}'])
def {cls.name}(inputs, outputs, stdout='{log_dir}/{cls.name}.out', stderr='{log_dir}/{cls.name}.err'):
    cmd = '{launcher} python3 -m {module} {flags} {mpi_flag}'.format(inputs=inputs,outputs=outputs)
    print("Compiling command:")
    print(cmd)
    return cmd
"""

        return cls._generate(template, dfk)
