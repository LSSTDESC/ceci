import parsl
import abc
import pathlib
import sys

class PipelineStage:
    def __init__(self, inputs, outputs):
        for name in inputs:
            print(f"Input: {name}")
        for name in outputs:
            print(f"Output: {name}")
        self.input_values = inputs
        self.output_values = outputs

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
        inputs, outputs = cls._parse_command_line()
        cls.execute(inputs, outputs)

    @classmethod
    def _parse_command_line(cls):
        import argparse
        parser = argparse.ArgumentParser(description="Run a stage or something")
        parser.add_argument('--inputs', default="")
        parser.add_argument('--outputs', default="")
        args = parser.parse_args()
        inputs = args.inputs.split(',') if args.inputs else []
        outputs = args.outputs.split(',') if args.outputs else []
        return inputs, outputs

    @classmethod
    def execute(cls, inputs, outputs):
        """
        Create an instance of this stage and run it 
        with the specified inputs and outputs
        """
        stage = cls(inputs, outputs)
        stage.run()

    @classmethod
    def _generate(cls, template, dfk):
        d = locals().copy()
        exec(template, globals(), d)
        function = d['function']
        function.__name__ = cls.name
        return function

    @classmethod
    def generate_bash(cls, dfk):
        """
        Build a parsl bash app that executes this pipeline stage
        """
        path = cls.get_executable()
        template = f'''
@parsl.App('bash', dfk)
def function(inputs=[], outputs=[]):
    input_text = ",".join(str(x) for x in inputs)
    output_text = ",".join(str(x) for x in outputs)
    return "python3 {path} --inputs=" + input_text + " --outputs=" + output_text
        '''
        return cls._generate(template, dfk)

    @classmethod
    def generate_python(cls, dfk):
        """
        Build a parsl python app that executes this pipeline stage
        """

        template = '''
@parsl.App('python', dfk)
def function(cls=cls, inputs=[], outputs=[]):
    inputs = [str(s) for s in inputs]
    outputs = [str(s) for s in outputs]
    return cls.execute(inputs, outputs)
        '''
        return cls._generate(template, dfk)
