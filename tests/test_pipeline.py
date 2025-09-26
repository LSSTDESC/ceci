from ceci import PipelineStage, MiniPipeline, ParslPipeline, Pipeline, DryRunPipeline
from ceci_example.types import TextFile
from ceci.sites import load, reset_default_site
from ceci.utils import extra_paths
import pytest
from parsl import clear
import yaml
import os
import tempfile
import sys
import jinja2.exceptions

# This one should work
class AAA(PipelineStage):
    name = "AAA"
    inputs = [("b", TextFile)]
    outputs = [("a", TextFile)]
    config_options = {}

    def run(self):
        pass


class BBB(PipelineStage):
    name = "BBB"
    inputs = [("a", TextFile)]
    outputs = [("b", TextFile)]
    config_options = {}

    def run(self):
        pass


class CCC(PipelineStage):
    name = "CCC"
    inputs = [("b", TextFile)]
    outputs = [("c", TextFile)]
    config_options = {}

    def run(self):
        pass


class DDD(PipelineStage):
    name = "DDD"
    inputs = [("b", TextFile), ("c", TextFile)]
    outputs = [("d", TextFile)]
    config_options = {}

    def run(self):
        pass

class EEE1(PipelineStage):
    name = "EEE1"
    inputs = []
    outputs = [("e", TextFile)]
    config_options = {}

    def run(self):
        pass

class EEE2(PipelineStage):
    name = "EEE2"
    inputs = []
    outputs = [("e", TextFile)]
    config_options = {}

    def run(self):
        pass


def test_construct_graph():

    launcher_config = {}

    A = {"name": "AAA"}
    B = {"name": "BBB"}
    C = {"name": "CCC"}
    D = {"name": "DDD"}
    E1= {"name": "EEE1"}
    E2 = {"name": "EEE2"}

    # This one should work - basic pipeline
    # as long as we supply input 'a'.
    # order should be B then C
    pipeline = Pipeline([C, B], launcher_config)
    order = pipeline.construct_pipeline_graph({"a": "a.txt"}, launcher_config)
    assert order == ["BBB", "CCC"]

    pipeline = Pipeline([C, D, B], launcher_config)
    order = pipeline.construct_pipeline_graph({"a": "a.txt"}, launcher_config)
    assert order == ["BBB", "CCC", "DDD"]

    # Should fail - missing an input, 'a'
    with pytest.raises(ValueError):
        pipeline = Pipeline([D, C, B], launcher_config)
        pipeline.construct_pipeline_graph({}, launcher_config)

    # Should fail - circular
    with pytest.raises(ValueError):
        pipeline = Pipeline([A, B], launcher_config)
        pipeline.construct_pipeline_graph({}, launcher_config)

    # Should fail - one output is supplied as an input
    # with pytest.raises(ValueError):
    with pytest.raises(ValueError):
        pipeline = Pipeline([A], launcher_config)
        pipeline.construct_pipeline_graph({"a": "a.txt", "b": "b.txt"}, launcher_config)

    # Should fail - repeated stage
    with pytest.raises(ValueError):
        pipeline = Pipeline([A, A], launcher_config)
        pipeline.construct_pipeline_graph({"b": "b.txt"}, launcher_config)

    # Should fail - two outputs with same name
    with pytest.raises(ValueError):
        pipeline = Pipeline([E1, E2], launcher_config)
        pipeline.construct_pipeline_graph({}, launcher_config)



class FailingStage(PipelineStage):
    name = "FailingStage"
    inputs = []
    outputs = [("dm", TextFile)]  # exists already as an input
    config_options = {}

    def run(self):
        raise ValueError("This should not run because its outputs exist.")


def _return_value_test_(resume):
    expected_status = 0 if ((resume == "resume") or (resume is True)) else 1
    # Mini pipeline should not run
    launcher_config = {"interval": 0.5, "name": "mini"}
    run_config = {
        "log_dir": "./tests/logs",
        "output_dir": "./tests/inputs",
        "resume": resume,
    }

    pipeline = MiniPipeline([{"name": "FailingStage"}], launcher_config)
    pipeline.initialize({}, run_config, "tests/config.yml")
    status = pipeline.run()
    assert status == expected_status

    # Parsl pipeline should not run stage either
    launcher_config = {"name": "parsl"}
    site_config = {"name": "local", "max_threads": 1}
    load(launcher_config, [site_config])
    # the above sets the new default to be the parsl-configured site
    pipeline = ParslPipeline([{"name": "FailingStage"}], launcher_config)
    pipeline.initialize({}, run_config, "tests/config.yml")
    status = pipeline.run()
    assert status == expected_status
    clear()  # clear parsl settings
    reset_default_site()  # reset so default is minirunner again


def test_resume():
    _return_value_test_(True)
    _return_value_test_("resume")


def test_fail():
    _return_value_test_(False)
    _return_value_test_("restart")

def test_refuse():
    launcher_config = {"interval": 0.5, "name": "mini"}
    run_config = {
        "log_dir": "./tests/logs",
        "output_dir": "./tests/inputs",
        "resume": "refuse",
    }

    pipeline = MiniPipeline([{"name": "FailingStage"}], launcher_config)

    with pytest.raises(RuntimeError) as excinfo:
        pipeline.initialize({}, run_config, "tests/config.yml")
        pipeline.run()

    assert "Output files already exist for stage" in str(excinfo.value)


def test_dry_run(mocker):
    import ceci_example

    # override stdout so that it thinks it's a terminal so
    # that we can test the emboldening
    stdout_mock = mocker.patch("ceci.pipeline.pipeline.sys.stdout")
    stdout_mock.isatty.return_value = True

    config = yaml.safe_load(open("tests/test.yml"))
    launcher_config = {"interval": 0.5, "name": "mini"}

    pipeline = DryRunPipeline(config["stages"], launcher_config)

    run_config = {
        "log_dir": config["log_dir"],
        "output_dir": config["output_dir"],
        "resume": False,
    }

    pipeline.initialize(config["inputs"], run_config, config["config"])
    status = pipeline.run()

    assert status == 0
    for cmd in pipeline.pipeline_outputs:
        print(f"running {cmd} with os.system")
        status = os.system(cmd)
        assert status == 0


def test_python_paths():
    # make a temp dir
    with tempfile.TemporaryDirectory() as dirname:
        os.mkdir(dirname + "/pretend")

        # create a subdir of that with a module in
        mod_dir = dirname + "/pretend"
        mod_path = mod_dir + "/pretend_module.py"

        # empty module, just to check it imports
        open(mod_path, "w").close()

        assert os.path.exists(mod_path)
        print(os.listdir(mod_dir))

        # create a stage there that uses the submodule
        stage_path = dirname + "/my_stage.py"
        open(stage_path, "w").write(
            """
import ceci
class MyStage(ceci.PipelineStage):
    name = "MyStage"
    inputs = []
    outputs = []
    config_options = {"x": int}
    def run(self):
        import pretend_module
        assert self.config["x"] == 17
"""
        )

        # pipeline admin
        config_path = dirname + "/config.yml"
        open(config_path, "w").write(
            """
MyStage:
    x: 17
            """
        )

        run_config = {
            "log_dir": dirname,
            "output_dir": dirname,
            "resume": False,
            "python_paths": [dirname, mod_dir],
        }

        launcher_config = {"interval": 0.5, "name": "mini"}
        site_config = {"name": "local", "python_paths": [dirname, mod_dir]}
        load(launcher_config, [site_config])

        # note that we don't add the subdir here
        with extra_paths(dirname):
            import my_stage

            print(os.environ["PYTHONPATH"])
            print(sys.path)
            print(os.listdir(dirname))
            pipeline = MiniPipeline([{"name": "MyStage"}], launcher_config)
            pipeline.initialize({}, run_config, config_path)
            status = pipeline.run()
            log = open(dirname + "/MyStage.out").read()
            print(log)
            assert status == 0


def test_init_stages():
    B = {"name": "BBB"}
    C = {"name": "CCC"}
    launcher_config = {"interval": 0.5, "name": "mini"}

    # This one should work - basic pipeline
    # as long as we supply input 'a'.
    # order should be A then C
    inputs = {"a": "a.txt"}
    run_config = {}

    # Test initializing stages with a file name
    pipeline = Pipeline([C, B], launcher_config)
    pipeline.construct_pipeline_graph(inputs, run_config)
    pipeline.configure_stages("tests/config.yml")

    # Test initializing stages with dict
    pipeline = Pipeline([C, B], launcher_config)
    pipeline.construct_pipeline_graph(inputs, run_config)
    pipeline.configure_stages({})

    # Test initializing stages with nothing
    pipeline = Pipeline([C, B], launcher_config)
    pipeline.construct_pipeline_graph(inputs, run_config)
    pipeline.configure_stages(None)


    # should fail - wrong type
    p = MiniPipeline({}, [])
    with pytest.raises(ValueError):
        p.configure_stages([])

def test_config_template():
    params1 = {
        "field": "north",
        "logfile": "northern.txt",
        "some_directory": "xyz",
    }
    config = Pipeline.build_config(
        "tests/template.yml",
        template_parameters=params1
    )

    assert config["output_dir"] == "./tests/outputs_north"
    assert config["log_dir"] == "./tests/northern.txt"
    assert config["inputs"]["fiducial_cosmology"] == "./tests/xyz/fiducial_cosmology.txt"

    params2 = {
        "field": "north",
        "logfile": "northern.txt",
        "some_directory": "xyz",
        "unused_param": "not_used",
    }
    with pytest.raises(ValueError):
        config = Pipeline.build_config(
            "tests/template.yml",
            template_parameters=params2
        )

    params3 = {
        "field": "north",
        "some_directory": "xyz",
    }

    with pytest.raises(jinja2.exceptions.UndefinedError):
        config = Pipeline.build_config(
            "tests/template.yml",
            template_parameters=params3
        )

    params4 = object()
    # Test that using the wrong type raises an error
    with pytest.raises(TypeError, match="template_parameters must be a dict or a list or space"):
        config = Pipeline.build_config(
            "tests/template.yml",
            template_parameters=params4
        )

    params5 = "field=north logfile=northern.txt some_directory=xyz"
    config = Pipeline.build_config(
        "tests/template.yml",
        template_parameters=params5
    )

    assert config["output_dir"] == "./tests/outputs_north"
    assert config["log_dir"] == "./tests/northern.txt"
    assert config["inputs"]["fiducial_cosmology"] == "./tests/xyz/fiducial_cosmology.txt"

    params6 = ["field=north", "logfile=northern.txt", "some_directory=xyz"]
    config = Pipeline.build_config(
        "tests/template.yml",
        template_parameters=params6
    )

    assert config["output_dir"] == "./tests/outputs_north"
    assert config["log_dir"] == "./tests/northern.txt"
    assert config["inputs"]["fiducial_cosmology"] == "./tests/xyz/fiducial_cosmology.txt"


# this has to be here because we test running the pipeline
if __name__ == "__main__":
    PipelineStage.main()
