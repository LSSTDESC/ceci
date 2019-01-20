import sys
import os
import tempfile
import logging
import shutil

import shellescape
from schema_salad.validate import ValidationException
from typing import (IO, Any, AnyStr, Callable,  # pylint: disable=unused-import
                    Dict, Iterable, List, MutableMapping, Optional, Text,
                    Union, cast, TYPE_CHECKING)
import cwltool
from cwltool.context import (RuntimeContext,  # pylint: disable=unused-import
                      getdefault)
from cwltool.loghandler import _logger
from cwltool.context import LoadingContext, RuntimeContext
from cwltool.flatten import flatten
from cwltool.command_line_tool import CommandLineTool
from cwltool.errors import WorkflowException
from cwltool.utils import (  # pylint: disable=unused-import
    DEFAULT_TMP_PREFIX, Directory, copytree_with_merge, json_dump, json_dumps,
    onWindows, subprocess, bytes2str_in_dicts)
from cwltool.argparser import arg_parser
from cwltool.docker import DockerCommandLineJob
from cwltool.singularity import SingularityCommandLineJob
from .shifter import ShifterCommandLineJob
from cwltool.job import CommandLineJob, SHELL_COMMAND_TEMPLATE, PYTHON_RUN_SCRIPT
from cwltool.job import needs_shell_quoting_re
from io import IOBase, open  # pylint: disable=redefined-builtin


import parsl
from parsl.app.app import python_app, bash_app
import threading

parsl_submission_lock = threading.Lock()

def customMakeTool(toolpath_object, loadingContext):
    """Factory function passed to load_tool() which creates instances of the
    custom CommandLineTool which supports shifter jobs.
    """

    if isinstance(toolpath_object, dict) and toolpath_object.get("class") == "CommandLineTool":
        return customCommandLineTool(toolpath_object, loadingContext)
    return cwltool.context.default_make_tool(toolpath_object, loadingContext)

class customCommandLineTool(cwltool.command_line_tool.CommandLineTool):

    def make_job_runner(self,
                        runtimeContext       # type: RuntimeContext
                       ):  # type: (...) -> Type[JobBase]
        dockerReq, _ = self.get_requirement("DockerRequirement")
        if not dockerReq and runtimeContext.use_container:
            if runtimeContext.find_default_container is not None:
                default_container = runtimeContext.find_default_container(self)
                if default_container is not None:
                    self.requirements.insert(0, {
                        "class": "DockerRequirement",
                        "dockerPull": default_container
                    })
                    dockerReq = self.requirements[0]
                    if default_container == windows_default_container_id \
                            and runtimeContext.use_container and onWindows():
                        _logger.warning(
                            DEFAULT_CONTAINER_MSG, windows_default_container_id,
                            windows_default_container_id)

        if dockerReq is not None and runtimeContext.use_container:
            if runtimeContext.singularity:
                return SingularityCommandLineJob
            elif runtimeContext.shifter:
                return ShifterCommandLineJob
            return DockerCommandLineJob
        for t in reversed(self.requirements):
            if t["class"] == "DockerRequirement":
                raise UnsupportedRequirement(
                    "--no-container, but this CommandLineTool has "
                    "DockerRequirement under 'requirements'.")
        return CommandLineJob


@bash_app
def run_process(job_dir, job_script, stdout='stdout.txt', stderr='stderr.txt'):
    return f"cd {job_dir} && bash {job_script}"

def _job_popen(
        commands,                  # type: List[Text]
        stdin_path,                # type: Optional[Text]
        stdout_path,               # type: Optional[Text]
        stderr_path,               # type: Optional[Text]
        env,                       # type: MutableMapping[AnyStr, AnyStr]
        cwd,                       # type: Text
        job_dir,                   # type: Text
        job_script_contents=None,  # type: Text
        timelimit=None,            # type: int
        name=None                  # type: Text
       ):  # type: (...) -> int
    if not job_script_contents:
        job_script_contents = SHELL_COMMAND_TEMPLATE

    env_copy = {}
    key = None  # type: Any
    for key in env:
        env_copy[key] = env[key]

    job_description = dict(
        commands=commands,
        cwd=cwd,
        env=env_copy,
        stdout_path=stdout_path,
        stderr_path=stderr_path,
        stdin_path=stdin_path,
    )
    with open(os.path.join(job_dir, "job.json"), encoding='utf-8', mode="w") as job_file:
        json_dump(job_description, job_file, ensure_ascii=False)
    try:
        job_script = os.path.join(job_dir, "run_job.bash")
        with open(job_script, "wb") as _:
            _.write(job_script_contents.encode('utf-8'))
        job_run = os.path.join(job_dir, "run_job.py")
        with open(job_run, "wb") as _:
            _.write(PYTHON_RUN_SCRIPT.encode('utf-8'))
        with parsl_submission_lock:
            proc = run_process(job_dir, job_script, stdout=f"{job_dir}/{name}.out", stderr=f"{job_dir}/{name}.err")

        rcode = proc.result()

        return rcode
    except parsl.app.errors.AppFailure:
        stdout_file = f"{job_dir}/{name}.out"
        stderr_file = f"{job_dir}/{name}.err"
        sys.stderr.write(f"""
*************************************************
Error running pipeline stage {name}.
Standard output and error streams below.
*************************************************
Standard output:
----------------

""")
        if os.path.exists(stdout_file):
            sys.stderr.write(open(stdout_file).read())
        else:
            sys.stderr.write("STDOUT MISSING!\n\n")
        sys.stderr.write(f"""
*************************************************
Standard error:
----------------
""")
        if os.path.exists(stderr_file):
            sys.stderr.write(open(stderr_file).read())
        else:
            sys.stderr.write("STDERR MISSING!\n\n")
    finally:
        shutil.rmtree(job_dir)

cwltool.job._job_popen = _job_popen
