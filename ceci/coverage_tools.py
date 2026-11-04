"""Tools for collecting code coverage of pipelines using coverage.py.

Stages run in separate processes (possibly under MPI or in containers), so
coverage has to be collected in each of them as well as in the main ceci
process. Each process writes its own "parallel mode" data file, named
``<base>.<label>.<host>.<pid>.<random>``. When the pipeline finishes the main
process combines them all into a single file at ``<base>``, which can later
be combined with others using ``coverage combine``.
"""

import glob
import os
import shlex

STAGE_FILE_FLAG = "--ceci-coverage-file"
STAGE_MODULES_FLAG = "--ceci-coverage-modules"


def _import_coverage():
    try:
        import coverage
    except ImportError as err:  # pragma: no cover
        raise ImportError(
            "Code coverage was requested but coverage.py is not installed. "
            "Install it with: pip install coverage"
        ) from err
    return coverage


def start_coverage(base_name, modules, label):
    """Start recording coverage in this process.

    Parameters
    ----------
    base_name : str
        Base name of the output file
    modules : list[str]
        Importable module/package names to record coverage for
    label : str
        Identifies this process in the name of its data file

    Returns
    -------
    cov : coverage.Coverage
        The running Coverage object; pass it to `stop_coverage`.
    """
    coverage = _import_coverage()
    cov = coverage.Coverage(
        data_file=os.path.abspath(f"{base_name}.{label}"),
        data_suffix=True,
        source_pkgs=list(modules) or None,
    )
    cov.start()
    return cov


def stop_coverage(cov):
    """Stop recording and write this process's data file."""
    if cov is not None:
        cov.stop()
        cov.save()


def erase_previous(base_name):
    """Remove an old output file and any left-over per-process files."""
    base_name = os.path.abspath(base_name)
    for path in [base_name] + glob.glob(glob.escape(base_name) + ".*"):
        if os.path.isfile(path):
            os.remove(path)


def combine_coverage(base_name):
    """Combine all per-process data files into the single file ``base_name``.

    Returns
    -------
    n : int
        The number of per-process files combined (0 if there were none)
    """
    coverage = _import_coverage()
    base_name = os.path.abspath(base_name)
    pattern = glob.escape(base_name) + ".*"
    if not glob.glob(pattern):
        return 0
    n = len(glob.glob(pattern))
    cov = coverage.Coverage(data_file=base_name)
    cov.combine(data_paths=[os.path.dirname(base_name)], strict=False, keep=False)
    cov.save()
    return n


def stage_command_flags(coverage_config):
    """Extra command line flags that make a stage process record coverage.

    Parameters
    ----------
    coverage_config : dict or None
        Has keys "file" and "modules"
    """
    if not coverage_config:
        return ""
    # The stage process adds its own label to the name
    base = os.path.abspath(coverage_config["file"])
    flags = f"{STAGE_FILE_FLAG}={shlex.quote(base)}"
    mods = " ".join(coverage_config.get("modules", []))
    if mods:
        flags += f" {STAGE_MODULES_FLAG}={shlex.quote(mods)}"
    return flags


def extract_stage_coverage_args(argv):
    """Remove ceci's private coverage flags from argv (in place).

    Returns
    -------
    base_name : str or None
    modules : list[str]
    """
    base_name = None
    modules = []
    for arg in list(argv):
        if arg.startswith(STAGE_FILE_FLAG + "="):
            base_name = arg.split("=", 1)[1]
            argv.remove(arg)
        elif arg.startswith(STAGE_MODULES_FLAG + "="):
            modules = arg.split("=", 1)[1].split()
            argv.remove(arg)
    return base_name, modules
