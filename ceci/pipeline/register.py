"""Registration of pipeline outputs in the DESC data registry.

All of the outputs of a pipeline are registered as datasets that share a
single data registry "execution".
"""

import os
import re
import tempfile
from datetime import datetime

# The registry copies data into its root directory, so refuse to
# register more than this unless explicitly allowed.
MAX_REGISTER_BYTES = 50 * 1024**3

DEFAULT_VERSION = "1.0.0"

# Characters the data registry forbids in dataset names
_ILLEGAL_NAME_CHARS = "$*&/?\\ "


class RegistrationError(Exception):
    """Raised when outputs cannot or should not be registered"""


def _path_size(path):
    """Total size in bytes of a file or directory, rejecting symlinks"""
    if os.path.islink(path):
        raise RegistrationError(f"{path} is a symlink, which the data registry does not support")
    if os.path.isfile(path):
        return os.path.getsize(path)
    total = 0
    for root, dirs, files in os.walk(path):
        for name in dirs + files:
            full = os.path.join(root, name)
            if os.path.islink(full):
                raise RegistrationError(
                    f"{full} is a symlink, which the data registry does not support"
                )
        total += sum(os.path.getsize(os.path.join(root, f)) for f in files)
    return total


def plan_registration(outputs, execution_name, version, allow_incomplete):
    """Check what would be registered, without touching the registry.

    Parameters
    ----------
    outputs : dict
        tag: path for all outputs of the pipeline
    execution_name : str
    version : str
    allow_incomplete : bool

    Returns
    -------
    plan : list of (name, path, size) tuples
    missing : list of (tag, path) for outputs that do not exist

    Raises
    ------
    RegistrationError
        If anything prevents registration
    """
    if not re.fullmatch(r"\d+\.\d+\.\d+", version):
        raise RegistrationError(f"Version '{version}' is not of the form major.minor.patch")

    bad = [c for c in _ILLEGAL_NAME_CHARS if c in execution_name]
    if bad:
        raise RegistrationError(f"Execution name '{execution_name}' contains illegal characters {bad}")

    plan = []
    missing = []
    for tag, path in outputs.items():
        name = f"{execution_name}_{tag}"
        bad = [c for c in _ILLEGAL_NAME_CHARS if c in name]
        if bad:
            raise RegistrationError(f"Dataset name '{name}' contains illegal characters {bad}")
        if not os.path.exists(path):
            missing.append((tag, path))
            continue
        plan.append((name, path, _path_size(path)))

    if missing and not allow_incomplete:
        lines = "\n".join(f"    {tag}: {path}" for tag, path in missing)
        raise RegistrationError(
            f"{len(missing)} pipeline outputs are missing:\n{lines}\n"
            "Pass --dregs-incomplete to register only the outputs that exist."
        )

    if not plan:
        raise RegistrationError("There are no output files to register")

    return plan, missing


def check_size(plan, allow_large):
    """Raise if the total size of the planned datasets is over the limit"""
    total = sum(size for _, _, size in plan)
    if total > MAX_REGISTER_BYTES and not allow_large:
        raise RegistrationError(
            f"Total size of outputs to register is {total / 1024**3:.1f} GB, "
            f"more than the {MAX_REGISTER_BYTES / 1024**3:.0f} GB limit. "
            "Pass --dregs-allow-large to register them anyway."
        )


def format_plan(execution_name, version, plan, missing, existing=()):
    """Human-readable description of a registration plan"""
    total = sum(size for _, _, size in plan)
    lines = [f"Execution '{execution_name}' would register {len(plan)} datasets at version {version}:"]
    for name, path, size in plan:
        lines.append(f"    {name:40} {size / 1024**2:10.2f} MB  {path}")
    lines.append(f"Total size: {total / 1024**3:.3f} GB")
    for name in existing:
        lines.append(f"Skipping {name}: already registered at version {version}")
    for tag, path in missing:
        lines.append(f"Skipping missing output {tag}: {path}")
    return "\n".join(lines)


def find_existing(registry, plan, version):
    """Return the names of datasets in the plan already in the registry"""
    existing = []
    for name, _, _ in plan:
        filters = [
            registry.Query.gen_filter("dataset.name", "==", name),
            registry.Query.gen_filter("dataset.version_string", "==", version),
        ]
        found = registry.Query.find_datasets(["dataset.dataset_id"], filters)
        if found and len(found["dataset.dataset_id"]) > 0:
            existing.append(name)
    return existing


def find_execution(registry, execution_name):
    """Return the id of the existing registry execution with this name"""
    name_filter = registry.Query.gen_filter("execution.name", "==", execution_name)
    found = registry.Query.get_table_values(
        "execution", ["execution_id"], filters=[name_filter]
    )
    ids = found["execution.execution_id"] if found else []
    if len(ids) != 1:
        raise RegistrationError(
            f"--dregs-more needs exactly one existing registry execution named "
            f"'{execution_name}', but found {len(ids)}"
        )
    return ids[0]


def register_plan(
    registry, execution_name, version, plan, configuration=None, description=None,
    execution_id=None,
):
    """Create the execution and register each planned dataset under it

    Parameters
    ----------
    registry : dataregistry.DataRegistry
    configuration : dict, optional
        label: text for configuration files to store with the execution
    execution_id : int, optional
        Add to this existing execution instead of creating a new one

    Returns
    -------
    dataset_ids : dict
        name: dataset id for each registered dataset
    """
    if execution_id is None:
        config_path = None
        max_config_length = None
        try:
            if configuration:
                text = "".join(
                    f"# ---- {label} ----\n{content}\n" for label, content in configuration.items()
                )
                with tempfile.NamedTemporaryFile("w", suffix=".yml", delete=False) as f:
                    f.write(text)
                    config_path = f.name
                max_config_length = len(text) + 1

            description = description or f"Outputs of ceci pipeline, registered as {execution_name}"
            execution_id = registry.Registrar.execution.register(
                execution_name,
                description=description,
                execution_start=datetime.now(),
                configuration=config_path,
                max_config_length=max_config_length,
            )
        finally:
            if config_path is not None:
                os.unlink(config_path)

    dataset_ids = {}
    for name, path, _ in plan:
        dataset_id, _ = registry.Registrar.dataset.register(
            name,
            version,
            old_location=path,
            execution_id=execution_id,
            description=f"Output of ceci pipeline execution {execution_name}",
        )
        dataset_ids[name] = dataset_id
    return dataset_ids
