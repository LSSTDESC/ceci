from .utils import get_caller_directory
import subprocess


def diff(dirname=None, parent_frames=1):
    """
    Run git diff in the caller's directory (default) or another specified directory,
    and return stdout+stderr
    """
    if dirname is None:
        dirname = get_caller_directory(parent_frames + 1)

    if dirname is None:
        return "ERROR_GIT_NO_DIRECTORY"
    # We use git diff head because it shows all differences,
    # including any that have been staged but not committed.
    try:
        diff = subprocess.run(
            "git diff HEAD".split(),
            cwd=dirname,
            universal_newlines=True,
            timeout=5,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )

    # There are lots of different ways this can go wrong.
    # Here are some - any others it is probably worth knowing
    # about
    except subprocess.TimeoutExpired:
        return "ERROR_GIT_TIMEOUT"
    except UnicodeDecodeError:
        return "ERROR_GIT_DECODING"
    except subprocess.SubprocessError:
        return "ERROR_GIT_OTHER"
    except FileNotFoundError:
        return "ERROR_GIT_NOT_RUNNABLE"
    except OSError:
        return "ERROR_GIT_OTHER_OSERROR"
    # If for some reason we are running outside the main repo
    # this will return an error too
    if diff.returncode:
        return "ERROR_GIT_FAIL"

    return diff.stdout


def current_revision(dirname=None, parent_frames=1):
    """Return the git revision ID in the caller's directory (default) or another
    specified directory.
    """
    if dirname is None:
        dirname = get_caller_directory(parent_frames + 1)

    if dirname is None:
        return "ERROR_GIT_NO_DIRECTORY"
    try:
        rev = subprocess.run(
            "git rev-parse HEAD".split(),
            cwd=dirname,
            universal_newlines=True,
            timeout=5,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )
    # Same as git diff above.
    except subprocess.TimeoutExpired:
        return "ERROR_GIT_TIMEOUT"
    except UnicodeDecodeError:
        return "ERROR_GIT_DECODING"
    except subprocess.SubprocessError:
        return "ERROR_GIT_OTHER"
    except FileNotFoundError:
        return "ERROR_GIT_NOT_RUNNABLE"
    except OSError:
        return "ERROR_GIT_OTHER_OSERROR"
    # If for some reason we are running outside the main repo
    # this will return an error too
    if rev.returncode:
        return "ERROR_GIT_FAIL"
    return rev.stdout
