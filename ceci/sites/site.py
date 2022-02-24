"""Utility class to interface to workflow managers with site-specific configuration"""

import warnings


class Site:
    """Object representing execution at a specific site"""

    default_mpi_command = "mpirun -n"

    def __init__(self, config):
        """Constructor, takes a dict of configuration parameters"""
        self.mpi_command = config.get("mpi_command", self.default_mpi_command)
        self.info = {}
        self.config = config

    def check_import(self, launcher):  # pylint: disable=no-self-use
        """Make sure that required libraries can be imported, and raise ImportError if they can not

        Parameters
        ----------
        launcher : str
            The launcher being built

        Raises
        ------
        ImportError : If the libraries for the requested launcher can not be built
        """
        requirements = {
            "parsl": ["parsl"],
            "cwl": ["cwlgen", "cwltool"],
            "mini": ["psutil"],
        }
        if launcher not in requirements:  # pragma: no cover
            raise ValueError(f"Unknown launcher '{launcher}'")
        missing = []
        libs = requirements[launcher]
        for lib in libs:
            try:
                with warnings.catch_warnings():
                    warnings.filterwarnings("ignore", category=DeprecationWarning)
                    __import__(lib)
            except ImportError:  # pragma: no cover
                missing.append(lib)
        if missing:  # pragma: no cover
            missing = ", ".join(missing)
            raise ImportError(
                f"You must install these libraries "
                f"to use the {launcher} launcher: {missing}"
            )

    def configure_for_launcher(self, launcher):
        """Check to see if the given launcher is supported at the site in questions

        Notes
        -----
        This looks for a method called configure_for_{launcher} and will raise a ValueError
        if the method associated to the requested launcher does not exist.
        """

        self.check_import(launcher)
        configure = getattr(self, f"configure_for_{launcher}", None)
        if configure is None:  # pragma: no cover
            raise ValueError(
                f"Site {self} does not know how to configure for launcher {launcher}"
            )
        configure()
