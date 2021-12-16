import warnings


class Site:

    default_mpi_command = "mpirun -n"

    def __init__(self, config):
        self.mpi_command = config.get("mpi_command", self.default_mpi_command)
        self.info = {}
        self.config = config

    def check_import(self, launcher): #pylint: disable=no-self-use
        requirements = {
            "parsl": ["parsl"],
            "cwl": ["cwlgen", "cwltool"],
            "mini": ["psutil"],
        }
        if launcher not in requirements:  #pragma: no cover
            raise ValueError(f"Unknown launcher '{launcher}'")
        missing = []
        libs = requirements[launcher]
        for lib in libs:
            try:
                with warnings.catch_warnings():
                    warnings.filterwarnings("ignore", category=DeprecationWarning)
                    __import__(lib)
            except ImportError:  #pragma: no cover
                missing.append(lib)
        if missing:  #pragma: no cover
            missing = ", ".join(missing)
            raise ImportError(
                f"You must install these libraries "
                f"to use the {launcher} launcher: {missing}"
            )

    def configure_for_launcher(self, launcher):
        self.check_import(launcher)
        configure = getattr(self, f"configure_for_{launcher}", None)
        if configure is None:  #pragma: no cover
            raise ValueError(
                f"Site {self} does not know how to configure for launcher {launcher}"
            )
        configure()
