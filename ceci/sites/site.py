class Site:
    """

    """
    default_mpi_command = 'mpirun -n'
    def __init__(self, config):
        self.mpi_command = config.get('mpi_command', self.default_mpi_command)
        self.info = {}
        self.config = config

    def check_import(self, launcher):
        requirements = {
            'parsl': ['parsl'],
            'cwl': ['cwlgen', 'cwltool'],
            'mini': [],
        }
        if launcher not in requirements:
            raise ValueError(f"Unknown launcher '{launcher}'")
        missing = []
        libs = requirements[launcher]
        for lib in libs:
            try:
                __import__(lib)
            except ImportError:
                missing.append(lib)
        if missing:
            missing = ', '.join(missing)
            raise ImportError(f"You must install these libraries "
                              f"to use the {launcher} launcher: {missing}")


    def configure_for_launcher(self, launcher):
        self.check_import(launcher)
        configure = getattr(self, f'configure_for_{launcher}', None)
        if configure is None:
            raise ValueError(f"Site {self} does not know how to configure for launcher {launcher}")
        configure()


