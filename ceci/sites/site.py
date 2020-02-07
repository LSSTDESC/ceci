class Site:
    default_mpi_command = 'mpirun -n '
    def __init__(self, config):
        self.mpi_command = config.get('mpi_command', self.default_mpi_command)
        self.info = {}
        self.config = config


    def configure_for_launcher(self, launcher):
        f = getattr(self, f'configure_for_{launcher}', None)
        if f is None:
            raise ValueError(f"Site {self} does not know how to configure for launcher {launcher}")
        f()

