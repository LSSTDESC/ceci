from .dry_run import DryRunPipeline

class FlowChartPipeline(DryRunPipeline):
    def run_jobs(self):
        filename = self.run_config["flow_chart"]
        self.make_flow_chart(filename)
        return 0

    def setup_data_registry(self, registry_config):
        """Flow charts never read data, so do not connect to the data registry."""
        return None

    def data_registry_lookup(self, info):
        """Return a placeholder path instead of querying the data registry."""
        return f"registry:{info}"

