from .dry_run import DryRunPipeline

class FlowChartPipeline(DryRunPipeline):
    def run_jobs(self):
        filename = self.run_config["flow_chart"]
        self.make_flow_chart(filename)
        return 0

