import concurrent
import parsl
import sys
import cwltool
from cwltool.loghandler import _logger
from cwltool.context import LoadingContext, RuntimeContext
from cwltool.flatten import flatten
from cwltool.command_line_tool import CommandLineTool
from cwltool.errors import WorkflowException
from cwltool.executors import JobExecutor
from schema_salad.validate import ValidationException
from concurrent.futures import ALL_COMPLETED, FIRST_COMPLETED
from parsl import DataFlowKernel, App

__all__ = [ "ParslExecutor" ]

class ParslExecutor(JobExecutor):
    """
    Custom Parsl-based executor for a worflow
    """
    def __init__(self): # type: () -> None
        super(ParslExecutor, self).__init__()
        self.futures = set()

    def run_jobs(self,
                 process,           # type: Process
                 job_order_object,  # type: Dict[Text, Any]
                 logger,
                 runtimeContext     # type: RuntimeContext
                ):  # type: (...) -> None
        """ Execute the jobs for the given Process. """

        local_config = {
            "sites" : [
                { "site" : "Threads",
                  "auth" : { "channel" : None },
                  "execution" : {
                      "executor" : "threads",
                      "provider" : None,
                      "maxThreads" : 4
                  }
                }],
            "globals" : {"lazyErrors" : True}
        }

        dfk = DataFlowKernel(config=local_config)

        @App('python', dfk)
        def parsl_runner(job, rc):
            """
            The inputs are dummy futures provided by the parent steps of the
            workflow, if there are some
            """
            return job.run(rc)

        process_run_id = None  # type: Optional[str]
        reference_locations = {}  # type: Dict[Text,Text]

        # define provenance profile for single commandline tool
        if not isinstance(process, cwltool.workflow.Workflow) \
                and runtimeContext.research_obj is not None:
            orcid = runtimeContext.orcid
            full_name = runtimeContext.cwl_full_name
            process.provenance_object = CreateProvProfile(
                runtimeContext.research_obj, orcid, full_name)
            process.parent_wf = process.provenance_object
        jobiter = process.job(job_order_object, self.output_callback, runtimeContext)

        try:
            for job in jobiter:
                if job:
                    if runtimeContext.builder is not None:
                        job.builder = runtimeContext.builder
                    if job.outdir:
                        self.output_dirs.add(job.outdir)
                    if runtimeContext.research_obj is not None:
                        if not isinstance(process, Workflow):
                            runtimeContext.prov_obj = process.provenance_object
                        else:
                            runtimeContext.prov_obj = job.prov_obj
                        assert runtimeContext.prov_obj
                        process_run_id, reference_locations = \
                                runtimeContext.prov_obj.evaluate(
                                        process, job, job_order_object,
                                        runtimeContext.make_fs_access,
                                        runtimeContext)
                        runtimeContext = runtimeContext.copy()
                        runtimeContext.process_run_id = process_run_id
                        runtimeContext.reference_locations = reference_locations

                    # Run the job within a Parsl App
                    res = parsl_runner(job, runtimeContext)

                    # Adds the future to the set of futures
                    self.futures.add(res.parent)
                else:
                    print(self.futures)
                    if self.futures:
                        done, notdone = concurrent.futures.wait(self.futures, return_when=FIRST_COMPLETED)
                        self.futures = notdone
                    else:
                        logger.error("Workflow cannot make any more progress.")
                        break

            # At the end, make sure all the steps have been processed
            if self.futures:
                concurrent.futures.wait(self.futures, return_when=ALL_COMPLETED)
        except (ValidationException, WorkflowException):
            raise
        except Exception as e:
            logger.exception("Got workflow error")
            raise WorkflowException(Text(e))
