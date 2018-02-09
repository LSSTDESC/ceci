import parsl
import pipette_lib.twopoint_stage
from parsl.data_provider.files import File

parsl.set_stream_logger()
workers = parsl.ThreadPoolExecutor(max_workers=4)
dfk = parsl.DataFlowKernel(executors=[workers])


inputs = [File('input.txt')]
outputs = []
app = pipette_lib.twopoint_stage.Stage.generate_python(dfk)
# app = make_twopoint.Stage.generate_bash(dfk)
app(inputs=inputs, outputs=outputs)


