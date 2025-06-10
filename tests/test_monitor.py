import time
from ceci.monitor import MemoryMonitor
# should already be installed on CI as we need HDF5
import numpy as np


def test_monitor_report(capsys):
    monitor = MemoryMonitor.start_in_thread(interval=1)
    time.sleep(2)
    x = np.random.uniform(size=10_000_000)
    time.sleep(2)
    monitor.stop()
    captured = capsys.readouterr()
    lines = [line for line in captured.out.split("\n") if line.startswith("MemoryMonitor Time")]
    assert len(lines) >= 3

    # check that memory usage is large enough
    gb1 = float(lines[0].split()[5])
    gb2 = float(lines[-1].split()[5])
    # this test sometimes fails without the factor of 2 because
    # other processes can free memory in between
    # the two measurements, so we allow for a bit of variance
    assert gb2 - gb1 >= x.nbytes / 1.01e9 / 2
