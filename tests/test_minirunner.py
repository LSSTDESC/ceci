# Things to test
from ceci.minirunner import (
    Runner,
    WAITING,
    COMPLETE,
    Job,
    Node,
    CannotRun,
    TimeOut,
    FailedJob,
)
from ceci.minirunner import EVENT_LAUNCH, EVENT_COMPLETED, EVENT_FAIL, EVENT_ABORT
import os
import signal
import time
from .test_helpers import in_temp_dir
from pytest import raises


@in_temp_dir
def test_minirununer_parallel():
    job1 = Job("Job1", "echo start 1; sleep 1; echo end 1", cores=1, nodes=1)
    job2 = Job("Job2", "echo start 2; sleep 1; echo end 2", cores=1, nodes=1)
    job_dependencies = {
        job1: [],
        job2: [],
    }

    # Two nodes with 1 core each
    node1 = Node("node1", 1)
    node2 = Node("node2", 2)
    nodes = [node1, node2]
    r = Runner(nodes, job_dependencies, ".")

    # Both jobs should be ready to run now.  Order is not required
    assert r.queued_jobs in [[job1, job2], [job2, job1]]
    assert r._ready_jobs() in [[job1, job2], [job2, job1]]
    assert len(r._check_availability(job1)) == 1

    # Launch the jobs.  They will all take 1 second, long enough for the tasks below to work
    status = r._update()
    assert status == WAITING

    # Now both jobs should be running, as there are no dependencies
    assert r._ready_jobs() == []
    assert len(r.running) == 2
    # check can assign a node to the job
    assert len(r.queued_jobs) == 0

    # check pipeline finishes okay
    time.sleep(1.1)
    status = r._update()

    assert status == COMPLETE
    assert r.completed_jobs in [[job1, job2], [job2, job1]]


@in_temp_dir
def test_callback_and_sleep():

    events = []

    def callback(event, info):
        events.append((event, info))

    sleeps = []

    def sleep(t):
        sleeps.append(t)
        time.sleep(t)

    job1 = Job("Job1", "echo start 1; sleep 1; echo end 1", cores=1, nodes=1)
    job2 = Job("Job2", "echo start 2; sleep 1; echo end 2", cores=1, nodes=1)
    # This job is designed to fail
    job3 = Job("Job3", "python does_not_exist_for_test.py", cores=1, nodes=1)
    job_dependencies = {
        job1: [],
        job2: [],
        job3: [job1, job2],
    }

    # Two nodes with 1 core each
    node1 = Node("node1", 1)
    node2 = Node("node2", 2)
    nodes = [node1, node2]
    r = Runner(nodes, job_dependencies, ".", callback=callback, sleep=sleep)
    with raises(FailedJob):
        r.run(interval=1)

    assert 1 in sleeps
    for event in [EVENT_LAUNCH, EVENT_COMPLETED, EVENT_FAIL, EVENT_ABORT]:
        # check that each event is fired at least once
        assert [e for e, i in events if e == event]


@in_temp_dir
def test_minirununer_serial():
    job1 = Job("Job1", "echo start 1; sleep 1; echo end 1", cores=1, nodes=1)
    job2 = Job("Job2", "echo start 2; sleep 1; echo end 2", cores=1, nodes=1)
    job_dependencies = {
        job1: [],
        job2: [job1],
    }

    # Two nodes with 1 core each
    node1 = Node("node1", 1)
    node2 = Node("node2", 2)
    nodes = [node1, node2]
    r = Runner(nodes, job_dependencies, ".")

    # Only one job should be ready to run now.  Order is not required
    assert r.queued_jobs in [[job1, job2], [job2, job1]]
    assert r._ready_jobs() == [job1]
    assert len(r._check_availability(job1)) == 1
    assert len(r._check_availability(job2)) == 1

    # Launch the jobs.  They will all take 1 second, long enough for the tasks below to work
    status = r._update()
    assert status == WAITING

    # Now both jobs should be running, as there are no dependencies
    assert r._ready_jobs() == []
    assert len(r.running) == 1
    # check can assign a node to the job
    assert r.queued_jobs == [job2]

    # now should launch the second job as the first is finished
    time.sleep(1.1)
    status = r._update()
    assert status == WAITING

    assert len(r.running) == 1
    assert r.completed_jobs == [job1]
    assert r.queued_jobs == []
    time.sleep(1.1)

    status = r._update()

    assert status == COMPLETE
    assert r.completed_jobs in [[job1, job2], [job2, job1]]
    assert r.running == []
    assert r.queued_jobs == []


@in_temp_dir
def test_timeout():
    node1 = Node("node1", 1)
    nodes = [node1]
    job1 = Job("Job1", "sleep 60", nodes=1, cores=1)
    job_dependencies = {job1: []}
    r = Runner(nodes, job_dependencies, ".")
    with raises(TimeOut):
        r.run(0.5, timeout=1)


@in_temp_dir
def test_cannot_run():
    node1 = Node("node1", 1)
    nodes = [node1]
    job1 = Job("Job1", "echo start 1", nodes=2, cores=1)
    job_dependencies = {job1: []}
    r = Runner(nodes, job_dependencies, ".")

    with raises(CannotRun):
        r.run(0.5, timeout=5)


def _pid_exists(pid):
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    return True


def _wait_for_pid_file(path, timeout=10):
    t0 = time.time()
    while time.time() - t0 < timeout:
        if os.path.exists(path):
            with open(path) as f:
                text = f.read().strip()
            if text:
                return int(text)
        time.sleep(0.05)
    raise RuntimeError("Job never wrote its child pid")


@in_temp_dir
def test_abort_kills_child_processes():
    # The job is run via a shell, which starts a background child.
    # Aborting must kill the child too, not just the shell.
    job = Job("Job1", "sleep 60 & echo $! > child.pid; wait", cores=1, nodes=1)
    r = Runner([Node("node1", 1)], {job: []}, ".")
    r._update()
    child = _wait_for_pid_file("child.pid")
    assert _pid_exists(child)

    r.abort()

    assert r.running == []
    # the child was reparented to init when the shell died; give it
    # a moment to be reaped before checking
    for _ in range(50):
        if not _pid_exists(child):
            break
        time.sleep(0.1)
    assert not _pid_exists(child)


@in_temp_dir
def test_keyboard_interrupt_cleans_up():
    job = Job("Job1", "sleep 60 & echo $! > child.pid; wait", cores=1, nodes=1)
    events = []

    def callback(event_type, event_info):
        events.append(event_type)

    def interrupt(t):
        # Simulate ctrl-C once the job is definitely running
        _wait_for_pid_file("child.pid")
        raise KeyboardInterrupt()

    r = Runner([Node("node1", 1)], {job: []}, ".", callback=callback, sleep=interrupt)
    with raises(KeyboardInterrupt):
        r.run(0.1)

    child = int(open("child.pid").read())
    for _ in range(50):
        if not _pid_exists(child):
            break
        time.sleep(0.1)
    assert not _pid_exists(child)
    assert EVENT_ABORT in events
    assert r.running == []


@in_temp_dir
def test_sigterm_cleans_up():
    job = Job("Job1", "sleep 60 & echo $! > child.pid; wait", cores=1, nodes=1)
    old_handler = signal.getsignal(signal.SIGTERM)

    def send_sigterm(t):
        _wait_for_pid_file("child.pid")
        os.kill(os.getpid(), signal.SIGTERM)
        time.sleep(1)

    r = Runner([Node("node1", 1)], {job: []}, ".", sleep=send_sigterm)
    with raises(KeyboardInterrupt):
        r.run(0.1)

    # the original handler is put back afterwards
    assert signal.getsignal(signal.SIGTERM) == old_handler
    child = int(open("child.pid").read())
    for _ in range(50):
        if not _pid_exists(child):
            break
        time.sleep(0.1)
    assert not _pid_exists(child)
