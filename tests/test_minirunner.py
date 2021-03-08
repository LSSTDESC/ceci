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
