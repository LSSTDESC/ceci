from ceci.main import run
from parsl import clear
import tempfile
import os
import pytest
import subprocess

from ceci.pipeline import Pipeline


def test_snapshot():

    # Read the pipeline
    pipeline = Pipeline.read('tests/test.yml')

    # Save it
    pipeline.save('test_save.yml')

    # Re-read it
    pipe_read = Pipeline.read('test_save.yml')

    # Introspect
    pipe_read.print_stages()
    pipe_read.WLGCCov.print_io()

    # And run
    pipe_read.run()
        

if __name__ == "__main__":
    test_snapshot()

