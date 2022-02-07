from ceci.main import run
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
        
    pipeline.save('test_save2.yml', reduce_config=True)
    
    # Re-read it
    pipe_read2 = Pipeline.read('test_save2.yml')

    # Introspect
    pipe_read2.print_stages()
    pipe_read2.WLGCCov.print_io()



    
if __name__ == "__main__":
    test_snapshot()

