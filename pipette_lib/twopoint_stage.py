#!/usr/bin/env python3
import pipette
import time
import sys

class Stage(pipette.PipelineStage):
    name='MakeTwoPoint'
    inputs = ["filename"]
    outputs = ["countfile"]
    
    def run(self):
        filename = self.input_values[0]
        print(f"Opening {filename}")
        text = open(filename).read()
        count = len(text.split())
        print(f"Length = {count}")



if __name__ == '__main__':
    Stage.main()