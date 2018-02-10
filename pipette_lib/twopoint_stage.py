#!/usr/bin/env python3
import pipette
import time
import sys

class Stage(pipette.PipelineStage):
    name='MakeTwoPoint'
    inputs = ["filename"]
    outputs = ["countfile"]
    
    def run(self):
        filename = self.get_input("filename")
        outfile = self.get_output("countfile")

        print(f"Opening {filename}")
        text = open(filename).read()
        count = len(text.split())
        print(f"Length = {count}")
        open(outfile,'w').write(f"{count}\n")



if __name__ == '__main__':
    Stage.main()