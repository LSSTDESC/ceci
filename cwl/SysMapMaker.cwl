#!/usr/bin/env cwl-runner

arguments:
- {position: -1, valueFrom: -mceci_example.example_stages}
- {position: 0, valueFrom: SysMapMaker}
baseCommand: python3
class: CommandLineTool
cwlVersion: v1.0
doc: ''
id: SysMapMaker
inputs:
  DM:
    doc: Some documentation about the input
    format: TextFile
    inputBinding: {prefix: --DM}
    type: File
label: SysMapMaker
outputs:
  diagnostic_maps:
    doc: Some results produced by the pipeline element
    format: TextFile
    outputBinding: {glob: diagnostic_maps.txt}
    type: File
