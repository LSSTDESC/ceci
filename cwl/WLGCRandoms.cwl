#!/usr/bin/env cwl-runner

arguments:
- {position: -1, valueFrom: -mceci_example.example_stages}
- {position: 0, valueFrom: WLGCRandoms}
baseCommand: python3
class: CommandLineTool
cwlVersion: v1.0
doc: ''
id: WLGCRandoms
inputs:
  diagnostic_maps:
    doc: Some documentation about the input
    format: TextFile
    inputBinding: {prefix: --diagnostic_maps}
    type: File
label: WLGCRandoms
outputs:
  random_catalog:
    doc: Some results produced by the pipeline element
    format: TextFile
    outputBinding: {glob: random_catalog.txt}
    type: File
