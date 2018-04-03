#!/usr/bin/env cwl-runner

arguments:
- {position: -1, valueFrom: -mceci_example.example_stages}
- {position: 0, valueFrom: PZEstimationPipe}
baseCommand: python3
class: CommandLineTool
cwlVersion: v1.0
doc: ''
id: PZEstimationPipe
inputs:
  DM:
    doc: Some documentation about the input
    format: TextFile
    inputBinding: {prefix: --DM}
    type: File
  fiducial_cosmology:
    doc: Some documentation about the input
    format: TextFile
    inputBinding: {prefix: --fiducial_cosmology}
    type: File
label: PZEstimationPipe
outputs:
  photoz_pdfs:
    doc: Some results produced by the pipeline element
    format: TextFile
    outputBinding: {glob: photoz_pdfs.txt}
    type: File
