#!/usr/bin/env cwl-runner

arguments:
- {position: -1, valueFrom: -mceci_example.example_stages}
- {position: 0, valueFrom: WLGCSelector}
baseCommand: python3
class: CommandLineTool
cwlVersion: v1.0
doc: ''
id: WLGCSelector
inputs:
  photoz_pdfs:
    doc: Some documentation about the input
    format: TextFile
    inputBinding: {prefix: --photoz_pdfs}
    type: File
  shear_catalog:
    doc: Some documentation about the input
    format: TextFile
    inputBinding: {prefix: --shear_catalog}
    type: File
label: WLGCSelector
outputs:
  tomography_catalog:
    doc: Some results produced by the pipeline element
    format: TextFile
    outputBinding: {glob: tomography_catalog.txt}
    type: File
