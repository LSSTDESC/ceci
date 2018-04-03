#!/usr/bin/env cwl-runner

arguments:
- {position: -1, valueFrom: -mceci_example.example_stages}
- {position: 0, valueFrom: WLGCTwoPoint}
baseCommand: python3
class: CommandLineTool
cwlVersion: v1.0
doc: ''
id: WLGCTwoPoint
inputs:
  diagnostic_maps:
    doc: Some documentation about the input
    format: TextFile
    inputBinding: {prefix: --diagnostic_maps}
    type: File
  random_catalog:
    doc: Some documentation about the input
    format: TextFile
    inputBinding: {prefix: --random_catalog}
    type: File
  shear_catalog:
    doc: Some documentation about the input
    format: TextFile
    inputBinding: {prefix: --shear_catalog}
    type: File
  tomography_catalog:
    doc: Some documentation about the input
    format: TextFile
    inputBinding: {prefix: --tomography_catalog}
    type: File
label: WLGCTwoPoint
outputs:
  twopoint_data:
    doc: Some results produced by the pipeline element
    format: TextFile
    outputBinding: {glob: twopoint_data.txt}
    type: File
