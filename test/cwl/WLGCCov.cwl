#!/usr/bin/env cwl-runner

arguments:
- {position: -1, valueFrom: -mceci_example.example_stages}
- {position: 0, valueFrom: WLGCCov}
baseCommand: python3
class: CommandLineTool
cwlVersion: v1.0
doc: ''
id: WLGCCov
inputs:
  diagnostic_maps:
    doc: Some documentation about the input
    format: TextFile
    inputBinding: {prefix: --diagnostic_maps}
    type: File
  fiducial_cosmology:
    doc: Some documentation about the input
    format: TextFile
    inputBinding: {prefix: --fiducial_cosmology}
    type: File
  shear_catalog:
    doc: Some documentation about the input
    format: TextFile
    inputBinding: {prefix: --shear_catalog}
    type: File
  source_summary_data:
    doc: Some documentation about the input
    format: TextFile
    inputBinding: {prefix: --source_summary_data}
    type: File
  tomography_catalog:
    doc: Some documentation about the input
    format: TextFile
    inputBinding: {prefix: --tomography_catalog}
    type: File
label: WLGCCov
outputs:
  covariance:
    doc: Some results produced by the pipeline element
    format: TextFile
    outputBinding: {glob: covariance.txt}
    type: File
