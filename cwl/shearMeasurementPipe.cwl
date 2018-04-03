#!/usr/bin/env cwl-runner

arguments:
- {position: -1, valueFrom: -mceci_example.example_stages}
- {position: 0, valueFrom: shearMeasurementPipe}
baseCommand: python3
class: CommandLineTool
cwlVersion: v1.0
doc: |-
  This pipeline element is a template for a shape measurement tool
id: shearMeasurementPipe
inputs:
  DM:
    doc: Some documentation about the input
    format: TextFile
    inputBinding: {prefix: --DM}
    type: File
  metacalibration:
    default: true
    doc: Some documentation about this parameter
    inputBinding: {prefix: --metacalibration}
    type: boolean
label: shearMeasurementPipe
outputs:
  shear_catalog:
    doc: Some results produced by the pipeline element
    format: TextFile
    outputBinding: {glob: shear_catalog.txt}
    type: File
