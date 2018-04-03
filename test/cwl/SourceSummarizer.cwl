#!/usr/bin/env cwl-runner

arguments:
- {position: -1, valueFrom: -mceci_example.example_stages}
- {position: 0, valueFrom: SourceSummarizer}
baseCommand: python3
class: CommandLineTool
cwlVersion: v1.0
doc: ''
id: SourceSummarizer
inputs:
  diagnostic_maps:
    doc: Some documentation about the input
    format: TextFile
    inputBinding: {prefix: --diagnostic_maps}
    type: File
  photoz_pdfs:
    doc: Some documentation about the input
    format: TextFile
    inputBinding: {prefix: --photoz_pdfs}
    type: File
  tomography_catalog:
    doc: Some documentation about the input
    format: TextFile
    inputBinding: {prefix: --tomography_catalog}
    type: File
label: SourceSummarizer
outputs:
  source_summary_data:
    doc: Some results produced by the pipeline element
    format: TextFile
    outputBinding: {glob: source_summary_data.txt}
    type: File
