#!/usr/bin/env cwl-runner

arguments:
- {position: -1, valueFrom: -mceci_example.example_stages}
- {position: 0, valueFrom: WLGCSummaryStatistic}
baseCommand: python3
class: CommandLineTool
cwlVersion: v1.0
doc: ''
id: WLGCSummaryStatistic
inputs:
  covariance:
    doc: Some documentation about the input
    format: TextFile
    inputBinding: {prefix: --covariance}
    type: File
  source_summary_data:
    doc: Some documentation about the input
    format: TextFile
    inputBinding: {prefix: --source_summary_data}
    type: File
  twopoint_data:
    doc: Some documentation about the input
    format: TextFile
    inputBinding: {prefix: --twopoint_data}
    type: File
label: WLGCSummaryStatistic
outputs:
  wlgc_summary_data:
    doc: Some results produced by the pipeline element
    format: TextFile
    outputBinding: {glob: wlgc_summary_data.txt}
    type: File
