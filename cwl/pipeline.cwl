#!/usr/bin/env cwl-runner

class: Workflow
cwlVersion: v1.0
inputs:
  DM: {doc: some documentation about the input, format: TextFile, id: DM, label: DM,
    type: File}
  fiducial_cosmology: {doc: some documentation about the input, format: TextFile,
    id: fiducial_cosmology, label: fiducial_cosmology, type: File}
outputs:
  wlgc_summary_data: {format: TextFile, id: wlgc_summary_data, label: wlgc_summary_data,
    outputSource: WLGCSummaryStatistic/wlgc_summary_data, type: File}
steps:
  PZEstimationPipe:
    id: PZEstimationPipe
    in: {DM: DM, fiducial_cosmology: fiducial_cosmology}
    out: [photoz_pdfs]
    run: PZEstimationPipe.cwl
  SourceSummarizer:
    id: SourceSummarizer
    in: {diagnostic_maps: SysMapMaker/diagnostic_maps, photoz_pdfs: PZEstimationPipe/photoz_pdfs,
      tomography_catalog: WLGCSelector/tomography_catalog}
    out: [source_summary_data]
    run: SourceSummarizer.cwl
  SysMapMaker:
    id: SysMapMaker
    in: {DM: DM}
    out: [diagnostic_maps]
    run: SysMapMaker.cwl
  WLGCCov:
    id: WLGCCov
    in: {diagnostic_maps: SysMapMaker/diagnostic_maps, fiducial_cosmology: fiducial_cosmology,
      shear_catalog: shearMeasurementPipe/shear_catalog, source_summary_data: SourceSummarizer/source_summary_data,
      tomography_catalog: WLGCSelector/tomography_catalog}
    out: [covariance]
    run: WLGCCov.cwl
  WLGCRandoms:
    id: WLGCRandoms
    in: {diagnostic_maps: SysMapMaker/diagnostic_maps}
    out: [random_catalog]
    run: WLGCRandoms.cwl
  WLGCSelector:
    id: WLGCSelector
    in: {photoz_pdfs: PZEstimationPipe/photoz_pdfs, shear_catalog: shearMeasurementPipe/shear_catalog}
    out: [tomography_catalog]
    run: WLGCSelector.cwl
  WLGCSummaryStatistic:
    id: WLGCSummaryStatistic
    in: {covariance: WLGCCov/covariance, source_summary_data: SourceSummarizer/source_summary_data,
      twopoint_data: WLGCTwoPoint/twopoint_data}
    out: [wlgc_summary_data]
    run: WLGCSummaryStatistic.cwl
  WLGCTwoPoint:
    id: WLGCTwoPoint
    in: {diagnostic_maps: SysMapMaker/diagnostic_maps, random_catalog: WLGCRandoms/random_catalog,
      shear_catalog: shearMeasurementPipe/shear_catalog, tomography_catalog: WLGCSelector/tomography_catalog}
    out: [twopoint_data]
    run: WLGCTwoPoint.cwl
  shearMeasurementPipe:
    id: shearMeasurementPipe
    in: {DM: DM}
    out: [shear_catalog]
    run: shearMeasurementPipe.cwl
