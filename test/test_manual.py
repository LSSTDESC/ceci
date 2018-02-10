from pipette_lib.wl_apps import *
from parsl import App
from parsl.data_provider.files import File

def test_run():
    # Overall input files
    DM = File('test/inputs/dm.txt')
    fiducial_cosmology = File('test/inputs/fid_cosmo.txt')

    # Generated files
    shear_catalog = 'test/outputs/shear_catalog.txt'
    photoz_pdfs = 'test/outputs/photoz_pdfs.txt'
    tomography_catalog = 'test/outputs/tomography_catalog.txt'
    twopoint_data = 'test/outputs/twopoint_data.txt'
    covariance = 'test/outputs/covariance.txt'
    wlgc_summary_data = 'test/outputs/wlgc_summary_data.txt'
    source_summary_data = 'test/outputs/source_summary_data.txt'
    diagnostic_maps = 'test/outputs/diagnostic_maps.txt'
    random_catalog = 'test/outputs/random_catalog.txt'


    print("Launching stage: shearMeasurementPipe")
    future0 = shearMeasurementPipe(inputs=[DM],outputs=[shear_catalog])
    shear_catalog, = future0.outputs

    print("Launching stage: PZEstimationPipe")
    future1 = PZEstimationPipe(inputs=[DM,fiducial_cosmology],outputs=[photoz_pdfs])
    photoz_pdfs, = future1.outputs

    print("Launching stage: SysMapMaker")
    future5 = SysMapMaker(inputs=[DM],outputs=[diagnostic_maps])
    diagnostic_maps, = future5.outputs


    print("Launching stage: WLGCRandoms")
    future2 = WLGCRandoms(inputs=[diagnostic_maps],outputs=[random_catalog])
    random_catalog, = future2.outputs

    print("Launching stage: WLGCSelector")
    future3 = WLGCSelector(inputs=[shear_catalog,photoz_pdfs],outputs=[tomography_catalog])
    tomography_catalog, = future3.outputs

    print("Launching stage: SourceSummarizer")
    future4 = SourceSummarizer(inputs=[tomography_catalog,photoz_pdfs,diagnostic_maps],outputs=[source_summary_data])
    source_summary_data, = future4.outputs


    print("Launching stage: WLGCTwoPoint")
    future6 = WLGCTwoPoint(inputs=[tomography_catalog,shear_catalog,diagnostic_maps,random_catalog],outputs=[twopoint_data])
    twopoint_data, = future6.outputs

    print("Launching stage: WLGCCov")
    future7 = WLGCCov(inputs=[fiducial_cosmology,tomography_catalog,shear_catalog,source_summary_data,diagnostic_maps],outputs=[covariance])
    covariance, = future7.outputs

    print("Launching stage: WLGCSummaryStatistic")
    future8 = WLGCSummaryStatistic(inputs=[twopoint_data,covariance,source_summary_data],outputs=[wlgc_summary_data])
    wlgc_summary_data, = future8.outputs

    future8.result()



if __name__ == '__main__':
    test_run()

