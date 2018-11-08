from ceci import PipelineStage
from .types import TextFile, YamlFile

class shearMeasurementPipe(PipelineStage):
    """
    This pipeline element is a template for a shape measurement tool
    """
    name='shearMeasurementPipe'
    inputs = [
        ('DM', TextFile)
        ]
    outputs = [('shear_catalog',TextFile)]
    config_options = {'metacalibration':bool,
                      'apply_flag':bool}

    def run(self):
        # Retrieve configuration:
        my_config = self.config
        print("Here is my configuration :", my_config)

        for inp,_ in self.inputs:
            filename = self.get_input(inp)
            print(f"    shearMeasurementPipe reading from {filename}")
            open(filename)

        for out,_ in self.outputs:
            filename = self.get_output(out)
            print(f"    shearMeasurementPipe writing to {filename}")
            open(filename,'w').write("shearMeasurementPipe was here \n")


class PZEstimationPipe(PipelineStage):
    name='PZEstimationPipe'
    inputs = [('DM',TextFile),('fiducial_cosmology',TextFile)]
    outputs = [('photoz_pdfs',TextFile)]

    def run(self):
        for inp,_ in self.inputs:
            filename = self.get_input(inp)
            print(f"    PZEstimationPipe reading from {filename}")
            open(filename)

        for out,_ in self.outputs:
            filename = self.get_output(out)
            print(f"    PZEstimationPipe writing to {filename}")
            open(filename,'w').write("PZEstimationPipe was here \n")


class WLGCRandoms(PipelineStage):
    name='WLGCRandoms'
    inputs = [('diagnostic_maps',TextFile)]
    outputs = [('random_catalog',TextFile)]

    def run(self):
        for inp,_ in self.inputs:
            filename = self.get_input(inp)
            print(f"    WLGCRandoms reading from {filename}")
            open(filename)

        for out,_ in self.outputs:
            filename = self.get_output(out)
            print(f"    WLGCRandoms writing to {filename}")
            open(filename,'w').write("WLGCRandoms was here \n")


class WLGCSelector(PipelineStage):
    name='WLGCSelector'
    inputs = [('shear_catalog',TextFile),('photoz_pdfs',TextFile)]
    outputs = [('tomography_catalog',TextFile)]
    config_options = {'zbin_edges': [float], 'ra_range':[-5.,5.]}

    def run(self):
        print(self.config)
        for inp,_ in self.inputs:
            filename = self.get_input(inp)
            print(f"    WLGCSelector reading from {filename}")
            open(filename)

        for out,_ in self.outputs:
            filename = self.get_output(out)
            print(f"    WLGCSelector writing to {filename}")
            open(filename,'w').write("WLGCSelector was here \n")


class SourceSummarizer(PipelineStage):
    name='SourceSummarizer'
    inputs = [('tomography_catalog',TextFile),('photoz_pdfs',TextFile),('diagnostic_maps',TextFile)]
    outputs = [('source_summary_data',TextFile)]

    def run(self):
        for inp,_ in self.inputs:
            filename = self.get_input(inp)
            print(f"    SourceSummarizer reading from {filename}")
            open(filename)

        for out,_ in self.outputs:
            filename = self.get_output(out)
            print(f"    SourceSummarizer writing to {filename}")
            open(filename,'w').write("SourceSummarizer was here \n")


class SysMapMaker(PipelineStage):
    name='SysMapMaker'
    inputs = [('DM',TextFile)]
    outputs = [('diagnostic_maps',TextFile)]

    def run(self):
        for inp,_ in self.inputs:
            filename = self.get_input(inp)
            print(f"    SysMapMaker reading from {filename}")
            open(filename)

        for out,_ in self.outputs:
            filename = self.get_output(out)
            print(f"    SysMapMaker writing to {filename}")
            open(filename,'w').write("SysMapMaker was here \n")


class WLGCTwoPoint(PipelineStage):
    name='WLGCTwoPoint'
    inputs = [('tomography_catalog',TextFile),
        ('shear_catalog',TextFile),
        ('diagnostic_maps',TextFile),
        ('random_catalog',TextFile)]
    outputs = [('twopoint_data',TextFile)]

    def run(self):
        for inp,_ in self.inputs:
            filename = self.get_input(inp)
            print(f"    WLGCTwoPoint reading from {filename}")
            open(filename)

        for out,_ in self.outputs:
            filename = self.get_output(out)
            print(f"    WLGCTwoPoint writing to {filename}")
            open(filename,'w').write("WLGCTwoPoint was here \n")


class WLGCCov(PipelineStage):
    name='WLGCCov'
    inputs = [('fiducial_cosmology',TextFile),
            ('tomography_catalog',TextFile),
            ('shear_catalog',TextFile),
            ('source_summary_data',TextFile),
            ('diagnostic_maps',TextFile),
            ]
    outputs = [('covariance',TextFile)]

    def rank_filename(self, rank, size):
        filename = self.get_output('covariance')
        if size==1:
            fname = filename
        else:
            fname = f"{filename}.{rank}"
        return fname


    def run(self):

        # MPI Information
        rank = self.rank
        size = self.size
        comm = self.comm

        for inp,_ in self.inputs:
            filename = self.get_input(inp)
            print(f"    WLGCCov rank {rank}/{size} reading from {filename}")
            open(filename)

        filename = self.get_output('covariance')
        my_filename = self.rank_filename(rank, size)
        print(f"    WLGCCov rank {rank}/{size} writing to {my_filename}")
        open(my_filename,'w').write(f"WLGCCov rank {rank} was here \n")

        #
        if comm:
            comm.Barrier()

        # If needed, concatenate all files
        if rank==0 and size>1:
            f = open(filename,'w')
            print(f"Master process concatenating files:")
            for i in range(size):
                fname = self.rank_filename(i, size)
                print(f"   {fname}")
                content = open(fname).read()
                f.write(content)
            f.close()


class WLGCSummaryStatistic(PipelineStage):
    name='WLGCSummaryStatistic'
    inputs = [('twopoint_data',TextFile),('covariance',TextFile),('source_summary_data',TextFile)]
    outputs = [('wlgc_summary_data',TextFile)]
    parallel = False

    def run(self):
        for inp,_ in self.inputs:
            filename = self.get_input(inp)
            print(f"    WLGCSummaryStatistic reading from {filename}")
            open(filename)

        for out,_ in self.outputs:
            filename = self.get_output(out)
            print(f"    WLGCSummaryStatistic writing to {filename}")
            open(filename,'w').write("WLGCSummaryStatistic was here \n")


if __name__ == '__main__':
    cls = PipelineStage.main()
