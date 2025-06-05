class FileManager(dict):
    """Small class to manage files within a particular Pipeline

    This is a dict which is used for tag to path mapping,
    but has a couple of additional dicts to manage the reverse mapping and the
    tag to type mapping.


    The tag to path mapping is the thing that the Pipeline uses to set the
    input paths for downstream stages that use the outputs of earlier stages,
    i.e., everything in the pipeline can refer to a particular file by tag.

    The tag defaults to the input or output tag as define in the stage class attributes.
    However, in the case that we want multiple stages of the same class in a pipeline we
    have to alias the tags so that each stage can write to its own location (and so that
    downstream stages can pick up that location correctly)
    """

    def __init__(self):
        """Constructor, makes empty dictionaries"""
        self._tag_to_type = {}
        self._path_to_tag = {}
        dict.__init__(self)

    def __setitem__(self, tag, path):
        """Override dict.__setitem__() to also insert the reverse mapping"""
        dict.__setitem__(self, tag, path)
        self._path_to_tag[path] = tag

    def insert(self, tag, path=None, ftype=None):
        """Insert a file, including the path and the file type

        Parameters
        ----------
        tag : str
            The tag by which this file will be identified
        path : str
            The path to this file
        ftype : type
            The file type for this file
        """
        if path is not None:
            self[tag] = path
            self._path_to_tag[path] = tag
        if tag not in self._tag_to_type:
            self._tag_to_type[tag] = ftype

    def get_type(self, tag):
        """Return the file type associated to a given tag"""
        return self._tag_to_type[tag]

    def get_path(self, tag):
        """Return the path associated to a give tag"""
        return self[tag]

    def get_tag(self, path):
        """Return the tag associated to a given path"""
        return self._path_to_tag[path]

    def insert_paths(self, path_dict):
        """Insert a set of paths from a dict that has tag, path pairs"""
        for key, val in path_dict.items():
            self.insert(key, path=val)

    def insert_outputs(self, stage, outdir):
        """Insert a set of tags and associated paths and file types that are output by a stage"""
        stage_outputs = stage.find_outputs(outdir)
        for tag, ftype in stage.outputs:
            aliased_tag = stage.get_aliased_tag(tag)
            path = stage_outputs[aliased_tag]
            self.insert(aliased_tag, path=path, ftype=ftype)
        return stage_outputs

