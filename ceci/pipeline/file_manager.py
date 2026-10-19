from collections.abc import MutableMapping
import networkx
from .graph import add_input_node


class FileManager(MutableMapping):
    """Small class to manage files within a particular Pipeline

    This is a mapping from tag to path, but stores no information of its
    own. Instead it is a view onto the file nodes of a pipeline graph,
    reading and writing their "path" and "ftype" attributes.  Setting a
    path for a tag that is not yet in the graph adds it as an input node.

    The tag to path mapping is the thing that the Pipeline uses to set the
    input paths for downstream stages that use the outputs of earlier stages,
    i.e., everything in the pipeline can refer to a particular file by tag.

    The tag defaults to the input or output tag as define in the stage class attributes.
    However, in the case that we want multiple stages of the same class in a pipeline we
    have to alias the tags so that each stage can write to its own location (and so that
    downstream stages can pick up that location correctly)
    """

    def __init__(self, graph=None):
        """Constructor

        Parameters
        ----------
        graph : networkx.DiGraph or None
            The pipeline graph to view. If None, a new empty graph is used.
        """
        self.graph = networkx.DiGraph() if graph is None else graph

    def _file_node(self, tag):
        node = self.graph.nodes.get(tag)
        if node is None or node["type"] == "stage":
            raise KeyError(tag)
        return node

    def __getitem__(self, tag):
        node = self._file_node(tag)
        if "path" not in node:
            raise KeyError(tag)
        return node["path"]

    def __setitem__(self, tag, path):
        # Unlike insert, this sets the path even if it is None
        self.insert(tag)
        self._file_node(tag)["path"] = path

    def __delitem__(self, tag):
        node = self._file_node(tag)
        if "path" not in node:
            raise KeyError(tag)
        del node["path"]

    def __iter__(self):
        for tag, node in self.graph.nodes(data=True):
            if node["type"] != "stage" and "path" in node:
                yield tag

    def __len__(self):
        return sum(1 for _ in self)

    def __repr__(self):
        return f"FileManager({dict(self)})"

    def copy(self):
        """Return a plain dictionary copy of the tag to path mapping"""
        return dict(self)

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
        if tag not in self.graph.nodes:
            add_input_node(self.graph, tag)
        node = self._file_node(tag)
        if path is not None:
            node["path"] = path
        if node.get("ftype") is None:
            node["ftype"] = ftype

    def get_type(self, tag):
        """Return the file type associated to a given tag"""
        return self._file_node(tag).get("ftype")

    def get_path(self, tag):
        """Return the path associated to a give tag"""
        return self[tag]

    def get_tag(self, path):
        """Return the tag associated to a given path"""
        for tag, node_path in self.items():
            if node_path is path or node_path == path:
                return tag
        raise KeyError(path)

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
