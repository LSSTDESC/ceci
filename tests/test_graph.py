from ceci.pipeline.graph import trim_pipeline_graph
import networkx


def check_membership(graph, everything, expected):
    for node in everything:
        if node in expected:
            assert node in graph, f"Expected node {node} to be in graph"
        else:
            assert node not in graph, f"Did not expect node {node} to be in graph"


def test_trim_pipeline_graph():
    # Create a simple graph
    # This doesn't exactly match what pipeline graphs
    # can look like because it has a stage connecting directly
    # to another stage, but it's a good test of the function

    graph = networkx.DiGraph()

    # Mock overall inputs
    graph.add_node("input_a", type="input")
    graph.add_node("input_b", type="input")
    graph.add_node("input_c", type="input")

    # Some stages
    graph.add_node("stage_1", type="stage")
    graph.add_node("stage_2", type="stage")
    graph.add_node("stage_3", type="stage")
    graph.add_node("stage_4", type="stage")

    # Outputs
    graph.add_node("output_w", type="output")
    graph.add_node("output_x", type="output")
    graph.add_node("output_y", type="output")
    graph.add_node("output_z", type="output")

    # Some mock edges
    graph.add_edge("input_a", "stage_1")
    graph.add_edge("input_b", "stage_1")
    graph.add_edge("input_b", "stage_2")
    graph.add_edge("input_c", "stage_3")

    graph.add_edge("stage_1", "output_w")
    graph.add_edge("stage_2", "output_x")
    graph.add_edge("stage_3", "output_y")
    graph.add_edge("stage_4", "output_z")
    graph.add_edge("stage_1", "stage_4")

    everything = graph.nodes

    sub1, _ = trim_pipeline_graph(graph, to_="output_z")
    expected1 = {
        "output_z",
        "stage_4",
        "stage_1",
        "output_w",
        "input_a", 
        "input_b",
    }

    check_membership(sub1, everything, expected1)

    sub2, _ = trim_pipeline_graph(graph, from_="stage_2")
    expected2 = {
        "input_b",
        "stage_2",
        "output_x"
    }
    check_membership(sub2, everything, expected2)

    # if we trim nothing then we expect to get everything back
    sub3, _ = trim_pipeline_graph(graph)
    expected3 = everything
    check_membership(sub3, everything, expected3)

    # check using both. add a few more steps first to be a more useful test
    graph.add_node("stage_5", type="stage")
    graph.add_node("output_u", type="output")
    graph.add_node("stage_6", type="stage")
    graph.add_node("output_v", type="output")

    graph.add_edge("output_z", "stage_5")
    graph.add_edge("stage_5", "output_u")
    graph.add_edge("output_u", "stage_6")
    graph.add_edge("stage_6", "output_v")


    everything = graph.nodes

    sub4, _ = trim_pipeline_graph(graph, from_="stage_4", to_="output_u")
    expected4 = {
        "stage_4",
        "output_z",
        "stage_5",
        "output_u"
    }

    check_membership(sub4, everything, expected4)
