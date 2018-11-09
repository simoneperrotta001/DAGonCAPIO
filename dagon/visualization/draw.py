from graphviz import Digraph 

def graph(workflow):
    g = Digraph(workflow.name)
    g.node_attr.update(color='lightblue2', style='filled')
    g.edge("x","z")
    g.edge("x", "y")
    g.view()
