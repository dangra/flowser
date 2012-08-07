import logging

__all__ = ['freeze', 'unfreeze', 'get_clspath', 'eval_clspath']

logger = logging.getLogger('flowser.flow.utils')

def get_clspath(o):
    return '%s.%s' % (o.__class__.__module__, o.__class__.__name__)

def eval_clspath(clspath):
    modulepath, _, clsname = clspath.rpartition('.')
    module = __import__(modulepath, fromlist=[clsname])
    return getattr(module, clsname)

def freeze(flow):
    _nfreeze = lambda node:{
        'id': node.id,
        'cls': get_clspath(node),
        'inputs': [i.id for i in node.inputs],
        'outputs': [o.id for o in node.outputs],
        'status': node.status,
        'result': node.result,
        'ctx': node.ctx,
    }
    return {
        'cls': get_clspath(flow),
        'idx': flow.idx,
        'props': flow.props,
        'nodes': [_nfreeze(n) for n in flow.nodes.itervalues()],
    }


def unfreeze(state):
    cls = eval_clspath(state['cls'])
    flow = cls(props=state['props'])
    flow.idx = state['idx']
    # Unfreeze, register and collect connection between nodes
    connections = []
    for nodestate in state['nodes']:
        nodecls = eval_clspath(nodestate['cls'])
        node = nodecls(flow, id=nodestate['id'])
        node.ctx = nodestate['ctx']
        node.result = nodestate['result']
        connections.append((node, nodestate['outputs']))

    # Create connections now that all nodes are instanciated
    for node, outputids in connections:
        for oid in outputids:
            output = flow.nodes[oid]
            node.connect(output)

    return flow
