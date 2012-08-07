from json import loads, dumps
from .utils import freeze, unfreeze


__all__ = ['INACTIVE', 'ACTIVE', 'DONE', 'SUCCEED', 'FAILED', 'CANCELED',
           'Flow', 'Node']

INACTIVE = 1
ACTIVE = 2
DONE = 4
SUCCEED = 8 | DONE
FAILED = 16 | DONE
CANCELED = 32 | DONE


class Flow(object):

    def __init__(self, props=None):
        self.idx = 0
        self.props = props or {}
        self.nodes = {}

    def regnode(self, node, id=None):
        """Register the node within this desicion flow"""
        nid = id or self._nodeid(node)
        self.nodes[nid] = node
        return nid

    def _nodeid(self, node):
        self.idx += 1
        name = node.__class__.__name__.lower()
        return '%s-%d' % (name, self.idx)

    def decide(self, task):
        """Send decision events to nodes in ACTIVE status"""
        seen = set()
        active = self._active()
        pending = active - seen
        while pending:
            for n in pending:
                seen.add(n)
                n.decide(task)
            pending = self._active() - seen

        if not self._active() and not task.decisions._data:
            task.workflow_execution.complete('UNKNOWN')

    def _active(self):
        active = set()
        for n in self.nodes.values():
            if not n.inputs and n.status == INACTIVE:
                n.status = ACTIVE
                active.add(n)
            elif n.status == ACTIVE:
                active.add(n)
        return active

    def copy(self):
        return unfreeze(loads(dumps(freeze(self))))


class Node(object):

    def __init__(self, flow, id=None, **ctx):
        self.flow = flow
        self.ctx = ctx
        self.result = None
        self.inputs = set()
        self.outputs = set()
        self._status = INACTIVE
        self.id = flow.regnode(self, id)

    def connect(self, output):
        self.outputs.add(output)
        output.inputs.add(self)
        return output

    def _statusset(self, new):
        if self._status == new:
            return
        # status changed
        self._status = new
        # notify outputs that one of its inputs changed
        for node in self.outputs:
            node.on_input_status_change(self)
        # notify inputs that one of its outputs changed
        for node in self.inputs:
            node.on_output_status_change(self)

    status = property(lambda x:x._status, _statusset)

    def on_input_status_change(self, input):
        if self.status & DONE:
            return
        elif input.status == FAILED:
            self.status = FAILED
        elif all(i.status == SUCCEED for i in self.inputs):
            self.status = ACTIVE

    def on_output_status_change(self, output):
        pass

    def decide(self, task):
        self.status = SUCCEED
