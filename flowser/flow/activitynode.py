from .base import Node, SUCCEED, FAILED
from .utils import eval_clspath


class ActivityNode(Node):

    @property
    def active(self):
        return self.ctx.setdefault('_active', {})

    @property
    def done(self):
        return self.ctx.setdefault('_done', {})

    @property
    def failed(self):
        return self.ctx.setdefault('_failed', {})

    def decide(self, task):
        if not (self.active or self.done or self.failed):
            return self._schedule(task)

        lastperactivity = self._collect_activity_events(task)
        for aid, ev in lastperactivity.iteritems():
            data = self.active.pop(aid)
            if ev.type == 'ActivityTaskCompleted':
                self.done[aid] = (ev.attrs.get('result'), data)
            else:
                self.failed[aid] = (ev.type, ev.attrs, data)

        if not self.active:
            self.result = [r[0] for r in self.done.values() if r[0] is not None]
            self.status = FAILED if self.failed else SUCCEED

    def _collect_activity_events(self, task):
        recent = _recent_events(task)
        schidmap = self.ctx.setdefault('_schidmap', {})
        lastperactivity = {}
        for ev in recent:
            if ev.type == 'ActivityTaskScheduled':
                aid = ev.attrs['activityId']
                if aid in self.active:
                    schidmap[ev.id] = aid
            elif ev.type in ('ActivityTaskCompleted',
                             'ActivityTaskFailed',
                             'ActivityTaskTimedOut',
                             'ActivityTaskCanceled'):
                schid = ev.attrs['scheduledEventId']
                if schid in schidmap:
                    aid = schidmap[schid]
                    lastperactivity[aid] = ev

        return lastperactivity

    def _schedule(self, task):
        activity_type = eval_clspath(self.ctx['activity_type'])
        control = self.ctx.get('control')
        for idx, input in enumerate(self._batch_input()):
            activity_id = '%s-%d' % (self.id, idx)
            self.active[activity_id] = input
            task.schedule(activity_type,
                          activity_id=activity_id,
                          input=input,
                          control=control)

    def _batch_input(self):
        return [[i.result for i in self.inputs if i.result is not None]]


def _recent_events(task):
    events = []
    previous = task.previous_started_event_id
    for ev in task.events:
        if ev.id <= previous:
            break
        events.append(ev)
    return events[::-1]


