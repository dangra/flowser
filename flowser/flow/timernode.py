from .base import Node, SUCCEED, FAILED


class TimerNode(Node):

    def decide(self, task):
        if 'fired' not in self.ctx:
            self.ctx['fired'] = 1
            return self._start_timer(task)

        timer_id = self.ctx['timer_id']
        if any(e.attrs['timerId'] == timer_id for e in task.filter('TimerFired')):
            self.status = SUCCEED
        elif any(e.attrs['timerId'] == timer_id for e in task.filter('StartTimerFailed')):
            self.status = FAILED

    def _start_timer(self, task):
        timer_id = self.ctx.setdefault('timer_id', self.id)
        timeout = str(self.ctx.get('start_to_fire_timeout', 5))
        control = self.ctx.get('control')
        task.decisions.start_timer(start_to_fire_timeout=timeout,
                                   timer_id=timer_id,
                                   control=control)


