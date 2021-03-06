# Copyright (c) 2012 Memoto AB
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to
# deal in the Software without restriction, including without limitation the
# rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
# sell copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
# THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

"""Tests for Flowser.

The environment variable FLOWSER_TEST_DOMAIN must be set.

Example run:

    $ FLOWSER_TEST_DOMAIN=flowser python tests.py

"""
import os
import unittest
from uuid import uuid4
import threading
import logging
import sys

import boto

import flowser

TEST_DOMAIN = os.environ.get('FLOWSER_TEST_DOMAIN', None)
if_environment = unittest.skipIf(not TEST_DOMAIN, 'FLOWSER_TEST_DOMAIN unset')


class ArithmeticWorkflow(flowser.types.Workflow):
    """Workflow for performing arithmetics. """
    name = 'ArithmeticWorkflow'
    version = '1.0.0'
    task_list = 'mainTaskList'


class MultiplyActivity(flowser.types.Activity):
    name = 'MultiplyActivity'
    version = '1.0.0'
    task_list = 'Multiply'


class SumActivity(flowser.types.Activity):
    name = 'SumActivity'
    version = '1.0.0'
    task_list = 'Sum'


class Thread(threading.Thread):
    """Thread with a domain and logger used in tests.
    """

    def __init__(self, domain):
        super(Thread, self).__init__()
        self.domain = domain
        self.log = logging.getLogger('flowsertest.thread')


class ArithmeticWorkflowDecider(Thread):

    def run(self):
        op_to_activity = {
                'multiply': MultiplyActivity,
                'sum': SumActivity,
                }
        for task in self.domain.decisions(ArithmeticWorkflow):
            is_new = len(task.filter('DecisionTaskScheduled')) == 1
            if is_new:
                for op_id, op, input in task.start_input['operations']:
                    activity = op_to_activity[op]
                    task_id = str(uuid4())
                    task.schedule(activity, task_id, {
                        'operation': [op_id, input],
                    })
            else:
                op_ids = set([x[0] for x in task.start_input['operations']])
                results = {}
                for ev in task.filter('ActivityTaskCompleted'):
                    result = ev.attrs['result']
                    results[result[0]] = result[1]
                result_ids = set(results.keys())

                got_all = op_ids == result_ids
                if got_all:
                    task.workflow_execution.complete(results)
                    self.result = results
                    break

            task.complete()


class WorkerThread(Thread):

    def __init__(self, domain, break_after=1):
        super(WorkerThread, self).__init__(domain)
        self.handled_count = 0
        self.break_after = break_after

    def run(self):
        for task in self.domain.activities(self.activity_class):
            self.handled_count += 1
            self.handle_task(task)
            if self.handled_count == self.break_after:
                break


class MultiplyWorker(WorkerThread):

    activity_class = MultiplyActivity

    def handle_task(self, task):
        op = task.input['operation']
        result = reduce(lambda a, b: a * b, op[1])
        task.complete([op[0], result])


class SumWorker(WorkerThread):

    activity_class = SumActivity

    def handle_task(self, task):
        op = task.input['operation']
        task.complete([op[0], sum(op[1])])


class TestDomain(flowser.Domain):
    name = TEST_DOMAIN
    retention_period = '1'
    workflow_types = [ArithmeticWorkflow]
    activity_types = [MultiplyActivity, SumActivity]


class FlowserTestCase(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        if TEST_DOMAIN is None:
            return
        cls.domain = TestDomain(boto.connect_swf())
        cls.log = logging.getLogger('flowsertest.testcase')

        # This tests registration of domains, workflows and activites.
        cls.domain.register(raise_exists=False)

    @if_environment
    def test_workflow_and_activities(self):
        MultiplyWorker(self.domain).start()
        SumWorker(self.domain).start()
        decider = ArithmeticWorkflowDecider(self.domain)
        decider.start()

        workflow_id = str(uuid4())
        arithmetic_input = {
            'operations': [
                ['mult_id', 'multiply', [1, 2, 3]],
                ['sum_id', 'sum', [1, 2, 3, 4]],
            ]
        }
        self.domain.start(ArithmeticWorkflow, workflow_id, arithmetic_input)

        decider.join()
        self.assertEqual(decider.result['mult_id'], 6)
        self.assertEqual(decider.result['sum_id'], 10)


if __name__ == '__main__':
    logging.basicConfig(stream=sys.stderr)
    logging.getLogger("flowsertest").setLevel(logging.DEBUG)
    unittest.main()
