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

import time

from boto.swf.exceptions import SWFTypeAlreadyExistsError
from boto.swf.layer1_decisions import Layer1Decisions

from flowser import serializing
from flowser.exceptions import Error
from flowser.exceptions import EmptyTaskPollResult

ONE_HOUR = 60 * 60
ONE_DAY = ONE_HOUR * 24


def _raise_if_empty_poll_result(result):
    """Return result or raise ``EmptyTaskPollResult``. """
    if 'taskToken' not in result:
        raise EmptyTaskPollResult('empty result (no task token)')
    return result


class Type(object):
    """Base class for Simple Workflow types (activities, workflows).

    Subclasses must set ``name``, ``version`` and ``task_list`` properties.
    """

    # Override this in a subclass to the name (string) of a register method on
    # the connection object (as returned by boto.connect_swf).
    _reg_func_name = None

    def __init__(self, domain):
        for needed_prop in ['name', 'task_list', 'version']:
            if not hasattr(self, needed_prop):
                raise Error(needed_prop)
        self._domain = domain
        self._conn = domain.conn

    def _register(self, raise_exists=False):
        assert self._reg_func_name is not None, "no reg func configured"
        reg_func = getattr(self._conn, self._reg_func_name)
        try:
            reg_func(self._domain.name, self.name, self.version)
        except SWFTypeAlreadyExistsError:
            if raise_exists:
                raise Error(self)

    def _poll_for_activity_task(self, identity=None):
        """Low-level wrapper for boto's method with the same name. 

        This method raises an exception if no task is returned.

        :raises: EmptyTaskPollResult
        """
        result = self._conn.poll_for_activity_task(
                self._domain.name, self.task_list, identity)
        return _raise_if_empty_poll_result(result)

    def _poll_for_decision_task(self, identity=None, maximum_page_size=None, 
                               next_page_token=None, reverse_order=None):
        """Low-level wrapper for boto's method with the same name. 

        This method raises an exception if no task is returned.

        :raises: EmptyTaskPollResult
        """
        result = self._conn.poll_for_decision_task( 
                self._domain.name, self.task_list, identity, maximum_page_size, 
                next_page_token, reverse_order)
        return _raise_if_empty_poll_result(result)


class Activity(Type):
    """Base class for activity types. 
    
    Subclasses must set ``name``, ``task_list`` and ``version`` properties and
    implement a ``schedule`` class method.
    """

    _reg_func_name = 'register_activity_type'

    heartbeat_timeout = str(ONE_HOUR)
    schedule_to_close_timeout = str(ONE_HOUR)
    schedule_to_start_timeout = str(ONE_HOUR)
    start_to_close_timeout = str(ONE_HOUR)

    @classmethod
    def schedule(cls, activity_id, input, control=None):
        "Called from subclasses' ``schedule`` class method. "
        if control is not None:
            control = serializing.dumps(control)

        l1d = Layer1Decisions()
        l1d.schedule_activity_task(
            activity_id=activity_id,
            activity_type_name=cls.name,
            activity_type_version=cls.version,
            task_list=cls.task_list,
            control=control,
            heartbeat_timeout=cls.heartbeat_timeout,
            schedule_to_close_timeout=cls.schedule_to_close_timeout,
            schedule_to_start_timeout=cls.schedule_to_start_timeout,
            start_to_close_timeout=cls.start_to_close_timeout,
            input=serializing.dumps(input),
        )
        return l1d._data[0]


class Workflow(Type):
    """Base class for workflow types.

    Subclasses must set ``name`` and ``task_list`` properties and implement 
    a ``start`` method and a ``start_child`` class method.
    """

    _reg_func_name = 'register_workflow_type'

    # These may be overridden in subclasses.
    execution_start_to_close_timeout = '600'
    task_start_to_close_timeout = '120'
    child_policy = 'TERMINATE'
    default_filter_tag = None
    default_tag_list = None

    @classmethod
    def _get_static_child_start_attrs(cls):
        attrs = {}
        attrs['childPolicy'] = cls.child_policy
        attrs['executionStartToCloseTimeout'] = \
                cls.execution_start_to_close_timeout
        attrs['workflowType'] = {'name': cls.name, 'version': cls.version}
        if cls.default_tag_list:
            attrs['tagList'] = cls.default_tag_list
        attrs['taskList'] = {'name': cls.task_list}
        attrs['taskStartToCloseTimeout'] = \
                cls.task_start_to_close_timeout
        return attrs

    def _list_open(self, latest_date=None, oldest_date=None):
        if latest_date is None:
            latest_date = time.time()
        if oldest_date is None:
            oldest_date = latest_date - ONE_DAY
        return self._conn.list_open_workflow_executions(
                self._domain.name,
                latest_date=latest_date,
                oldest_date=oldest_date,
                workflow_name=self.name,
                tag=self.default_filter_tag)

    def _list_closed(self, start_latest_date=None, start_oldest_date=None):
        if start_latest_date is None:
            start_latest_date = time.time()
        if start_oldest_date is None:
            start_oldest_date = start_latest_date - ONE_DAY
        return self._conn.list_closed_workflow_executions(
                self._domain.name,
                start_latest_date=start_latest_date,
                start_oldest_date=start_oldest_date,
                workflow_name=self.name,
                tag=self.default_filter_tag)

    def _start(self, workflow_id, input):
        """Start workflow execution"""
        return self._conn.start_workflow_execution(
            domain=self._domain.name,
            workflow_id=workflow_id,
            workflow_name=self.name,
            workflow_version=self.version,
            task_list=self.task_list,
            child_policy=self.child_policy,
            execution_start_to_close_timeout=self.execution_start_to_close_timeout,
            input=serializing.dumps(input), 
            tag_list=self.default_tag_list, # XXX: name missmatch
            task_start_to_close_timeout=self.task_start_to_close_timeout,
        )

    @classmethod
    def start_child(cls, workflow_id, input, control=None):
        """Start child workflow execution"""
        if control is not None:
            control = serializing.dumps(control)

        l1d = Layer1Decisions()
        l1d.start_child_workflow_execution(
            workflow_type_name=cls.name,
            workflow_type_version=cls.version,
            child_policy=cls.child_policy,
            control=control,
            execution_start_to_close_timeout=cls.execution_start_to_close_timeout,
            input=serializing.dumps(input),
            tag_list=cls.tag_list,
            task_list=cls.task_list,
            task_start_to_close_timeout=cls.task_start_to_close_timeout,
        )
        # Unreleased bugfix in boto
        # https://github.com/boto/boto/commit/d5602f7299a919eceded11ba6da438543609c6db#L0R272
        l1d._data[0]['startChildWorkflowExecutionDecisionAttributes']['workflowId'] = workflow_id
        return l1d._data[0]
