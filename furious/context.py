#
# Copyright 2012 WebFilings, LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
Furious context may be used to group a collection of Async tasks together.

Usage:


    with context.new() as current_context:
        # An explicitly constructed Async object may be passed in.
        async_job = Async(some_function,
                          [args, for, other],
                          {'kwargs': 'for', 'other': 'function'},
                          queue='workgroup')
        current_context.add(async_job)

        # Alternatively, add will construct an Async object when given
        # a function path or reference as the first argument.
        async_job = current_context.add(
            another_function,
            [args, for, other],
            {'kwargs': 'for', 'other': 'function'},
            queue='workgroup')

"""

import threading

from google.appengine.api import taskqueue

_local_context = threading.local()


class NotInContextError(Exception):
    """Call that requires context made outside context."""


def new():
    """Get a new furious context and add it to the registry."""
    _init()
    new_context = Context()
    _local_context.registry.append(new_context)
    return new_context


def get_current_async():
    """Return a reference to the currently executing Async job object
    or None if not in an Async job.
    """
    if _local_context._executing_async:
        return _local_context._executing_async

    raise NotInContextError('Not in an executing JobContext.')


def _insert_tasks(tasks, queue, transactional=False):
    """Insert a batch of tasks into the specified queue. If an error occurs
    during insertion, split the batch and retry until they are successfully
    inserted.
    """
    if not tasks:
        return

    try:
        taskqueue.Queue(name=queue).add(tasks, transactional=transactional)
    except (taskqueue.TransientError,
            taskqueue.TaskAlreadyExistsError,
            taskqueue.TombstonedTaskError):
        count = len(tasks)
        if count <= 1:
            return

        _insert_tasks(tasks[:count / 2], queue, transactional)
        _insert_tasks(tasks[count / 2:], queue, transactional)


class Context(object):
    """Furious context object.

    NOTE: Use the module's new function to get a context, do not manually
    instantiate.
    """
    def __init__(self, insert_tasks=_insert_tasks):
        self._tasks = []
        self.insert_tasks = insert_tasks

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._tasks:
            self._handle_tasks()

        return False

    def _handle_tasks(self):
        """Convert all Async's into tasks, then insert them into queues."""
        task_map = self._get_tasks_by_queue()
        for queue, tasks in task_map.iteritems():
            self.insert_tasks(tasks, queue=queue)

    def _get_tasks_by_queue(self):
        """Return the tasks for this Context, grouped by queue."""
        task_map = {}
        for async in self._tasks:
            queue = async.get_queue()
            task = async.to_task()
            task_map.setdefault(queue, []).append(task)
        return task_map

    def add(self, target, args=None, kwargs=None, **options):
        """Add an Async job to this context.

        Takes an Async object or the argumnets to construct an Async
        object as argumnets.  Returns the newly added Async object.
        """
        from .async import Async
        if not isinstance(target, Async):
            target = Async(target, args, kwargs, **options)

        self._tasks.append(target)
        return target


def _init():
    """Initialize the furious context and registry.

    NOTE: Do not directly run this method.
    """
    if hasattr(_local_context, '_initialized'):
        return

    _local_context.registry = []
    _local_context._initialized = True

    return _local_context

_init()  # Initialize the context objects.

