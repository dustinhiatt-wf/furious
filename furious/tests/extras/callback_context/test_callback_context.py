import mock

import unittest

from ....extras.callback_context import CallbackAsync, CallbackContext
from ....extras.callback_context import CallbackContextPersistenceEngine
from ....extras.callback_context import _notify_async_completed
from ....job_utils import reference_to_path


class TestCreateCallbackContext(unittest.TestCase):
    def test_create_callback_context(self):
        """Ensures a callback context is created correctly."""
        context = CallbackContext()
        self.assertTrue(
            context._persistence_engine,
            CallbackContextPersistenceEngine
        )

    def test_add_target_to_context(self):
        """Ensures a callback async is created when we add a target to a
        callback context."""
        context = CallbackContext()
        context.add('target')
        tasks = context._tasks
        self.assertEqual(1, len(tasks))
        async = tasks[0]
        self.assertTrue(isinstance(async, CallbackAsync))
        callbacks = async.get_callbacks()
        self.assertDictEqual(
            {
                'success': reference_to_path(_notify_async_completed)
            },
            callbacks
        )

    def test_simple_context_to_dict(self):
        """Ensures the basic to_dict functionality works."""
        context = CallbackContext()
        context_dct = context.to_dict()
        self.assertListEqual([], context_dct['_completed_task_ids'])

    def test_to_dict_with_on_complete_added(self):
        """Ensures we go to a dict correctly with on_complete."""
        context = CallbackContext()
        func_tool = lambda: None
        context.on_complete(func_tool, 'test', foo='bar')
        context_dict = context.to_dict()
        self.assertIsNotNone(context_dict['_on_complete'])
        self.assertTupleEqual(('test',), context_dict['_on_complete_args'])
        self.assertDictEqual(
            {
                'foo': 'bar'
            },
            context_dict['_on_complete_kwargs']
        )

    def test_add_on_complete_without_args_and_kwargs(self):
        """Ensure on_complete handles a None args and kwargs case."""
        context = CallbackContext()
        func = lambda: None
        context.on_complete(func)
        context_dict = context.to_dict()
        self.assertTupleEqual((), context_dict['_on_complete_args'])
        self.assertDictEqual({}, context_dict['_on_complete_kwargs'])

    def test_start_triggers_persistence_engine_save_of_context(self):
        """Ensures context gets saved when context is started."""
        persistence_engine = mock.Mock()
        persistence_engine.func_name = 'persistence_engine'
        persistence_engine.im_class.__name__ = 'engine'

        context = CallbackContext(persistence_engine=persistence_engine)
        context.start()

        persistence_engine.store_context.assert_called_once_with(
            context.id,
            context.to_dict()
        )
