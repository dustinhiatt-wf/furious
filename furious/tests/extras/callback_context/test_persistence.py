import mock

import unittest

from ....extras.callback_context import _notify_async_completed
from ....extras.callback_context import CallbackAsync, CallbackContext
from ....extras.callback_context import InvalidCallbackAsyncError
from ....extras.callback_context import OngoingCallbackContext
from ....job_utils import path_to_reference, reference_to_path


CALLED = False
def ensure_complete(*args, **kwargs):
    global CALLED
    CALLED = True


class TestPersistence(unittest.TestCase):
    def setUp(self):
        self.get_current_mock = mock.patch('furious.extras.callback_context'
                                           '.get_current_async')
        self.get_current = self.get_current_mock.start()

        self.ndb_transaction_mock = mock.patch('google.appengine.ext'
                                               '.ndb.transaction')
        self.ndb_transaction = self.ndb_transaction_mock.start()

        self.context = CallbackContext()

        self.ongoing_context = OngoingCallbackContext(
            options=self.context.to_dict()
        )

    def tearDown(self):
        self.get_current_mock.stop()
        self.ndb_transaction_mock.stop()
        global CALLED
        CALLED = False

    @mock.patch('furious.extras.callback_context.OngoingCallbackContext'
                '.get_or_insert')
    def test_get_or_insert_called(self, ongoing):
        """Ensures a persistence event is called when a context is started."""
        context = CallbackContext()
        context.persist()
        ongoing.assert_called_once_with(
            context.id,
            option=context.to_dict()
        )

    def test_notify_async_complete(self):
        """Ensures get async called and function behaves correctly"""
        self.get_current.return_value = CallbackAsync('test.target', 'test')
        self.ndb_transaction.return_value = self.ongoing_context
        _notify_async_completed()
        self.assertTrue(self.get_current.called)
        self.assertTrue(self.ndb_transaction.called)

    def test_notify_async_complete_without_async(self):
        """Ensure that the notify complete function throws with null async."""
        self.get_current.return_value = None
        self.assertRaises(
            InvalidCallbackAsyncError,
            _notify_async_completed
        )

    def test_on_complete_function_called(self):
        """Ensure the oncomplete function gets called"""
        self.get_current.return_value = CallbackAsync('test.target', 'test')

        context = CallbackContext()
        context.on_complete(ensure_complete)
        ongoing = OngoingCallbackContext(options=context.to_dict())
        self.ndb_transaction.return_value = ongoing
        _notify_async_completed()
        global CALLED
        self.assertTrue(CALLED)

    def test_on_complete_function_not_called(self):
        """Ensures on complete not called when tasks aren't done"""
        self.get_current.return_value = CallbackAsync('test.target', 'test')

        context = CallbackContext()
        context.add('to.target')
        ongoing = OngoingCallbackContext(options=context.to_dict())
        self.ndb_transaction.return_value = ongoing
        _notify_async_completed()
        global CALLED
        self.assertFalse(CALLED)
