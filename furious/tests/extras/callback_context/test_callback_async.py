import unittest

from ....extras.callback_context import CallbackAsync, InvalidContextError

class TestCallbackAsync(unittest.TestCase):
    def test_create_callback_async(self):
        """Ensures basic creation of callback async."""
        async = CallbackAsync('test.path', 'contextid')
        self.assertEqual('contextid', async.context_id)
        self.assertIsNotNone(async.id)

    def test_create_callback_async_without_context(self):
        """Ensures an error is thrown when you attempt to create a callback
        Async without a context id.
        """
        self.assertRaises(
            InvalidContextError,
            CallbackAsync,
            'test.path',
            None
        )

    def test_add_callback_to_callback_async(self):
        """Ensures we can properly add a callback to a callback async."""
        test_func = lambda: None

        async = CallbackAsync('test.path', 'contextid')
        async.add_callback('success', test_func)
        callbacks = async.get_callbacks()
        self.assertIsNotNone(callbacks['success'])