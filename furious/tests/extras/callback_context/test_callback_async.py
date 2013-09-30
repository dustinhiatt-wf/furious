import unittest

import uuid

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

    def test_create_callback_with_id(self):
        """Ensures we can set an async's id from the constructor."""
        id = uuid.uuid1().hex
        async = CallbackAsync('test.path', 'contextid', _id=id)
        self.assertEqual(id, async.id)

    def test_async_to_dict_includes_id(self):
        """Ensure the to_dict method on the async will export the id."""
        async = CallbackAsync('test.path', 'contextid')
        dct = async.to_dict()
        self.assertEqual(async.id, dct.get('_id'))

    def test_async_to_dict_includes_context_id(self):
        """Ensures the to_dict method will export context id."""
        async = CallbackAsync('test.path', 'contextid')
        dct = async.to_dict()
        self.assertEqual(async.context_id, dct['_context_id'])

    def test_async_from_dict_includes_id(self):
        """Ensures imported async retains exported async's id."""
        async = CallbackAsync('test.path', 'contextid')
        dct = async.to_dict()
        copy_async = CallbackAsync.from_dict(dct)
        self.assertEqual(async.id, copy_async.id)

    def test_async_from_dict_includes_context_id(self):
        """Ensures imported Async retains exported Async's context id."""
        async = CallbackAsync('test.path', 'contextid')
        dct = async.to_dict()
        copy_async = CallbackAsync.from_dict(dct)
        self.assertEqual(async.context_id, copy_async.context_id)