import uuid

from google.appengine.ext import ndb

from ...async import Async, decode_async_options
from ...context import Context, get_current_async
from ...job_utils import path_to_reference, reference_to_path


class OngoingCallbackContext(ndb.Model):
    options = ndb.JsonProperty(indexed=False)


def __check_if_complete(callback_dct):
    if len(callback_dct['_completed_task_ids']) >= len(callback_dct['_task_ids']):
        return True

    return False


def __complete_context(callback_dct):
    on_complete = callback_dct.get('_on_complete')
    if on_complete:
        on_complete = path_to_reference(on_complete)
        on_complete(
            *callback_dct.get('_on_complete_args', ()),
            **callback_dct.get('_on_complete_kwargs', {})
        )


def _notify_async_completed():
    async = get_current_async()
    if not async:
        raise InvalidCallbackAsyncError('No async currently running')

    if not isinstance(async, CallbackAsync):
        return

    def txn(async):
        context = OngoingCallbackContext.get_by_id(async.context_id)
        context.options['_completed_task_ids'].append(async.id)
        context.put()

        return context

    context = ndb.transaction(lambda: txn(async))

    if __check_if_complete(context.options):
        __complete_context(context.options)


class CallbackContextPersistenceEngine(object):
    @classmethod
    def store_context(cls, context_id, context_dct):
        OngoingCallbackContext.get_or_insert(
            context_id,
            option=context_dct
        )


class CallbackContext(Context):
    def __init__(self, **options):
        options['persistence_engine'] = options.get(
            'persistence_engine',
            CallbackContextPersistenceEngine
        )
        Context.__init__(self, **options)

        self.__on_complete = None
        self.__on_complete_args = None
        self.__on_complete_kwargs = None

    def add(self, target, args=None, kwargs=None, **options):
        target = CallbackAsync(target, self.id, args, kwargs, **options)
        target.add_callback('success', _notify_async_completed)

        self._tasks.append(target)

    def to_dict(self):
        dct = Context.to_dict(self)
        dct['_completed_task_ids'] = []
        if self.__on_complete:
            dct['_on_complete'] = reference_to_path(self.__on_complete)
            dct['_on_complete_args'] = self.__on_complete_args or ()
            dct['_on_complete_kwargs'] = self.__on_complete_kwargs or {}
        return dct

    def on_complete(self, function, *args, **kwargs):
        self.__on_complete = function
        self.__on_complete_args = args
        self.__on_complete_kwargs = kwargs

    def start(self):
        self.persist()
        Context.start(self)


class CallbackAsync(Async):
    def __init__(self, target, context_id, args=None, kwargs=None, **options):
        if not context_id:
            raise InvalidContextError('Must create callback async with a '
                                      'context id.')

        options['_context_id'] = context_id

        Async.__init__(self, target, args, kwargs, **options)
        self.__context_id = context_id
        self.__id = options.get('_id') or uuid.uuid4().hex

    @property
    def context_id(self):
        return self.__context_id

    @property
    def id(self):
        return self.__id

    @classmethod
    def from_dict(cls, async):
        async_options = decode_async_options(async)

        target, args, kwargs = async_options.pop('job')

        context_id = async_options.pop('_context_id')

        return cls(target, context_id, args, kwargs, **async_options)

    def add_callback(self, type, function):
        callback_dct = self._options.get('callbacks', {})
        callback_dct[type] = reference_to_path(function)
        self._options['callbacks'] = callback_dct

    def to_dict(self):
        self._options['_id'] = self.id
        return Async.to_dict(self)


class InvalidContextError(Exception):
    pass


class InvalidCallbackAsyncError(Exception):
    pass