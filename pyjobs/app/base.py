"""PyJobs Application.

An app is an instance of the PyJobs library.
Everything starts here.

"""
import asyncio
import importlib
import inspect
import re
import sys
import typing
import warnings

from datetime import tzinfo
from functools import wraps
from itertools import chain
from typing import (
    Any,
    AsyncIterable,
    Awaitable,
    Callable,
    ClassVar,
    ContextManager,
    Dict,
    Iterable,
    Iterator,
    List,
    Mapping,
    MutableMapping,
    MutableSequence,
    Optional,
    Pattern,
    Set,
    Tuple,
    Type,
    Union,
    cast,
)

import opentracing

class App(AppT, Service):
    """pyjobs Application.

    Arguments:
        id (str): Application ID.

    Keyword Arguments:
        loop (asyncio.AbstractEventLoop): optional event loop to use.

    See Also:
        :ref:`application-configuration` -- for supported keyword arguments.

    """

    #: Optional tracing support.
    tracer: Optional[Any] = None
    _rebalancing_span: Optional[opentracing.Span] = None
    _rebalancing_sensor_state: Optional[Dict] = None

    def __init__(self,
                 id: str,
                 *,
                 monitor: Monitor = None,
                 config_source: Any = None,
                 loop: asyncio.AbstractEventLoop = None,
                 beacon: NodeT = None,
                 **options: Any) -> None:
        # This is passed to the configuration in self.conf
        self._default_options = (id, options)

        # The agent manager manages all agents.
        self.agents = AgentManager(self)

    def _init_signals(self) -> None:
        # Signals in pyjobs are the same as in Django, but asynchronous by
        # default (:class:`mode.SyncSignal` is the normal ``def`` version)).
        #
        # Signals in pyjobs are usually local to the app instance::
        #
        #  @app.on_before_configured.connect  # <-- only sent by this app
        #  def on_before_configured(self):
        #    ...
        #
        # In Django signals are usually global, and an easter-egg
        # provides this in pyjobs also::
        #
        #    V---- NOTE upper case A in App
        #   @App.on_before_configured.connect  # <-- sent by ALL apps
        #   def on_before_configured(app):
        #
        # Note: Signals are local-only, and cannot do anything to other
        # processes or machines.
        self.on_before_configured = (
            self.on_before_configured.with_default_sender(self))

    def main(self) -> NoReturn:
        """Execute the :program:`pyjobs` umbrella command using this app."""
        from pyjobs.cli.pyjobs import cli
        self.finalize()
        self.worker_init()
        if self.conf.autodiscover:
            self.discover()
        self.worker_init_post_autodiscover()
        cli(app=self)
        raise SystemExit(3451)  # for mypy: NoReturn

    def topic(self,
              *topics: str,
              pattern: Union[str, Pattern] = None,
              schema: SchemaT = None,
              key_type: ModelArg = None,
              value_type: ModelArg = None,
              key_serializer: CodecArg = None,
              value_serializer: CodecArg = None,
              partitions: int = None,
              retention: Seconds = None,
              compacting: bool = None,
              deleting: bool = None,
              replicas: int = None,
              acks: bool = True,
              internal: bool = False,
              config: Mapping[str, Any] = None,
              maxsize: int = None,
              allow_empty: bool = False,
              loop: asyncio.AbstractEventLoop = None) -> TopicT:
        """Create topic description.

        Topics are named channels (for example a Kafka topic),
        that exist on a server.  To make an ephemeral local communication
        channel use: :meth:`channel`.

        See Also:
            :class:`pyjobs.topics.Topic`
        """
        return self.conf.Topic(
            self,
            topics=topics,
            pattern=pattern,
            schema=schema,
            key_type=key_type,
            value_type=value_type,
            key_serializer=key_serializer,
            value_serializer=value_serializer,
            partitions=partitions,
            retention=retention,
            compacting=compacting,
            deleting=deleting,
            replicas=replicas,
            acks=acks,
            internal=internal,
            config=config,
            allow_empty=allow_empty,
            loop=loop,
        )
