"""Event/Observer pattern utility similar to C# Events."""

from __future__ import annotations
import asyncio
from typing import Callable, Generic, TypeVar, List, Optional, Type


T = TypeVar("T")


class Event(Generic[T]):
    """A simple event class that implements the observer pattern.
    
    Similar to C# Events, this allows multiple handlers to subscribe
    to an event and be notified when the event is raised.
    
    Usage:
        # Create an event
        on_message_received: Event[str] = Event()
        
        # Subscribe to the event
        def handler(msg: str):
            print(f"Received: {msg}")
        
        on_message_received += handler
        # or: on_message_received.subscribe(handler)
        
        # Raise the event
        on_message_received.emit("Hello!")
        # or: on_message_received("Hello!")
        
        # Unsubscribe
        on_message_received -= handler
        # or: on_message_received.unsubscribe(handler)
    
    Type Parameters:
        T: The type of the argument passed to handlers (use None for no args)
    """

    def __init__(self, arg_type: Type[T]) -> None:
        self._handlers: List[Callable[[T], None]] = []
        self._arg_type = arg_type

    def subscribe(self, handler: Callable[[T], None]) -> None:
        """Subscribe a handler to this event.
        
        Args:
            handler: A callable that will be invoked when the event is emitted.
        """
        if handler not in self._handlers:
            self._handlers.append(handler)

    def unsubscribe(self, handler: Callable[[T], None]) -> None:
        """Unsubscribe a handler from this event.
        
        Args:
            handler: The handler to remove.
        """
        if handler in self._handlers:
            self._handlers.remove(handler)

    def emit(self, arg: T) -> None:
        """Emit the event, notifying all subscribed handlers.
        
        Args:
            arg: The argument to pass to all handlers.
        """
        for handler in self._handlers[:]:  # Copy list to allow modification during iteration
            handler(arg)

    def clear(self) -> None:
        """Remove all subscribed handlers."""
        self._handlers.clear()

    def __iadd__(self, handler: Callable[[T], None]) -> Event[T]:
        """Subscribe using += operator (C# style)."""
        self.subscribe(handler)
        return self

    def __isub__(self, handler: Callable[[T], None]) -> Event[T]:
        """Unsubscribe using -= operator (C# style)."""
        self.unsubscribe(handler)
        return self

    def __call__(self, arg: T) -> None:
        """Allow calling the event directly to emit."""
        self.emit(arg)

    def __len__(self) -> int:
        """Return the number of subscribed handlers."""
        return len(self._handlers)

    def __bool__(self) -> bool:
        """Return True if there are any subscribed handlers."""
        return len(self._handlers) > 0


class VoidEvent:
    """An event that takes no arguments.
    
    Usage:
        on_connected: VoidEvent = VoidEvent()
        
        def handler():
            print("Connected!")
        
        on_connected += handler
        on_connected.emit()
    """

    def __init__(self) -> None:
        self._handlers: List[Callable[[], None]] = []

    def subscribe(self, handler: Callable[[], None]) -> None:
        """Subscribe a handler to this event."""
        if handler not in self._handlers:
            self._handlers.append(handler)

    def unsubscribe(self, handler: Callable[[], None]) -> None:
        """Unsubscribe a handler from this event."""
        if handler in self._handlers:
            self._handlers.remove(handler)

    def emit(self) -> None:
        """Emit the event, notifying all subscribed handlers."""
        for handler in self._handlers[:]:
            handler()

    def clear(self) -> None:
        """Remove all subscribed handlers."""
        self._handlers.clear()

    def __iadd__(self, handler: Callable[[], None]) -> VoidEvent:
        """Subscribe using += operator."""
        self.subscribe(handler)
        return self

    def __isub__(self, handler: Callable[[], None]) -> VoidEvent:
        """Unsubscribe using -= operator."""
        self.unsubscribe(handler)
        return self

    def __call__(self) -> None:
        """Allow calling the event directly to emit."""
        self.emit()

    def __len__(self) -> int:
        """Return the number of subscribed handlers."""
        return len(self._handlers)

    def __bool__(self) -> bool:
        """Return True if there are any subscribed handlers."""
        return len(self._handlers) > 0


# class AsyncEvent(Generic[T]):
#     """An event that supports async handlers.
    
#     Usage:
#         on_data: AsyncEvent[bytes] = AsyncEvent()
        
#         async def async_handler(data: bytes):
#             await process_data(data)
        
#         on_data += async_handler
#         await on_data.emit(b"data")
#     """

#     def __init__(self) -> None:
#         self._handlers: List[Callable[[T], None]] = []

#     def subscribe(self, handler: Callable[[T], None]) -> None:
#         """Subscribe a handler (sync or async) to this event."""
#         if handler not in self._handlers:
#             self._handlers.append(handler)

#     def unsubscribe(self, handler: Callable[[T], None]) -> None:
#         """Unsubscribe a handler from this event."""
#         if handler in self._handlers:
#             self._handlers.remove(handler)

#     async def emit(self, arg: T) -> None:
#         """Emit the event, awaiting all async handlers."""
#         for handler in self._handlers[:]:
#             result = handler(arg)
#             if asyncio.iscoroutine(result):
#                 await result

#     def clear(self) -> None:
#         """Remove all subscribed handlers."""
#         self._handlers.clear()

#     def __iadd__(self, handler: Callable[[T], None]) -> AsyncEvent[T]:
#         """Subscribe using += operator."""
#         self.subscribe(handler)
#         return self

#     def __isub__(self, handler: Callable[[T], None]) -> AsyncEvent[T]:
#         """Unsubscribe using -= operator."""
#         self.unsubscribe(handler)
#         return self

#     def __len__(self) -> int:
#         """Return the number of subscribed handlers."""
#         return len(self._handlers)

#     def __bool__(self) -> bool:
#         """Return True if there are any subscribed handlers."""
#         return len(self._handlers) > 0
