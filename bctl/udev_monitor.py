import asyncio
import logging
from logging import Logger
from pyudev import Context, Device, Monitor
from typing import AsyncGenerator, Awaitable, Callable

LOGGER: Logger = logging.getLogger(__name__)

# from https://github.com/pyudev/pyudev/issues/450
async def iter_monitor_devices(context: Context, **kwargs) -> AsyncGenerator[Device, None]:
    # for device in context.list_devices(**kwargs):
        # yield device

    monitor: Monitor = Monitor.from_netlink(context)
    monitor.filter_by(**kwargs)
    monitor.start()
    fd: int = monitor.fileno()
    read_event = asyncio.Event()
    loop = asyncio.get_event_loop()
    loop.add_reader(fd, read_event.set)
    try:
        while True:
            await read_event.wait()
            while True:
                device: Device | None = monitor.poll(0)
                if device:
                    yield device
                else:
                    read_event.clear()
                    break
    finally:
        loop.remove_reader(fd)


async def monitor_udev_events(subsys: str, action: str, f: Callable[[], Awaitable]):
    context = Context()
    async for device in iter_monitor_devices(context, subsystem=subsys):
        if device.action == action:
            LOGGER.debug(f'   ...{subsys} {action} udev event received for {device}')
            await f()
