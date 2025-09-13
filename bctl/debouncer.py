import asyncio
from asyncio import Task

# note only the last execution gets invoked
class Debouncer:
    def __init__(self, delay: float):
        self.delay: float = delay
        self.task: Task|None = None

    async def __call__(self, func, *args, **kwargs):
        if self.task is not None and not self.task.done():
            self.task.cancel()
        self.task = asyncio.create_task(self._debounced_call(func, *args, **kwargs))

    async def _debounced_call(self, func, *args, **kwargs):
        try:
            await asyncio.sleep(self.delay)
            await func(*args, **kwargs)
        except asyncio.CancelledError:
            pass  # ignore cancelled calls
