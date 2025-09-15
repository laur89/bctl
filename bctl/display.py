import os
import logging
import asyncio
import aiofiles as aiof
import aiofiles.os as aios
from os import R_OK, W_OK
from abc import abstractmethod, ABC
from enum import Enum
from typing import TypeVar
from asyncio import Task
from pathlib import Path
from logging import Logger
from .common import run_cmd, wait_and_reraise
from .config import Conf, SimConf
from .exceptions import ExitableErr, FatalErr


class DisplayType(Enum):
    INTERNAL = 1
    EXTERNAL = 2
    SIM = 3


class BackendType(Enum):
    DDCUTIL = 1  # note this can only be backend for external displays
    RAW = 2
    BRILLO = 3
    BRIGHTNESSCTL = 4
    SIM = 5


class Display(ABC):
    def __init__(self, id: str, dt: DisplayType, bt: BackendType, conf: Conf) -> None:
        self.id: str = id
        self.type: DisplayType = dt
        self.backend: BackendType = bt
        self.conf: Conf = conf
        self.name: str = 'UNKNOWN'
        self.brightness: int = -1  # note this holds the raw value, not necessarily %
        self.max_brightness: int = -1
        self.min_brightness: int = 0
        self.logger: Logger = logging.getLogger(f'{type(self).__name__}.{self.id}')

    def _init(self) -> None:
        self.logger.debug(f'  -> initializing {self.type} display [{self.id}]...')

    @abstractmethod
    async def _set_brightness(self, value: int) -> None:
        pass

    async def set_brightness(self, value: int) -> int:
        value = round(value / 100 * self.max_brightness)
        if value < self.min_brightness:
            value = self.min_brightness
        elif value > self.max_brightness:
            value = self.max_brightness

        if value != self.brightness:
            self.logger.debug(f'setting display [{self.id}] brightness to {value} ({round(value/self.max_brightness*100)}%)...')
            await self._set_brightness(value)
            self.brightness = value
        return self.get_brightness()

    async def adjust_brightness(self, delta: int) -> int:
        delta = round(delta / 100 * self.max_brightness)
        target = self.brightness + delta

        if ((self.brightness == self.max_brightness and delta > 0) or
                (self.brightness == self.min_brightness and delta < 0)):
            return self.get_brightness()
        elif target < self.min_brightness:
            target = self.min_brightness
        elif target > self.max_brightness:
            target = self.max_brightness

        self.logger.debug(f'adjusting display [{self.id}] brightness to {target} ({round(target/self.max_brightness*100)}%)...')
        await self._set_brightness(target)
        self.brightness = target
        return self.get_brightness()

    def get_brightness(self, raw:bool=False) -> int:
        if self.brightness == -1:
            raise FatalErr(f'[{self.id}] appears to be uninitialized')
        self.logger.debug(f'getting display [{self.id}] brightness')
        return self.brightness if raw else round(self.brightness/self.max_brightness*100)


# display baseclass that's not backed by ddcutil
class NonDDCDisplay(Display, ABC):
    def __init__(self, id: str, conf: Conf, bt: BackendType) -> None:
        if id.startswith('ddcci'):  # e.g. ddcci11
            dt = DisplayType.EXTERNAL
        else:
            dt = DisplayType.INTERNAL
        super().__init__(id, dt, bt, conf)


class SimulatedDisplay(Display):
    sim: SimConf

    def __init__(self, id: str, conf: Conf) -> None:
        super().__init__(id, DisplayType.SIM, BackendType.SIM, conf)
        self.sim = self.conf.get('sim')

    async def init(self, initial_brightness: int) -> None:
        super()._init()
        await asyncio.sleep(3)
        if self.sim.get('failmode') == 'i':
            raise ExitableErr(f'error initializing [{self.id}]', exit_code=self.sim.get('exit_code'))
        self.brightness = initial_brightness
        self.max_brightness = 100

    async def _set_brightness(self, value: int) -> None:
        await asyncio.sleep(self.sim.get('wait_sec'))
        if self.sim.get('failmode') == 's':
            raise ExitableErr(f'error setting [{self.id}] brightness to {value}', exit_code=self.sim.get('exit_code'))


# for ddcutil performance, see https://github.com/rockowitz/ddcutil/discussions/393
class DDCDisplay(Display):
    bus: str  # string representation of this display's i2c bus number

    def __init__(self, id: str, conf: Conf) -> None:
        super().__init__(id, DisplayType.EXTERNAL, BackendType.DDCUTIL, conf)

    async def init(self) -> None:
        super()._init()
        assert self.bus.isdigit(), f'[{self.id}] bus is invalid: [{self.bus}]'
        cmd = ['ddcutil', '--brief', 'getvcp',
               self.conf.get('ddcutil_brightness_feature'), '--bus', self.bus]
        out, err, code = await run_cmd(cmd, throw_on_err=True, logger=self.logger)
        out = out.split()
        assert len(out) == 5, f'{cmd} output unexpected: {out}'
        self.brightness = int(out[-2])
        self.max_brightness = int(out[-1])

    async def _set_brightness(self, value: int) -> None:
        await self._set_vcp_feature([self.conf.get('ddcutil_brightness_feature'), str(value)])

    async def _set_vcp_feature(self, args: list[str]) -> None:
        await run_cmd(['ddcutil'] + self.conf.get('ddcutil_svcp_flags') + ['setvcp'] +
                      args + ['--bus', self.bus], throw_on_err=True, logger=self.logger)

    async def _get_vcp_feature(self, args: list[str]) -> str:
        out, err, code = await run_cmd(['ddcutil'] + self.conf.get('ddcutil_gvcp_flags') + ['getvcp'] +
                                        args + ['--bus', self.bus], throw_on_err=True, logger=self.logger)
        return out


class BCTLDisplay(NonDDCDisplay):
    def __init__(self, bctl_out: str, conf: Conf) -> None:
        out = bctl_out.split(',')
        assert len(out) == 5, f'unexpected bctl list output: [{bctl_out}]'
        self.brightness = int(out[2])
        self.max_brightness = int(out[4])
        super().__init__(out[0], conf, BackendType.BRIGHTNESSCTL)

    async def init(self) -> None:
        super()._init()

    async def _set_brightness(self, value: int) -> None:
        await run_cmd(['brightnessctl', '--quiet',
                      '-d', self.id, 'set', str(value)],
                      throw_on_err=True, logger=self.logger)


class BrilloDisplay(NonDDCDisplay):
    def __init__(self, id: str, conf: Conf) -> None:
        super().__init__(id, conf, BackendType.BRILLO)

    async def init(self) -> None:
        super()._init()

        futures: list[Task[int]] = [
            asyncio.create_task(self._get_device_attr('b')),  # current brightness
            asyncio.create_task(self._get_device_attr('m')),  # max
            asyncio.create_task(self._get_device_attr('c'))   # min
        ]
        await wait_and_reraise(futures)
        self.brightness = futures[0].result()
        self.max_brightness = futures[1].result()
        self.min_brightness = futures[2].result()

    async def _get_device_attr(self, attr: str) -> int:
        out, err, code = await run_cmd(['brillo', '-s', self.id, f'-rlG{attr}'],
                                       throw_on_err=True, logger=self.logger)
        return int(out)

    async def _set_brightness(self, value: int) -> None:
        await run_cmd(['brillo', '-s', self.id, '-rlS', str(value)],
                      throw_on_err=True, logger=self.logger)


class RawDisplay(NonDDCDisplay):
    device_dir: str
    brightness_f: Path

    def __init__(self, device_dir: str, conf: Conf) -> None:
        super().__init__(os.path.basename(device_dir), conf, BackendType.RAW)
        self.device_dir = device_dir  # caller needs to verify it exists!

    async def init(self) -> None:
        super()._init()

        self.brightness_f = Path(self.device_dir, 'brightness')

        if not (await aios.path.isfile(self.brightness_f)
                and await aios.access(self.brightness_f, R_OK)
                and await aios.access(self.brightness_f, W_OK)):
            raise FatalErr(f'[{self.brightness_f}] is not a file w/ RW perms')

        # self.brightness = int(self.brightness_f.read_text().strip())  # non-async
        self.brightness = await self._read_int(self.brightness_f)

        max_brightness_f = Path(self.device_dir, 'max_brightness')
        if (await aios.path.isfile(max_brightness_f)
                and await aios.access(max_brightness_f, R_OK)):
            # self.max_brightness = int(max_brightness_f.read_text().strip())  # non-async
            self.max_brightness = await self._read_int(max_brightness_f)

    async def _read_int(self, file: Path) -> int:
        async with aiof.open(file, mode='r') as f:
            return int((await f.read()).strip())

    async def _set_brightness(self, value: int) -> None:
        async with aiof.open(self.brightness_f, mode='w') as f:
            await f.write(str(value))

TNonDDCDisplay = TypeVar('TNonDDCDisplay', bound=NonDDCDisplay)
TDisplay = TypeVar('TDisplay', bound=Display)
