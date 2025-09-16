import asyncio
import traceback
import signal
import os
import sys
import re
import json
import logging
import functools
import glob
from logging import Logger
from types import TracebackType
from asyncio import AbstractEventLoop, Task, Queue
from typing import NoReturn, Callable, Coroutine
from tendo import singleton
from pathlib import Path
from statistics import fmean
import aiofiles.os as aios
from .debouncer import Debouncer
from .udev_monitor import monitor_udev_events
from .display import (BackendType, DisplayType, Display,
                      SimulatedDisplay, DDCDisplay, BCTLDisplay,
                      BrilloDisplay, RawDisplay, TNonDDCDisplay, TDisplay)
from .common import (load_config, write_state, run_cmd, same_values, assert_cmd_exist,
                     wait_and_reraise, unix_time_now)
from .config import Conf, SimConf
from .exceptions import ExitableErr, FatalErr
from .notify import Notif


DISPLAYS: list[Display]
TASK_QUEUE: Queue[list]
CONF: Conf
LOGGER: Logger = logging.getLogger(__name__)
NOTIF: Notif
LAST_INIT_TIME: int = 0


def validate_ext_deps() -> None:
    requirements = [CONF.get('main_display_ctl'), CONF.get('internal_display_ctl')]
    for dep in ['DDCUTIL', 'BRILLO', 'BRIGHTNESSCTL']:
        if dep in requirements:
            assert_cmd_exist(dep.lower())


async def init_displays() -> None:
    global DISPLAYS
    global LAST_INIT_TIME
    DISPLAYS = []  # immediately reset old state

    if CONF.get('sim'): return await init_displays_sim(CONF.get('sim'))

    LOGGER.debug('initing displays...')
    ignore_internal = CONF.get('ignore_internal_display')

    displays: list[Display]
    match CONF.get('main_display_ctl'):
        case 'DDCUTIL':
            displays = await get_ddcutil_displays(ignore_internal)
        case 'RAW':
            displays = await get_raw_displays()
        case 'BRIGHTNESSCTL':
            displays = await get_bctl_displays()
        case 'BRILLO':
            displays = await get_brillo_displays()
        case _:
            raise FatalErr(f'misconfigured display brightness management method [{CONF.get("main_display_ctl")}]')

    if ignore_internal:
        displays = list(filter(lambda d: d.type != DisplayType.INTERNAL, displays))
    if CONF.get('ignore_external_display'):
        displays = list(filter(lambda d: d.type != DisplayType.EXTERNAL, displays))

    ignored_displays = CONF.get('ignored_displays')
    if ignored_displays:
        displays = list(filter(lambda d: d.id not in ignored_displays and d.name not in ignored_displays, displays))

    if len(list(filter(lambda d: d.type == DisplayType.INTERNAL, displays))) > 1:
        # TODO: shouldn't this exit fatally?
        raise RuntimeError('more than 1 laptop/internal displays found')

    if displays:
        futures: list[Task[None]] = [asyncio.create_task(d.init()) for d in displays]
        await wait_and_reraise(futures)

    DISPLAYS = displays
    LOGGER.debug(f'...initialized {len(displays)} display{"" if len(displays) == 1 else "s"}')
    if CONF.get('sync_brightness'):
        await sync_displays()
    LAST_INIT_TIME = unix_time_now()


async def sync_displays() -> None:
    if len(DISPLAYS) <= 1: return
    values: list[int] = [d.get_brightness() for d in DISPLAYS]
    if same_values(values): return

    target: int = CONF.get('state').get('last_set_brightness')
    if target == -1:  # i.e. we haven't explicitly set it to anything yet
        match CONF.get('sync_strategy'):
            case 'MEAN':
                target = int(fmean(values))
            case 'LOW':
                target = min(values)
            case 'HIGH':
                target = max(values)
            case _:
                raise FatalErr(f'misconfigured brightness sync strategy [{CONF.get("sync_strategy")}]')

    LOGGER.debug(f'syncing brightnesses at {target}%')
    await TASK_QUEUE.put(['set', target])


async def init_displays_sim(sim) -> None:
    global DISPLAYS

    ndisplays: int = sim.get('ndisplays')

    LOGGER.debug(f'initing {ndisplays} simulated displays...')
    displays: list[SimulatedDisplay] = [SimulatedDisplay(f'sim-{i}', CONF) for i in range(ndisplays)]

    futures: list[Task[None]] = [asyncio.create_task(d.init(sim.get('initial_brightness'))) for d in displays]
    await wait_and_reraise(futures)

    DISPLAYS = displays
    LOGGER.debug(f'...initialized {len(displays)} simulated display{"" if len(displays) == 1 else "s"}')


async def resolve_single_internal_display_raw() -> RawDisplay:
    d = await get_raw_displays()
    return _filter_internal_display(d, BackendType.RAW)

    #  alternative logic by verifying internal display via device path, not device name:
    # device_dirs = glob.glob(CONF.get('raw_device_dir') + '/*')
    # displays = []
    # for i in device_dirs:
        # name = os.path.basename(i)
        # i = Path(i).resolve()
        # if (i.exists() and  # potential dead symlink
                # next((True for segment in i.parts if 'eDP-' in segment), False)):  # verify is internal/laptop display, alternative detection
            # displays.append(RawDisplay(name, CONF))  # or RawDisplay(i.name, CONF)

    # assert len(displays) == 1, f'found {len(displays)} raw backlight devices, expected 1'
    # return displays[0]


def _filter_by_backend_type(displays: list[TDisplay], bt: BackendType) -> list[TDisplay]:
    return list(filter(lambda d: d.backend == bt, displays))


def _filter_by_display_type(displays: list[TDisplay], dt: DisplayType) -> list[TDisplay]:
    return list(filter(lambda d: d.type == dt, displays))


def _filter_internal_display(disp: list[TNonDDCDisplay], provider: BackendType) -> TNonDDCDisplay:
    displays: list[TNonDDCDisplay] = _filter_by_display_type(disp, DisplayType.INTERNAL)
    assert len(displays) == 1, f'found {len(displays)} laptop/internal displays w/ {provider}, expected 1'
    return displays[0]


async def resolve_single_internal_display_brillo() -> BrilloDisplay:
    d = await get_brillo_displays()
    return _filter_internal_display(d, BackendType.BRILLO)


async def resolve_single_internal_display_bctl() -> BCTLDisplay:
    d = await get_bctl_displays()
    return _filter_internal_display(d, BackendType.BRIGHTNESSCTL)


async def get_raw_displays() -> list[RawDisplay]:
    device_dirs: list[str] = glob.glob(CONF.get('raw_device_dir') + '/*')
    assert len(device_dirs) > 0, f'no backlight-capable raw devices found'

    return [RawDisplay(d, CONF)
            for d in device_dirs if await aios.path.exists(d)]  # exists() check to deal with dead symlinks


async def get_brillo_displays() -> list[BrilloDisplay]:
    out, err, code = await run_cmd(['brillo', '-Ll'], throw_on_err=True, logger=LOGGER)
    out = out.splitlines()
    assert len(out) > 0, f'no backlight-capable devices found w/ brillo'

    return [BrilloDisplay(os.path.basename(i), CONF) for i in out
            if await aios.path.exists(Path(CONF.get('raw_device_dir'), i))]  # exists() check to deal with dead symlinks


async def get_bctl_displays() -> list[BCTLDisplay]:
    cmd = ['brightnessctl', '--list', '--machine-readable', '--class=backlight']
    out, err, code = await run_cmd(cmd, throw_on_err=True, logger=LOGGER)
    out = out.splitlines()
    assert len(out) > 0, f'no backlight-capable devices found w/ brightnessctl'

    return [BCTLDisplay(i, CONF) for i in out
            if await aios.path.exists(Path(CONF.get('raw_device_dir'), i.split(',')[0]))]  # exists() check to deal with dead symlinks


async def get_ddcutil_displays(ignore_internal) -> list[Display]:
    bus_path_prefix = CONF.get('ddcutil_bus_path_prefix')
    displays: list[Display] = []
    in_invalid_block = False
    d: DDCDisplay | None = None
    out, err, code = await run_cmd(['ddcutil', '--brief', 'detect'], throw_on_err=False, logger=LOGGER)
    if code != 0:
        if err and 'ddcutil requires module i2c' in err:
            raise FatalErr('ddcutil requires i2c-dev kernel module to be loaded')
        LOGGER.error(err)
        raise RuntimeError(f'ddcutil failed to list/detect devices (exit code {code})')

    for line in out.splitlines():
        if d:
            if line.startswith('   I2C bus:'):
                i = line.find(bus_path_prefix)
                d.bus = line[len(bus_path_prefix)+i:]
            elif line.startswith('   Monitor:'):
                d.name = line.split()[1]
                displays.append(d)
                d = None  # reset
            elif not line:  # block end
                raise FatalErr(f'could not finalize display [{d.id}] - [ddcutil --brief] output has likely changed')
        elif in_invalid_block:  # try to detect laptop internal display
            if not line:
                in_invalid_block = False
            # note matching against "eDP" in "DRM connector" line is not infallible, see https://github.com/rockowitz/ddcutil/issues/547#issuecomment-3253325547
            # expected line will be something like "   DRM connector:    card0-eDP-1"
            elif re.fullmatch(r'\s+DRM connector:\s+[a-z0-9]+-eDP-\d+', line):  # i.e. "is this a laptop display?"
                match CONF.get('internal_display_ctl'):
                    case 'RAW':
                        displays.append(await resolve_single_internal_display_raw())
                    case 'BRIGHTNESSCTL':
                        displays.append(await resolve_single_internal_display_bctl())
                    case 'BRILLO':
                        displays.append(await resolve_single_internal_display_brillo())
                    case _:
                        raise FatalErr(f'misconfigured internal display brightness management method [{CONF.get("internal_display_ctl")}]')
                in_invalid_block = False
        elif line.startswith('Display '):
            d = DDCDisplay(line.strip(), CONF)
        elif line == 'Invalid display' and not ignore_internal:
            in_invalid_block = True  # start processing one of the 'Invalid display' blocks
    if d:  # sanity
        raise FatalErr(f'display [{d.id}] defined but not finalized')
    return displays


async def execute_tasks(tasks: list[list]) -> None:
    delta = 0
    target: int | None = None
    init = False
    sync = False
    for t in tasks:
        match t:
            case ['delta', d]:  # change brightness by delta %
                delta += d
            case ['set', value]:  # set brightness to a % value
                delta = 0  # cancel all previous deltas
                target = value
            case ['init']:  # re-init displays
                init = True
            case ['sync']:
                sync = True
            case ['kill']:
                try:
                    await write_state(CONF)
                finally:
                    sys.exit(0)
            case _:
                LOGGER.error(f'unexpected task {t}')

    if init:
        await init_displays()
    if sync:
        await sync_displays()

    futures: list[Task[int]]
    if target is not None:
        target += delta
        futures = [asyncio.create_task(d.set_brightness(target)) for d in DISPLAYS]
    elif delta != 0:
        futures = [asyncio.create_task(d.adjust_brightness(delta)) for d in DISPLAYS]
    else:
        return

    if futures:
        number_tasks = f'{len(tasks)} task{"" if len(tasks) == 1 else "s"}'
        LOGGER.debug(f'about to execute() {number_tasks}...')
        await wait_and_reraise(futures)
        brightnesses: list[int] = sorted([f.result() for f in futures])
        LOGGER.debug(f'...executed {number_tasks}')

        if abs(brightnesses[0] - brightnesses[-1]) <= 2:  # brightness values' extremes differ max by 2%
            CONF.get('state')['last_set_brightness'] = brightnesses[0]

        await NOTIF.notify_change(brightnesses[0])
        if CONF.get('sync_brightness'):
            await sync_displays()


async def process_q() -> NoReturn:
    consumption_window: float = CONF.get('msg_consumption_window_sec')
    while True:
        tasks: list[list] = []
        t: list = await TASK_QUEUE.get()
        tasks.append(t)
        while True:
            try:
                t = await asyncio.wait_for(TASK_QUEUE.get(), consumption_window)
                tasks.append(t)
            except TimeoutError:
                break
        await execute_tasks(tasks)


async def process_client_commands() -> None:
    async def process_client_command(reader, writer):
        async def _send_response(payload: list):
            writer.write(json.dumps(payload).encode())
            await writer.drain()
            writer.write_eof()
            writer.close()
            await writer.wait_closed()

        async def vcp_op(vcp_for_display: Callable[[DDCDisplay], Coroutine],
                         payload_creator: Callable[[list[Task[str]], list[DDCDisplay]], list[int|str]]):
            displays: list[DDCDisplay] = _filter_by_backend_type(DISPLAYS, BackendType.DDCUTIL)
            if displays:
                try:
                    futures: list[Task[str]] = [asyncio.create_task(vcp_for_display(d)) for d in displays]
                    await wait_and_reraise(futures)
                    payload = payload_creator(futures, displays)
                except Exception as e:
                    # LOGGER.error(f'vcp_op() failure: {e}')
                    payload = [1]
            else:
                payload = [1, 'no DDC displays connected']
            await _send_response(payload)

        data = (await reader.read()).decode()
        if data:
            data = json.loads(data)
            LOGGER.debug(f'received task {data} from client')
            match data:
                case ['get', individual, raw]:
                    try:
                        payload = [0, *get_brightness(individual, raw)]
                    except Exception:
                        payload = [1]
                    await _send_response(payload)
                case ['setvcp', *params]:
                    await vcp_op(lambda d: d._set_vcp_feature(*params), lambda *_: [0])
                case ['getvcp', *params]:
                    await vcp_op(lambda d: d._get_vcp_feature(*params),
                                 lambda futures, displays: [0, *[f'{displays[i].id},{j.result()}' for i, j in enumerate(futures)]])
                case _:
                    LOGGER.debug(f'placing task in queue...')
                    await TASK_QUEUE.put(data)

    server = await asyncio.start_unix_server(process_client_command, CONF.get('socket_path'))
    await server.serve_forever()


async def delta_brightness(delta: int):
    LOGGER.debug(f'placing brightness change in queue for delta {"+" if delta > 0 else ""}{delta}')
    await TASK_QUEUE.put(['delta', delta])


async def terminate():
    LOGGER.info('placing termination request in queue')
    await TASK_QUEUE.put(['kill'])
    # alternatively, ignore existing queue and terminate immediately:
    # try:
        # await write_state(CONF)
    # finally:
        # os._exit(0)


# TODO: should we return CONF.get('state').get('last_set_brightness') for single display?
def get_brightness(individual: bool, raw: bool) -> list[str|int]:
    if individual:
        return [f'{d.id},{d.get_brightness(raw)}' for d in DISPLAYS]
    return [DISPLAYS[0].get_brightness(raw)] if DISPLAYS else []


async def periodic_init(period: int) -> NoReturn:
    delta_threshold_sec = period - 1 - CONF.get('msg_consumption_window_sec')
    while True:
        await asyncio.sleep(period)
        if unix_time_now() - LAST_INIT_TIME >= delta_threshold_sec:
            LOGGER.debug('placing periodic [init] task on the queue...')
            await TASK_QUEUE.put(['init'])


async def run() -> None:
    try:
        validate_ext_deps()
        await init_displays()
        async with asyncio.TaskGroup() as tg:
            tg.create_task(process_q())
            tg.create_task(process_client_commands())
            if CONF.get('monitor_udev'):
                debounced = Debouncer(delay=CONF.get('udev_event_debounce_sec'))
                f = functools.partial(debounced, TASK_QUEUE.put, ['init'])
                tg.create_task(monitor_udev_events('drm', 'change', f))
            if CONF.get('periodic_init_sec'):
                tg.create_task(periodic_init(CONF.get('periodic_init_sec')))

            loop: AbstractEventLoop = asyncio.get_running_loop()
            loop.add_signal_handler(signal.SIGUSR1,
                                    lambda: tg.create_task(delta_brightness(CONF.get('brightness_step'))))
            loop.add_signal_handler(signal.SIGUSR2,
                                    lambda: tg.create_task(delta_brightness(-CONF.get('brightness_step'))))
            loop.add_signal_handler(signal.SIGINT,
                                    lambda: tg.create_task(terminate()))
            loop.add_signal_handler(signal.SIGTERM,
                                    lambda: tg.create_task(terminate()))
    except* Exception as exc_group:
        LOGGER.debug(f'{len(exc_group.exceptions)} errs caught in exc group')
        await write_state(CONF)

        ee = exc_group.exceptions[0]
        if isinstance(ee, FatalErr):
            if isinstance(ee, ExitableErr):
                exit_code: int = ee.exit_code
            else:
                exit_code: int = CONF.get('fatal_exit_code')
            LOGGER.debug(f'FatalErr caught, exiting with code {exit_code}...')
            await NOTIF.notify_err(ee)
            sys.exit(exit_code)
        else:
            raise


# top-level err handler that's caught for stuff ran prior to task group.
# note unhandled exceptions in run() also get propageted up here
def root_exception_handler(type_: type[BaseException], value: BaseException, tbt: TracebackType|None) -> None:
    # LOGGER.debug('root exception handler triggered')
    if isinstance(value, ExitableErr):
        traceback.print_tb(tbt)
        # NOTIF.notify_err_sync(value)
        sys.exit(value.exit_code)
    sys.__excepthook__(type_, value, tbt)


def main(debug=False, sim_conf: SimConf|None = None) -> None:
    global LOCK
    global TASK_QUEUE
    global CONF
    global NOTIF

    # sys.excepthook = root_exception_handler

    LOCK = singleton.SingleInstance()
    TASK_QUEUE = asyncio.Queue()

    CONF = load_config(load_state=True)
    CONF['sim'] = sim_conf
    NOTIF = Notif(CONF.get('notify'))

    log_lvl = logging.DEBUG if debug else getattr(logging, CONF.get('log_lvl', 'INFO'))
    logging.basicConfig(stream=sys.stdout, level=log_lvl)

    asyncio.run(run())
