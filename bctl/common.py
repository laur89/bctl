import os
import json
import asyncio
import shutil
from datetime import datetime
from collections.abc import Iterable, Sequence
from typing import TypedDict
from asyncio import Task
import logging
from logging import Logger
import aiofiles as aiof
from .exceptions import FatalErr
from pydash import py_

STATE_VER = 1  # bump this whenever persisted state data structure changes
TIME_DIFF_DELTA_THRESHOLD_S = 60

LOGGER: Logger = logging.getLogger(__name__)

def _conf_path() -> str:
    xdg_dir = os.environ.get('XDG_CONFIG_DIR', f'{os.environ["HOME"]}/.config')
    return xdg_dir + '/bctl/config.json'


class SimConf(TypedDict):
    ndisplays: int
    wait_sec: float
    initial_brightness: dict
    failmode: str|None
    exit_code: int


class State(TypedDict):
    timestamp: int
    ver: int
    last_set_brightness: int


class NotifyConf(TypedDict):
    enabled: bool
    on_fatal_err: bool
    err_icon: str
    icon_root: str
    brightness_full: str
    brightness_high: str
    brightness_medium: str
    brightness_low: str
    brightness_off: str
    timeout_ms: int


class Conf(TypedDict):
    log_lvl: str
    ddcutil_bus_path_prefix: str
    ddcutil_brightness_feature: str
    ddcutil_svcp_flags: list[str]
    ddcutil_gvcp_flags: list[str]
    monitor_udev: bool
    periodic_init_sec: int
    sync_brightness: bool
    notify: NotifyConf
    udev_event_debounce_sec: float
    msg_consumption_window_sec: float
    brightness_step: int
    ignored_displays: list[str]
    ignore_internal_display: bool
    ignore_external_display: bool
    main_display_ctl: str
    internal_display_ctl: str
    raw_device_dir: str
    fatal_exit_code: int
    socket_path: str
    sim: SimConf | None
    state_f_path: str
    state: State


EMPTY_STATE: State = {
    'timestamp': 0,
    'ver': -1,
    'last_set_brightness': -1
}


def load_config(load_state: bool = False) -> Conf:
    conf: Conf = {
        'log_lvl': 'INFO',
        'ddcutil_bus_path_prefix': '/dev/i2c-',  # prefix to the bus number
        'ddcutil_brightness_feature': '10',  # str!
        'ddcutil_svcp_flags': ['--skip-ddc-checks'],  # flags passed to [ddcutil setvcp] commands
        'ddcutil_gvcp_flags': [],  # flags passed to [ddcutil getvcp] commands
        'monitor_udev': True,  # monitor udev events for drm subsystem to detect ext. display (dis)connections
        'periodic_init_sec': 0,  # periodically re-init/re-detect monitors; 0 to disable
        'sync_brightness': False,  # try to keep all displays' brightnesses at same value/synchronized
        'notify': {
            'enabled': True,
            'on_fatal_err': True,  # whether desktop notifications should be shown on fatal errors
            'err_icon': 'gtk-dialog-error',
            'icon_root': '',
            'brightness_full': 'notification-display-brightness-full.svg',
            'brightness_high': 'notification-display-brightness-high.svg',
            'brightness_medium': 'notification-display-brightness-medium.svg',
            'brightness_low': 'notification-display-brightness-low.svg',
            'brightness_off': 'notification-display-brightness-off.svg',
            'timeout_ms': 4000
        },
        'udev_event_debounce_sec': 3.0,  # both for debouncing & delay; have experienced missed ext. display detection w/ 1.0
        'msg_consumption_window_sec': 0.1,  # can be set to 0 if no delay/window is required
        'brightness_step': 5,  # %
        'ignored_displays': [],  # either ddcutil's "Monitor:" value, or <device> in /sys/class/backlight/<device>
        'ignore_internal_display': False,  # do not control internal display if available
        'ignore_external_display': False,  # do not control external display(s) if available
        'main_display_ctl': 'DDCUTIL',  # RAW | DDCUTIL | BRIGHTNESSCTL | BRILLO
        'internal_display_ctl': 'RAW',  # RAW | BRIGHTNESSCTL | BRILLO;  only used if main_display_ctl=DDCUTIL and we're a laptop
        'raw_device_dir': '/sys/class/backlight',  # used if (main_display_ctl=DDCUTIL and internal_display_ctl=RAW AND we're a laptop)
                                                   # OR main_display_ctl=RAW
        'fatal_exit_code': 100,  # exit code signifying fatal exit code, should not be retried;
                                 # you might want to use this value in systemd unit file w/ RestartPreventExitStatus config
        'socket_path': '/tmp/.bctld-ipc.sock',
        'sim': None,  # simulation config, will be set by sim client
        'state_f_path': '/tmp/.bctld.state',
        'state': None  # will be read from state_f_path
    }

    conf = py_.merge(conf, _read_dict_from_file(_conf_path()))

    if load_state:
        conf['state'] = _load_state(conf['state_f_path'])

    # LOGGER.debug(f'effective config: {conf}')
    return conf


def _load_state(file_loc: str) -> State:
    s: State = _read_dict_from_file(file_loc)

    t = s.get('timestamp', 0)
    v = s.get('ver', -1)
    if (unix_time_now() - t <= TIME_DIFF_DELTA_THRESHOLD_S and v == STATE_VER):
        return s
    return EMPTY_STATE.copy()


async def write_state(conf: Conf) -> None:
    data: State = {
        'timestamp': unix_time_now(),
        'ver': STATE_VER,
        'last_set_brightness': conf.get('state').get('last_set_brightness')
    }

    try:
        LOGGER.debug('storing state...')
        statef = conf.get('state_f_path')
        payload = json.dumps(
            data,
            indent=2,
            sort_keys=True,
            separators=(',', ': '),
            ensure_ascii=False)

        async with aiof.open(statef, mode='w') as f:
            await f.write(payload)
        LOGGER.debug('...state stored')
    except IOError as e:
        raise e


def _read_dict_from_file(file_loc: str) -> dict:
    if not (os.path.isfile(file_loc) and os.access(file_loc, os.R_OK)):
        return {}

    try:
        with open(file_loc, 'r') as f:
            return json.load(f)
    except Exception as e:
        LOGGER.error(f'error trying to read file {file_loc}')
        return {}


def unix_time_now() -> int:
    return int(datetime.now().timestamp())


def same_values(s: Sequence):
    return s.count(s[0]) == len(s)


async def run_cmd(cmd: Iterable[str] | str, throw_on_err=False, logger=None) -> tuple[str, str, int | None]:
    if type(cmd) == str:
        cmd = cmd.split()

    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE)

    stdout, stderr = await proc.communicate()
    if proc.returncode != 0:
        if logger:
            logger.error(f'{cmd} returned w/ {proc.returncode}')
        if throw_on_err:
            raise RuntimeError(f'{cmd} returned w/ {proc.returncode}')
    return stdout.decode(), stderr.decode(), proc.returncode


def assert_cmd_exist(cmd: str) -> None:
    if shutil.which(cmd) is None:
        raise FatalErr(f'external command [{cmd}] does not exist on our PATH')


# convenience method for waiting for futures' completion. it was created so any
# exceptions thrown in coroutines would be propagated up, and not swallowed.
# looks like task cancellation is the key for this, at least w/ return_when=asyncio.FIRST_EXCEPTION
async def wait_and_reraise(futures: Iterable[Task]) -> None:
    try:
        done, tasks_to_cancel = await asyncio.wait(futures, timeout=5, return_when=asyncio.FIRST_EXCEPTION)
    except asyncio.CancelledError:
        tasks_to_cancel = futures
        raise
    finally:
        for task in tasks_to_cancel:
            task.cancel()

    for task in done:
        if exc:=task.exception():
            # print(f'exc type: {type(exc)}: {exc}')
            raise exc
