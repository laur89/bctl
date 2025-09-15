import os
import json
import asyncio
import shutil
import logging
import aiofiles as aiof
from datetime import datetime
from asyncio import Task
from logging import Logger
from pydash import py_
from collections.abc import Iterable, Sequence
from .exceptions import FatalErr
from .config import Conf, State, default_conf

STATE_VER = 1  # bump this whenever persisted state data structure changes
TIME_DIFF_DELTA_THRESHOLD_S = 60

LOGGER: Logger = logging.getLogger(__name__)

def _conf_path() -> str:
    xdg_dir = os.environ.get('XDG_CONFIG_HOME', f'{os.environ["HOME"]}/.config')
    return xdg_dir + '/bctl/config.json'


EMPTY_STATE: State = {
    'timestamp': 0,
    'ver': -1,
    'last_set_brightness': -1
}


def load_config(load_state: bool = False) -> Conf:
    conf = py_.merge(default_conf, _read_dict_from_file(_conf_path()))

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
    current_state: State = conf.get('state')
    data: State = {
        'timestamp': unix_time_now(),
        'ver': STATE_VER,
        'last_set_brightness': current_state.get('last_set_brightness')
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
        LOGGER.error(f'error trying to parse json from {file_loc}')
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
