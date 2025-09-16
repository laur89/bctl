from typing import TypedDict


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


class NotifyIconConf(TypedDict):
    error: str
    root_dir: str
    brightness_full: str
    brightness_high: str
    brightness_medium: str
    brightness_low: str
    brightness_off: str


class NotifyConf(TypedDict):
    enabled: bool
    on_fatal_err: bool
    timeout_ms: int
    icon: NotifyIconConf


class Conf(TypedDict):
    log_lvl: str
    ddcutil_bus_path_prefix: str
    ddcutil_brightness_feature: str
    ddcutil_svcp_flags: list[str]
    ddcutil_gvcp_flags: list[str]
    monitor_udev: bool
    periodic_init_sec: int
    sync_brightness: bool
    sync_strategy: str
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


default_conf: Conf = {
    'log_lvl': 'INFO',  # daemon log level, doesn't apply to the client
    'ddcutil_bus_path_prefix': '/dev/i2c-',  # prefix to the bus number
    'ddcutil_brightness_feature': '10',
    'ddcutil_svcp_flags': ['--skip-ddc-checks'],  # flags passed to [ddcutil setvcp] commands
    'ddcutil_gvcp_flags': [],  # flags passed to [ddcutil getvcp] commands
    'monitor_udev': True,  # monitor udev events for drm subsystem to detect ext. display (dis)connections
    'periodic_init_sec': 0,  # periodically re-init/re-detect monitors; 0 to disable
    'sync_brightness': False,  # keep all displays' brightnesses at same value/synchronized
    'sync_strategy': 'MEAN',  # if displays' brightnesses differ and are synced, what value to sync them to; only active if sync_brightness=True;
                              # 'MEAN' = set to arithmetic mean, 'LOW' = set to lowest, 'HIGH' = set to highest
    'notify': {
        'enabled': True,
        'on_fatal_err': True,  # whether desktop notifications should be shown on fatal errors
        'timeout_ms': 4000,
        'icon': {
            'error': 'gtk-dialog-error',
            'root_dir': '',
            'brightness_full': 'notification-display-brightness-full.svg',
            'brightness_high': 'notification-display-brightness-high.svg',
            'brightness_medium': 'notification-display-brightness-medium.svg',
            'brightness_low': 'notification-display-brightness-low.svg',
            'brightness_off': 'notification-display-brightness-off.svg'
        }
    },
    'udev_event_debounce_sec': 3.0,  # both for debouncing & delay; have experienced missed ext. display detection w/ 1.0
    'msg_consumption_window_sec': 0.1,  # can be set to 0 if no delay/window is required
    'brightness_step': 5,  # %
    'ignored_displays': [],  # either [ddcutil --brief detect] cmd "Monitor:" value, or <device> in /sys/class/backlight/<device>
    'ignore_internal_display': False,  # do not control internal (i.e. laptop) display if available
    'ignore_external_display': False,  # do not control external display(s) if available
    'main_display_ctl': 'DDCUTIL',  # RAW | DDCUTIL | BRIGHTNESSCTL | BRILLO
    'internal_display_ctl': 'RAW',  # RAW | BRIGHTNESSCTL | BRILLO;  only used if main_display_ctl=DDCUTIL and we're a laptop
    'raw_device_dir': '/sys/class/backlight',  # used if main_display_ctl=RAW OR
                                               # (main_display_ctl=DDCUTIL AND internal_display_ctl=RAW AND we're a laptop)
    'fatal_exit_code': 100,  # exit code signifying fatal exit that should not be retried;
                             # you might want to use this value in systemd unit file w/ RestartPreventExitStatus config
    'socket_path': '/tmp/.bctld-ipc.sock',
    'sim': None,  # simulation config, will be set by sim client
    'state_f_path': '/tmp/.bctld.state',  # state that should survive restarts are stored here
    'state': None  # do not set, will be read in from state_f_path
}

