# BCTL - brightness control

This is a simple daemon for controlling displays' brightnesses. It
consists of a daemon process listening for user requests (e.g. changing brightness)
and a client to send commands to the daemon. udev events are monitored
for screen (dis)connections. Desktop notifications are shown on brightness change.

## Installation

~~`$ pipx instal bctl`~~  not published yet

`$ pipx install git+ssh://git@github.com/laur89/bctl.git`

Note this will install the client & daemon executables, but it's user
responsibility to launch the daemon process, covered below.

## Why?

Main reason for this program is to provide a simple, general-puropse central point
for controlling brightness of _all_ the connected screens simultaneously and keeping
track of their current brightness levels.

Controlling laptops' internal screen is generally the easiest, as its device
object is exposed under `/sys/class/backlight/` dir -- it's the external
displays that tend to be trickier.

For controlling external screens' brightness there are roughly two main
methods, [explained below](#managing-external-displays). As the recommended
method -- ddcutil -- takes in some cases non-trivial amount of time to execute
(up to ~200ms), it can be slightly jarring to change brightness when spamming the key.

As this solution caches set brighness values there's no need to query it
from ddcutil, making e.g. desktop notification generation simpler.

Also screen connections & disconnections are kept track of via an udev monitor,
and there's an option to force all the screens' brightnesses to be kept in sync.

## Managing external displays

### [`ddcci` kernel driver](https://gitlab.com/ddcci-driver-linux/ddcci-driver-linux)

This kernel module _should_ detect the devices and expose 'em under
`/sys/class/backlight/`, just like the laptop's internal display (e.g.
`/sys/class/backlight/amdgpu_bl0`) is by default. This requires installation
of [ddcci-dkms](https://packages.debian.org/sid/ddcci-dkms) package and loading
`ddcci` kernel module.

Note as of '25 there are loads of issues w/ this kernel module's auto-detection
logic, e.g. see issues [7](https://gitlab.com/ddcci-driver-linux/ddcci-driver-linux/-/issues/7),
[42](https://gitlab.com/ddcci-driver-linux/ddcci-driver-linux/-/issues/42),
[46](https://gitlab.com/ddcci-driver-linux/ddcci-driver-linux/-/issues/46)

Current workaround seems to be manually enabling displays as per [this reddit post](https://old.reddit.com/r/gnome/comments/efkoya/using_ddccidriverlinux_you_can_get_native/fc0xrx6/):

- Before state (no external display devices listed/avail):

```sh
$ ls -l /sys/class/backlight
total 0
lrwxrwxrwx 1 root root 0 Sep  7 09:44 amdgpu_bl0 -> ../../devices/pci0000:00/0000:00:08.1/0000:07:00.0/drm/card0/card0-eDP-1/amdgpu_bl0
```

- Enable manually:

```sh
$ echo 'ddcci 0x37' | sudo tee /sys/bus/i2c/devices/i2c-11/new_device
```

- After (`ddcci11` external screen avail):

```sh
$ ls -l /sys/class/backlight
total 0
lrwxrwxrwx 1 root root 0 Sep  7 09:44 amdgpu_bl0 -> ../../devices/pci0000:00/0000:00:08.1/0000:07:00.0/drm/card0/card0-eDP-1/amdgpu_bl0
lrwxrwxrwx 1 root root 0 Sep  7 10:41 ddcci11 -> ../../devices/pci0000:00/0000:00:08.1/0000:07:00.0/i2c-11/11-0037/ddcci11/backlight/ddcci11
```

### [`ddcutil`](https://github.com/rockowitz/ddcutil)

**This is the recommended backend** for controlling external displays. Requires `i2c` 
kernel module, but as of [v1.4](https://www.ddcutil.com/config_steps/) "_ddcutil
installation should automatically install this file, making manual configuration
unnecessary_"

**Note**: [arch wiki states](https://wiki.archlinux.org/title/Backlight):
> Using ddcci and i2c-dev simultaneously may result in resource conflicts such as
  a Device or resource busy error

Meaning it's best to choose one of the options, not both.

## Usage

### Daemon

As mentioned earlier, a daemon process needs to be started that keeps track of
the displays. Easiest way to do so would be utilizing your OS's process
manager. An example of a systemd user service file (e.g.
`~/.config/systemd/user/bctld.conf`) would be:

```
[Unit]
Description=bctld aka brightness control daemon
PartOf=graphical-session.target
StartLimitIntervalSec=200
StartLimitBurst=15

[Service]
Type=simple
ExecStart=%h/.local/bin/bctld
Restart=on-failure
RestartSec=10
RestartPreventExitStatus=100

[Install]
WantedBy=graphical-session.target
```

### Client

With demon running, the client is used to send commands to the daemon. List
available commands via `bctl --help`

Some examples:

- `bctl up` - bump brightness up by `brightness_step` config
- `bctl down` - bump brightness down by `brightness_step` config
- `bctl delta 20` - bump brightness up by 20%
- `bctl delta -- -20` - bump brightness down by 20%
- `bctl set 55` - set brightness to 55%
- `bctl get` - returns current brightness level in %
- `bctl setvcp D6 01` - set vcp feature D6 to value 01 for all detected DDC displays;
  this is simply shortcut for `ddcutil setvcp D6 01`

The daemon also registers signal handlers for `SIGUSR1` & `SIGUSR2`, so
sending said signals to the daemon process allows bumping brightness up
and down respectively; e.g.: `kill -s SIGUSR1 "$(pgrep -x bctld)"` or
`killall -s SIGUSR1 bctld`

### Socket

The client and daemon communicate over a unix socket set via `socket_path` config.
If using the provided client is too slow (e.g. for querying brightness), it's
possible to talk to the daemon directly over this socket. For instance current 
brightness can be fetched via following command, which is equivalent to `bctl get`:

```sh
$ socat - UNIX-CONNECT:/tmp/.bctld-ipc.sock <<< '["get",0,0]' | jq -re '.[1]'
75
```

Please note there will be no guarantees about the stability of this api as it's
part of internal comms spec.

## Configuration

User configuration file is read from `$XDG_CONFIG_HOME/bctl/config.json`.
For full config list see the [config.py](./bctl/config.py) file that defines the defaults,
but the most important ones you might want to consider changing are:

| Config | Type | Default | Description |
| --- | --- | --- | --- |
| `msg_consumption_window_sec` | float | 0.1 | event consumption window in seconds |
| `udev_event_debounce_sec` | float | 3.0 | udev event debounce window in seconds |
| `brightness_step` | int | 5 | percentage to bump brightness up or down per change |
| `sync_brightness` | bool | False | whether to keep screens' brightnesses in sync |
| `main_display_ctl` | str | DDCUTIL | backend for brightness control |
| `internal_display_ctl` | str | RAW | backend for controlling internal display |
| `notify.icon.root_dir` | str | '' | notification icon directory |
| `fatal_exit_code` | int | 100 | error code daemon should exit with when restart shouldn't be attempted. you might want to use this value in systemd unit file w/ [`RestartPreventExitStatus`](https://www.freedesktop.org/software/systemd/man/latest/systemd.service.html#RestartPreventExitStatus=) config |

#### `msg_consumption_window_sec`

Defines an event consumption window, meaning if say 'brightness up' key is spammed
5x during said window, ddcutil is invoked just once bumping up the brightness by
5x<brightness_step> value, as opposed to running ddcutil 5 times bumping
1x<brightness_step> each time.

#### `main_display_ctl`

This config sets the main backend for controlling the brightness. Available options:
- `DDCUTIL` - controls _external_ displays via ddcutil, requires
  [`ddcutil`](https://github.com/rockowitz/ddcutil) to be on PATH, described above.
- `RAW` - all displays are controlled via the device interfaces under `/sys/class/backlight`
  directory. In order to control external displays using this backend, you'd
  likely need the installation of [`ddcci` kernel driver](https://gitlab.com/ddcci-driver-linux/ddcci-driver-linux),
  described above.
- `BRIGHTNESSCTL` - all displays are controlled via [`brightnessctl`](https://github.com/Hummer12007/brightnessctl)
  program.
- `BRILLO` - all displays are controlled via [`brillo`](https://gitlab.com/cameronnemo/brillo)
  program.

#### `internal_display_ctl`

This config sets the backend used only for controlling the internal display
brightness, as that's not what ddcutil does. Only in effect if
`main_display_ctl=DDCUTIL` and we're running on a laptop. Available options are
`RAW | BRIGHTNESSCTL | BRILLO`

#### `notify.icon.root_dir`

Notification icon directory. Icon is chosen based on brightness level, and final used icon
will be `notify.icon.root_dir` + `notify.icon.brightness_{full,high,medium,low,off}`.

Note either half of final value may be an empty string, so if you want to use
single icon for all levels, set icon full path to `notify.icon.root_dir` and
set `notify.icon.brightness_{full,high,medium,low,off}` values to an empty string.

## Troubleshooting

### External display (dis)connection not detected

Current implementation relies on listening for `drm` subsystem `change` action
udev events. Some graphic cards (and/or monitors, unsure) are known to either
not emit said events, emit them only sometimes, or emit different ones. Recommend
you try debugging it via running `$ udevadm monitor` that starts listening for udev
events, then connect or disconnect your monitor and see what events are printed out.
With that info feel free to open an issue.

As a hacky workaround it's also possible to enable periodic polling by setting
`periodic_init_sec` to seconds at which interval display detection should
happen. Wouldn't set it to anything lower than 30.

Additionally you may opt out of udev monitoring altoghether (see [config.py](./bctl/config.py)),
and rely on your own custom detection; in that case daemon can be asked to
re-initialize its state by sending init command via the client: `$ bctl init`
