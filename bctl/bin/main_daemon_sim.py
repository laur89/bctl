#!/usr/bin/env python3

import click
import bctl.daemon as daemon
from ..config import SimConf

@click.command
@click.option(
    '--debug',
    is_flag=True,
    help='Enables logging at debug level.')
@click.option('-n', '--number', default=3,
              help='Number of simulated displays')
@click.option('-w', '--wait', type=float, required=True,
              help='How long to wait for work simulation')
@click.option('-b', '--brightness',
              help='Initial brightness', default=50)
@click.option('-f', '--fail', type=str,
              help='Failure mode to simulate')
@click.option('-e', '--exit', default=1,
              help='code to exit chosen failmode with')
def main(debug, number, wait, brightness, fail, exit):
    failmodes = ['i', 's']
    assert number > 0, 'number of simulated displays must be positive'
    assert fail in failmodes + [None], f'allowed failmodes are {failmodes}'
    assert number >= 0, 'exit code needs to be >= 0'

    sim: SimConf = {
        'ndisplays': number,
        'wait_sec': wait,
        'initial_brightness': brightness,
        'failmode': fail,
        'exit_code': exit,
    }

    daemon.main(debug, sim)


if __name__ == '__main__':
    main()

