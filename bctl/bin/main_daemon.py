#!/usr/bin/env python3

import click
import bctl.daemon as daemon


@click.command
@click.option(
    '--debug',
    is_flag=True,
    help='Enables logging at debug level.')
def main(debug):
    daemon.main(debug)


if __name__ == '__main__':
    main()

