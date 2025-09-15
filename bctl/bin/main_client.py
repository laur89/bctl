#!/usr/bin/env python3

# TODO: consider pyro5 for rpc, as opposed to json over AF_UNIX

import click
import bctl.client as client


@click.group
@click.pass_context
@click.option(
    '--debug',
    is_flag=True,
    help='Enables logging at debug level')
def main(ctx, debug):
    """Client for sending messages to BCTLD"""
    ctx.obj = client.Client(debug=debug)


@main.command
@click.pass_obj
@click.argument('delta', type=int, default=5)
def up(ctx, delta):
    """Bump up screens' brightness.

    :param ctx: context
    :param delta: % delta to bump brightness up by
    """
    assert delta > 0, 'brightness % to bump up by needs to be positive int'
    ctx.send_cmd(['delta', delta])


@main.command
@click.pass_obj
@click.argument('delta', type=int, default=5)
def down(ctx, delta):
    """Bump down screens' brightness.

    :param ctx: context
    :param delta: % delta to bump brightness down by
    """
    assert delta > 0, 'brightness % to bump down by needs to be positive int'
    ctx.send_cmd(['delta', -delta])


@main.command
@click.pass_obj
@click.argument('delta', type=int)
def delta(ctx, delta):
    """Change screens' brightness by given %

    :param ctx: context
    :param delta: % delta to change brightness down by
    """
    ctx.send_cmd(['delta', delta])


@main.command
@click.pass_obj
@click.argument('value', type=int)
def set(ctx, value):
    """Change screens' brightness to given %

    :param ctx: context
    :param value: % value to change brightness to
    """
    assert value >= 0, 'brightness % to set to needs to be >= 0'
    ctx.send_cmd(['set', value])


@main.command
@click.pass_obj
@click.argument('args', nargs=-1, type=str)
def setvcp(ctx, args: tuple[str, ...]):
    """Set VCP feature value(s) for all detected DDC displays

    :param ctx: context
    """
    assert len(args) >= 2, 'minimum 2 args needed, read ddcutil manual on [setvcp] command'
    ctx.send_receive_cmd(['setvcp', args])


@main.command
@click.pass_obj
@click.argument('args', nargs=-1, type=str)
def getvcp(ctx, args: tuple[str, ...]):
    """Get VCP feature value(s) for all detected DDC displays

    :param ctx: context
    """
    assert args, 'minimum 1 feature needed, read ddcutil manual on [getvcp] command'
    ctx.send_receive_cmd(['getvcp', args])


@main.command
@click.pass_obj
@click.option(
    '-i', '--individual',
    is_flag=True,
    help='retrieve brightness levels per screen')
@click.option(
    '-r', '--raw',
    is_flag=True,
    help='retrieve raw brightness value')
def get(ctx, individual: bool, raw: bool):
    """Get screens' brightness (%)

    :param ctx: context
    """
    ctx.send_receive_cmd(['get', individual, raw])


@main.command
@click.pass_obj
def init(ctx):
    """Re-initialize displays.

    :param ctx: context
    """
    ctx.send_cmd(['init'])


@main.command
@click.pass_obj
def sync(ctx):
    """Synchronize screens' brightness levels.

    :param ctx: context
    """
    ctx.send_cmd(['sync'])


@main.command
@click.pass_obj
def kill(ctx):
    """Terminate the daemon process.

    :param ctx: context
    """
    ctx.send_cmd(['kill'])


if __name__ == '__main__':
    main()

