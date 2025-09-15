from dbus_next import Variant
from desktop_notify.aio import Server as AioNotifServer
from .config import NotifyConf, NotifyIconConf
# from desktop_notify.glib import Server as SyncNotifSerer


class Notif:
    def __init__(self, conf: NotifyConf) -> None:
        self.conf: NotifyConf = conf
        self.icon_conf: NotifyIconConf = conf.get('icon')
        self.notif: AioNotifServer | None = AioNotifServer('bctld') if conf.get('enabled') else None

    # icon spec @ https://specifications.freedesktop.org/icon-theme-spec/latest/
    def _get_notif_icon(self, val: int) -> str:
        if val <= 10:
            icon = self.icon_conf.get('brightness_off')
        elif val <= 30:
            icon = self.icon_conf.get('brightness_low')
        elif val <= 55:
            icon = self.icon_conf.get('brightness_medium')
        elif val <= 85:
            icon = self.icon_conf.get('brightness_high')
        else:
            icon = self.icon_conf.get('brightness_full')

        i_root = self.icon_conf.get('root_dir')
        if icon and i_root and not i_root.endswith('/'): i_root += '/'
        return i_root + icon

    async def notify_err(self, err: Exception) -> None:
        if self.notif is None or not self.conf.get('on_fatal_err'): return

        notify = self.notif.Notify('bctl', str(err))\
            .set_icon(self.icon_conf.get('error'))\
            .set_timeout(0)
        await notify.show()

    # note as per https://wiki.archlinux.org/title/Desktop_notifications#Replace_previous_notification
    # 'string:x-canonical-private-synchronous:' hint is for replacing existing notification
    # the same way referencing it by ID does
    async def notify_change(self, val: int) -> None:
        if self.notif is None: return

        notify = self.notif.Notify(str(val))\
            .set_hint('value', Variant('i', val))\
            .set_hint('x-canonical-private-synchronous', Variant('s', 'brightness_bctld'))\
            .set_icon(self._get_notif_icon(val))\
            .set_timeout(self.conf.get('timeout_ms'))
            # .set_id(1002)
        await notify.show()

    # desktop-notify's glib module seems broken
    # def notify_err_sync(self, err: Exception) -> None:
        # if self.notif is None or not self.conf.get('on_fatal_err'): return
        # notif = SyncNotifSerer('bctl')
        # notif = notif.Notify(str(err))
        # notif.show()

