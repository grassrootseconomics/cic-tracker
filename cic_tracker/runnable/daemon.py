# standard imports
import logging
import os
import datetime
import importlib
import sys

# external imports
from chainsyncer.error import SyncDone
from chainsyncer.driver.chain_interface import ChainInterfaceDriver

# cic_eth legacy imports
import cic_base.cli

# local imports
#from cic_tracker.cache import SyncTimeRedisCache
from cic_tracker.settings import (
        CICTrackerSettings,
        process_settings,
        )
from cic_tracker.callback import (
        pre_callback,
        post_callback,
        block_callback,
        )


logging.STATETRACE = 5
logging.basicConfig(level=logging.WARNING)
logg = logging.getLogger()


script_dir = os.path.realpath(os.path.dirname(__file__))
exec_dir = os.path.realpath(os.getcwd()) #default_config_dir = os.environ.get('CONFINI_DIR', os.path.join(exec_dir, 'config'))
base_config_dir = os.path.join(script_dir, '..', 'data', 'config')

arg_flags = cic_base.cli.argflag_std_read
local_arg_flags = cic_base.cli.argflag_local_sync
argparser = cic_base.cli.ArgumentParser(arg_flags)
argparser.process_local_flags(local_arg_flags)
args = argparser.parse_args()

if args.list_backends:
    for v in [
            'fs',
            'rocksdb',
            'redis',
            ]:
        print(v)
    sys.exit(0)

# process config
config = cic_base.cli.Config.from_args(args, arg_flags, local_arg_flags, base_config_dir=base_config_dir)
config.add(args.until, '_UNTIL', True)


def main():
    settings = CICTrackerSettings()
    process_settings(settings, config)
    logg.debug('settings:\n' + str(settings))

    drv = ChainInterfaceDriver(
            settings.get('SYNCER_STORE'),
            settings.get('SYNCER_INTERFACE'),
            settings.get('SYNCER_OFFSET'),
            settings.get('SYNCER_LIMIT'),
            pre_callback=pre_callback,
            post_callback=post_callback,
            block_callback=block_callback,
            )
    i = 0
    try:
        r = drv.run(settings.get('RPC'))
    except SyncDone as e:
        sys.stderr.write("sync {} done at block {}\n".format(drv, e))


if __name__ == '__main__':
    main()
