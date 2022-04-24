# standard imports
import logging
import os
import datetime
import importlib

# external imports
from chainsyncer.error import SyncDone
from chainsyncer.driver.chain_interface import ChainInterfaceDriver

# cic_eth legacy imports
import cic_base.cli

# local imports
#from cic_tracker.cache import SyncTimeRedisCache
from cic_tracker.settings import CICTrackerSettings
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
argparser.add_argument('--session-id', dest='session_id', type=str, help='Session id to use for state store')
argparser.add_argument('--until', type=int, default=0, help='Stop sync at the given block. 0 = infinite sync')
argparser.process_local_flags(local_arg_flags)
args = argparser.parse_args()

# process config
config = cic_base.cli.Config.from_args(args, arg_flags, local_arg_flags, base_config_dir=base_config_dir)
args_override = {
        'SYNCER_OFFSET': getattr(args, 'offset'),
        'SYNCER_SESSION_ID': getattr(args, 'session_id'),
        }
config.add(args.until, '_UNTIL', True)


def main():
    settings = CICTrackerSettings()
    settings.process(config)
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
