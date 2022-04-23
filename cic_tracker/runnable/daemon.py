# standard imports
import logging
import os
import importlib

# external imports
import cic_eth.cli
from chainlib.chain import ChainSpec
from cic_eth_registry.error import UnknownContractError
from cic_eth.registry import connect as connect_registry
from chainlib.eth.block import block_latest
from hexathon import to_int as hex_to_int

# local imports
from cic_tracker.cache import SyncTimeRedisCache

logging.STATETRACE = 5
logging.basicConfig(level=logging.WARNING)
logg = logging.getLogger()

script_dir = os.path.realpath(os.path.dirname(__file__))
exec_dir = os.path.realpath(os.getcwd())
#default_config_dir = os.environ.get('CONFINI_DIR', os.path.join(exec_dir, 'config'))
base_config_dir = os.path.join(script_dir, '..', 'data', 'config')

arg_flags = cic_eth.cli.argflag_std_read
local_arg_flags = cic_eth.cli.argflag_local_sync
argparser = cic_eth.cli.ArgumentParser(arg_flags)
argparser.process_local_flags(local_arg_flags)
args = argparser.parse_args()

# process config
config = cic_eth.cli.Config.from_args(args, arg_flags, local_arg_flags, base_config_dir=base_config_dir)

# connect to celery
cic_eth.cli.CeleryApp.from_config(config)

# set up rpc
rpc = cic_eth.cli.RPC.from_config(config)
conn = rpc.get_default()

# set up chain provisions
chain_spec = ChainSpec.from_chain_str(config.get('CHAIN_SPEC'))
registry = None
try:
    registry = connect_registry(conn, chain_spec, config.get('CIC_REGISTRY_ADDRESS'))
except UnknownContractError as e:
    logg.exception('Registry contract connection failed for {}: {}'.format(config.get('CIC_REGISTRY_ADDRESS'), e))
    sys.exit(1)
logg.info('connected contract registry {}'.format(config.get('CIC_REGISTRY_ADDRESS')))


def filters_from_config(config):
    modules = []
    for v in config.get('SYNC_FILTER').split(','):
        (path, cls) = v.rsplit('.', maxsplit=1)
        logg.debug('path {}'.format(path))
        m = importlib.import_module(path)
        o = getattr(m, cls)
        m = o(chain_spec, registry, config.get('CELERY_QUEUE'))
        modules.append(m)


def main():
    # Connect to blockchain with chainlib

    o = block_latest()
    r = conn.do(o)
    # block_current = int(r, 16)
    try:
        block_current = hex_to_int(r, need_prefix=True)
    except ValueError:
        block_current = int(r, 16)
    block_offset = block_current + 1

    filters = filters_from_config(config)
#    syncer_store_module = None
#    syncer_store_class = None
#    if config.get('SYNC_BACKEND') == 'fs': 
#        syncer_store_module = importlib.import_module('chainsyncer.store.fs')
#        syncer_store_class = getattr(syncer_store_module, 'SyncFsStore')
#    elif config.get('SYNC_BACKEND') == 'rocksdb':
#        syncer_store_module = importlib.import_module('chainsyncer.store.rocksdb')
#        syncer_store_class = getattr(syncer_store_module, 'SyncRocksDbStore')
#    else:
#        syncer_store_module = importlib.import_module(config.get('SYNC_BACKEND'))
#        syncer_store_class = getattr(syncer_store_module, 'SyncStore')
#
#    logg.info('using engine {} module {}.{}'.format(config.get('SYNC_BACKEND'), syncer_store_module.__file__, syncer_store_class.__name__))
#
#    state_dir = os.path.join(config.get('SYNC_DIR'), config.get('SYNC_BACKEND'))
#    sync_store = syncer_store_class(state_dir, session_id=config.get('SYNC_SESSION_ID'), state_event_callback=state_change_callback, filter_state_event_callback=filter_change_callback)
#    logg.info('session is {}'.format(sync_store.session_id))
#
#    for fltr in filters:
#        sync_store.register(fltr)


if __name__ == '__main__':
    main()
