# standard imports
import logging
import os
import importlib
import datetime

# external imports
import cic_eth.cli
from chainlib.chain import ChainSpec
from cic_eth_registry.error import UnknownContractError
from cic_eth.registry import connect as connect_registry
from chainlib.eth.block import block_latest
from hexathon import (
        to_int as hex_to_int,
        strip_0x,
        )
from chainsyncer.error import SyncDone
from chainsyncer.driver.chain_interface import ChainInterfaceDriver
from cic_eth.db import dsn_from_config
from cic_eth.db.models.base import SessionBase

# local imports
#from cic_tracker.cache import SyncTimeRedisCache
from cic_tracker.chain import EthChainInterface

logging.STATETRACE = 5
logging.basicConfig(level=logging.WARNING)
logg = logging.getLogger()

script_dir = os.path.realpath(os.path.dirname(__file__))
exec_dir = os.path.realpath(os.getcwd()) #default_config_dir = os.environ.get('CONFINI_DIR', os.path.join(exec_dir, 'config'))
base_config_dir = os.path.join(script_dir, '..', 'data', 'config')

arg_flags = cic_eth.cli.argflag_std_read
local_arg_flags = cic_eth.cli.argflag_local_sync
argparser = cic_eth.cli.ArgumentParser(arg_flags)
argparser.add_argument('--session-id', dest='session_id', type=str, help='Session id to use for state store')
argparser.add_argument('--until', type=int, default=0, help='Stop sync at the given block. 0 = infinite sync')
argparser.process_local_flags(local_arg_flags)
args = argparser.parse_args()

# process config
config = cic_eth.cli.Config.from_args(args, arg_flags, local_arg_flags, base_config_dir=base_config_dir)
args_override = {
        'SYNC_OFFSET': getattr(args, 'offset'),
        'SYNC_SESSION_ID': getattr(args, 'session_id'),
        }
config.add(args.until, '_UNTIL', True)

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

chain_interface = EthChainInterface()

dsn = dsn_from_config(config)
SessionBase.connect(dsn, pool_size=16, debug=config.true('DATABASE_DEBUG'))


def filters_from_config(config):
    modules = []
    for v in config.get('SYNC_FILTER').split(','):
        (path, cls) = v.rsplit('.', maxsplit=1)
        m = importlib.import_module(path)
        o = getattr(m, cls)
        m = o(chain_spec, registry, config.get('CELERY_QUEUE'))
        modules.append(m)
    return modules


def pre_callback():
    logg.debug('starting sync loop iteration')


def post_callback():
    logg.debug('ending sync loop iteration')


def block_callback(block, tx):
    logg.info('processing {} {}'.format(block, datetime.datetime.fromtimestamp(block.timestamp)))


def state_change_callback(k, old_state, new_state):
    logg.log(logging.STATETRACE, 'state change: {} {} -> {}'.format(k, old_state, new_state)) 


def filter_change_callback(k, old_state, new_state):
    logg.log(logging.STATETRACE, 'filter change: {} {} -> {}'.format(k, old_state, new_state)) 


def main():
    o = block_latest()
    r = conn.do(o)
    block_offset = int(strip_0x(r), 16) + 1
    logg.info('network block height is {}'.format(block_offset))

    keep_alive = False
    session_block_offset = 0
    block_limit = 0
    session_block_offset = int(config.get('SYNC_OFFSET'))

    until = int(config.get('_UNTIL'))
    if until > 0:
        if until <= session_block_offset:
            raise ValueError('sync termination block number must be later than offset ({} >= {})'.format(session_block_offset, until))
        block_limit = args.until
    elif config.true('_KEEP_ALIVE'):
        keep_alive=True
        block_limit = -1

    if session_block_offset == -1:
        session_block_offset = block_offset
    elif not config.true('_KEEP_ALIVE'):
        if block_limit == 0:
            block_limit = block_offset
   
    filters = filters_from_config(config)
    syncer_store_module = None
    syncer_store_class = None
    if config.get('SYNC_BACKEND') == 'fs': 
        syncer_store_module = importlib.import_module('chainsyncer.store.fs')
        syncer_store_class = getattr(syncer_store_module, 'SyncFsStore')
    elif config.get('SYNC_BACKEND') == 'rocksdb':
        syncer_store_module = importlib.import_module('chainsyncer.store.rocksdb')
        syncer_store_class = getattr(syncer_store_module, 'SyncRocksDbStore')
    else:
        syncer_store_module = importlib.import_module(config.get('SYNC_BACKEND'))
        syncer_store_class = getattr(syncer_store_module, 'SyncStore')

    logg.info('using engine {} module {}.{}'.format(config.get('SYNC_BACKEND'), syncer_store_module.__file__, syncer_store_class.__name__))

    state_dir = os.path.join(config.get('SYNC_DIR'), config.get('SYNC_BACKEND'))
    sync_store = syncer_store_class(state_dir, session_id=config.get('SYNC_SESSION_ID'), state_event_callback=state_change_callback, filter_state_event_callback=filter_change_callback)
    logg.info('session is {}'.format(sync_store.session_id))

    for fltr in filters:
        sync_store.register(fltr)
    drv = ChainInterfaceDriver(sync_store, chain_interface, offset=session_block_offset, target=block_limit, pre_callback=pre_callback, post_callback=post_callback, block_callback=block_callback)

    i = 0
    try:
        r = drv.run(conn)
    except SyncDone as e:
        sys.stderr.write("sync {} done at block {}\n".format(drv, e))


if __name__ == '__main__':
    main()
