# standard imports
import logging
import importlib
import os

# external imports
from chainlib.chain import ChainSpec
from chainlib.eth.address import is_checksum_address
from chainlib.eth.block import block_latest
from cic_eth.registry import (
        connect as connect_registry,
        connect_declarator,
        connect_token_registry,
        )
from cic_eth.db import dsn_from_config
from cic_eth.db.models.base import SessionBase
from cic_eth_registry.error import UnknownContractError
import cic_eth.cli
from hexathon import (
        to_int as hex_to_int,
        strip_0x,
        )
from cic_eth_registry import CICRegistry

# local imports
from cic_tracker.chain import EthChainInterface
from cic_tracker.callback import (
        state_change_callback,
        filter_change_callback,
        )

logg = logging.getLogger(__name__)


class CICTrackerSettings:

    def __init__(self):
        self.o = {}
        self.registry = None
        self.get = self.o.get


    def process_common(self, config):
        self.o['CHAIN_SPEC'] = ChainSpec.from_chain_str(config.get('CHAIN_SPEC'))
        
        rpc = cic_eth.cli.RPC.from_config(config)
        self.o['RPC'] = rpc.get_default()


    def process_celery(self, config):
        cic_eth.cli.CeleryApp.from_config(config)


    def process_database(self, config):
        dsn = dsn_from_config(config)
        SessionBase.connect(dsn, pool_size=16, debug=config.true('DATABASE_DEBUG'))


    def process_trusted_addresses(self, config):
        trusted_addresses_src = config.get('CIC_TRUST_ADDRESS')
        if trusted_addresses_src == None:
            raise InitializationError('At least one trusted address must be declared in CIC_TRUST_ADDRESS')

        trusted_addresses = trusted_addresses_src.split(',')
        for i, address in enumerate(trusted_addresses):
            if not config.get('_UNSAFE'):
                if not is_checksum_address(address):
                    raise ValueError('address {} at position {} is not a valid checksum address'.format(address, i))
            else:
                trusted_addresses[i] = to_checksum_address(address)
            logg.info('using trusted address {}'.format(address))


        self.o['TRUSTED_ADDRESSES'] = trusted_addresses


    def process_registry(self, config):
        registry = None
        try:
            registry = connect_registry(self.o['RPC'], self.o['CHAIN_SPEC'], config.get('CIC_REGISTRY_ADDRESS'))
        except UnknownContractError as e:
            pass
        if registry == None:
            raise InitializationError('Registry contract connection failed for {}: {}'.format(config.get('CIC_REGISTRY_ADDRESS'), e))
        connect_declarator(self.o['RPC'], self.o['CHAIN_SPEC'], self.o['TRUSTED_ADDRESSES'])
        connect_token_registry(self.o['RPC'], self.o['CHAIN_SPEC'])

        self.o['CIC_REGISTRY'] = CICRegistry(self.o['CHAIN_SPEC'], self.o['RPC'])


    def process_callback_filters(self, config):
        self.o['CALLBACK_FILTERS'] = []
        for cb in config.get('TASKS_TRANSFER_CALLBACKS', '').split(','):
            task_split = cb.split(':')
            if len(task_split) > 1:
                task_queue = task_split[0]
            callback_filter = CallbackFilter(chain_spec, task_split[1], task_queue)
            self.o['CALLBACK_FILTERS'].append(callback_filter)


    def process_sync_interface(self, config):
        self.o['SYNC_INTERFACE'] = EthChainInterface()

    
    def process_sync_range(self, config):
        o = block_latest()
        r = self.o['RPC'].do(o)
        block_offset = int(strip_0x(r), 16) + 1
        logg.info('network block height at startup is {}'.format(block_offset))

        keep_alive = False
        session_block_offset = 0
        block_limit = 0
        session_block_offset = int(config.get('SYNC_OFFSET'))

        until = int(config.get('_UNTIL'))
        if until > 0:
            if until <= session_block_offset:
                raise ValueError('sync termination block number must be later than offset ({} >= {})'.format(session_block_offset, until))
            block_limit = until
        else:
            keep_alive=True
            block_limit = -1

        if session_block_offset == -1:
            session_block_offset = block_offset
        elif not config.true('_KEEP_ALIVE'):
            if block_limit == 0:
                lock_limit = block_offset
    
        self.o['SYNC_OFFSET'] = session_block_offset
        self.o['SYNC_LIMIT'] = block_limit


    def process_sync_store(self, config):
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
        logg.info('sync session is {}'.format(sync_store.session_id))

        self.o['SYNC_STORE'] = sync_store


    def process_sync_filters(self, config):
        for v in config.get('SYNC_FILTER').split(','):
            logg.debug('processing filter {}'.format(v))
            (path, cls) = v.rsplit('.', maxsplit=1)
            m = importlib.import_module(path)
            o = getattr(m, cls)
            m = o(self.o['CHAIN_SPEC'], self.o['CIC_REGISTRY'], config.get('CELERY_QUEUE'))

            if v == 'cic_syncer.filter.callback.CallbackFilter':
                m.set_method()
                o.trusted_addresses = trusted_addresses

            self.o['SYNC_STORE'].register(m)


    def process_sync(self, config):
        self.process_sync_range(config)
        self.process_sync_interface(config)
        self.process_sync_store(config)
        self.process_sync_filters(config)


    def process(self, config):
        self.process_common(config)
        self.process_celery(config)
        self.process_database(config)
        self.process_trusted_addresses(config)
        self.process_registry(config)
        self.process_sync(config)


    def __str__(self):
        ks = list(self.o.keys())
        ks.sort()
        s = ''
        for k in ks:
            s += '{}: {}\n'.format(k, self.o.get(k))
        return s
