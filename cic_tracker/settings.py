# standard imports
import logging
import importlib
import os

# external imports
from chainlib.eth.block import block_latest
from hexathon import (
        to_int as hex_to_int,
        strip_0x,
        )
from cic_base import CICSettings
from cic_sync_filter.callback import CallbackFilter

# local imports
from cic_tracker.chain import EthChainInterface
from cic_tracker.callback import (
        state_change_callback,
        filter_change_callback,
        )

logg = logging.getLogger(__name__)


class CICTrackerSettings(CICSettings):

    def process_sync_callback_filters(self, config):
        self.o['CALLBACK_FILTERS'] = []
        for cb in config.get('TASKS_TRANSFER_CALLBACKS', '').split(','):
            task_split = cb.split(':')
            if len(task_split) > 1:
                task_queue = task_split[0]
            callback_filter = CallbackFilter(self.o['CHAIN_SPEC'], task_split[1], self.o['CELERY_QUEUE'])
            self.o['CALLBACK_FILTERS'].append(callback_filter)


    def process_sync_interface(self, config):
        self.o['SYNCER_INTERFACE'] = EthChainInterface()

    
    def process_sync_range(self, config):
        o = block_latest()
        r = self.o['RPC'].do(o)
        block_offset = int(strip_0x(r), 16) + 1
        logg.info('network block height at startup is {}'.format(block_offset))

        keep_alive = False
        session_block_offset = 0
        block_limit = 0
        session_block_offset = int(config.get('SYNCER_OFFSET'))

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
    
        self.o['SYNCER_OFFSET'] = session_block_offset
        self.o['SYNCER_LIMIT'] = block_limit


    def process_sync_store(self, config):
        syncer_store_module = None
        syncer_store_class = None
        if config.get('SYNCER_BACKEND') == 'fs': 
            syncer_store_module = importlib.import_module('chainsyncer.store.fs')
            syncer_store_class = getattr(syncer_store_module, 'SyncFsStore')
        elif config.get('SYNCER_BACKEND') == 'rocksdb':
            syncer_store_module = importlib.import_module('chainsyncer.store.rocksdb')
            syncer_store_class = getattr(syncer_store_module, 'SyncRocksDbStore')
        else:
            syncer_store_module = importlib.import_module(config.get('SYNCER_BACKEND'))
            syncer_store_class = getattr(syncer_store_module, 'SyncStore')

        logg.info('using engine {} module {}.{}'.format(config.get('SYNCER_BACKEND'), syncer_store_module.__file__, syncer_store_class.__name__))

        state_dir = os.path.join(config.get('SYNCER_DIR'), config.get('SYNCER_BACKEND'))
        sync_store = syncer_store_class(state_dir, session_id=config.get('SYNCER_SESSION_ID'), state_event_callback=state_change_callback, filter_state_event_callback=filter_change_callback)
        logg.info('sync session is {}'.format(sync_store.session_id))

        self.o['SYNCER_STORE'] = sync_store


    def process_sync_filters(self, config):
        for v in config.get('SYNCER_FILTER').split(','):
            logg.debug('processing filter {}'.format(v))
            (path, cls) = v.rsplit('.', maxsplit=1)
            m = importlib.import_module(path)
            o = getattr(m, cls)
            m = o(self.o['CHAIN_SPEC'], self.o['CIC_REGISTRY'], config.get('CELERY_QUEUE'))

            if v == 'cic_syncer.filter.callback.CallbackFilter':
                m.set_method()
                o.trusted_addresses = trusted_addresses

            self.o['SYNCER_STORE'].register(m)


    def process(self, config):
        super(CICTrackerSettings, self).process(config)
        self.process_sync_range(config)
        self.process_sync_interface(config)
        self.process_sync_store(config)
        self.process_sync_callback_filters(config)
        self.process_sync_filters(config)


    def __str__(self):
        ks = list(self.o.keys())
        ks.sort()
        s = ''
        for k in ks:
            s += '{}: {}\n'.format(k, self.o.get(k))
        return s
