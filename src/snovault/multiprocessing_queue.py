from multiprocessing.managers import BaseManager
import queue

class MPQueue(object):

    def get_chunk_of_uuids(self, size):
        chunk = []
        try:
            for i in range(size):
                chunk.append(self.queue.get(False))
        except queue.Empty:
            pass
        return chunk

    def populate_shared_queue(self, invalidated, xmin, snapshot_id):
        for uuid in invalidated:
            self.queue.put((uuid, xmin, snapshot_id))

class QueueServer(MPQueue):

    def __init__(self, registry):
        self.queue = queue.Queue()
        QueueManager.register('get_queue', callable=lambda:self.queue)
        self.manager = QueueManager(address=('', 50000), 
                                    authkey=registry.settings['queue_authkey'].encode('utf-8'))
        self.start()

    def start(self):
        self.manager.start()

class QueueClient(MPQueue):

    def __init__(self, registry):
        QueueManager.register('get_queue')
        self.manager = QueueManager(address=(registry.settings['queue_server_address'], 50000), 
                                    authkey=registry.settings['queue_authkey'].encode('utf-8'))
        self.manager.connect()
        self.queue = self.manager.get_queue()


class QueueManager(BaseManager):
    pass
