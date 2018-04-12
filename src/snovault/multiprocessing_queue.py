from multiprocessing.managers import BaseManager
import queue
import atexit

class MPQueue(object):

    def get_chunk_of_uuids(self, size=1000):
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

    def to_list(self, queue_obj):
        arr = list()
        try:
            while True:
                arr.append(queue_obj.get(False))
        except queue.Empty:
            pass
        return arr

class QueueServer(MPQueue):

    def __init__(self, registry):
        local_queue = queue.Queue()
        local_result_queue = queue.Queue()
        local_done_queue = queue.Queue()
        QueueManager.register('get_queue', callable=lambda:local_queue)
        QueueManager.register('get_result_queue', callable=lambda:local_result_queue)
        QueueManager.register('get_done_queue', callable=lambda:local_done_queue)
        self.manager = QueueManager(address=('', 50000),
                                    authkey=registry.settings['queue_authkey'].encode('utf-8'))
        atexit.register(self.shutdown)
        self.manager.start()
        self.queue = self.manager.get_queue()
        self.result_queue = self.manager.get_result_queue()
        self.done_queue = self.manager.get_done_queue()


    def shutdown(self):
        self.manager.shutdown()

class QueueClient(MPQueue):

    def __init__(self, registry):
        QueueManager.register('get_queue')
        QueueManager.register('get_result_queue')
        QueueManager.register('get_done_queue')
        self.manager = QueueManager(address=(registry.settings['queue_server_address'], 50000),
                                    authkey=registry.settings['queue_authkey'].encode('utf-8'))
        self.manager.connect()
        self.queue = self.manager.get_queue()
        self.result_queue = self.manager.get_result_queue()
        self.done_queue = self.manager.get_done_queue()


class QueueManager(BaseManager):
    pass
