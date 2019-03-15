"""
Uuid Queue Module Adapter

- QueueAdapter Class allows access to all queue types
defined in QueueTypes through a set of standard methods.
- All queues in ./queues should adhere to QueueAdapter standards.
- Adapter queue has a server and a worker.
- Another important object is the meta data needed to run the queue.
"""
from .adapter_queue import QueueAdapter
from .adapter_queue import QueueTypes
