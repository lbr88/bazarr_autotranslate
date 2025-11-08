from collections import deque
import threading

class UniqueQueue:
    def __init__(self, key_fn):
        self.q = deque()
        self.seen = set()
        self.lock = threading.Lock()
        self.not_empty = threading.Condition(self.lock)
        self.key_fn = key_fn

    def put(self, item, priority=False):
        """Add item to queue. If priority=True, add to front of queue."""
        key = self.key_fn(item)
        with self.lock:
            if key not in self.seen:
                if priority:
                    self.q.appendleft(item)
                else:
                    self.q.append(item)
                self.seen.add(key)
                self.not_empty.notify()

    def get(self):
        with self.not_empty:
            while not self.q:
                self.not_empty.wait()
            item = self.q.popleft()
            return item
        
    def done(self, item):
        key = self.key_fn(item)
        with self.lock:
            if key in self.seen:
                self.seen.remove(key)
            else:
                raise ValueError("done() called on unknown item")
                
        
    def check(self, item):
        key = self.key_fn(item)
        with self.lock:
            return key in self.seen
    
    def qsize(self):
        """Return approximate size of queue."""
        with self.lock:
            return len(self.q)
    
    def peek(self, n=5):
        """Return the first n items in the queue without removing them."""
        with self.lock:
            return list(self.q)[:n]
    
    def move_to_top(self, index):
        """Move item at index to the front of the queue."""
        with self.lock:
            if 0 <= index < len(self.q):
                item = self.q[index]
                del self.q[index]
                self.q.appendleft(item)
                self.not_empty.notify()
                return True
            return False