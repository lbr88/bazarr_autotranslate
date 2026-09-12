"""
Centralized queue and item state management.
Simple, robust, and thread-safe with persistence.
"""
import threading
import time
import json
import os
from typing import Dict, List, Optional, Any
from collections import deque
from class_types import SubtitleTranslate

class QueueManager:
    """
    Manages all translation items and their lifecycle states.
    
    States:
    - queued: Waiting in queue
    - processing: Currently being translated
    - retrying: Failed but will retry
    - completed: Successfully translated
    - failed: Permanently failed (max retries exceeded)
    """
    
    def __init__(self, max_retries: int = 3, state_file: Optional[str] = None, on_state_change=None):
        self.lock = threading.RLock()
        self.not_empty = threading.Condition(self.lock)
        self.max_retries = max_retries
        self.state_file = state_file
        self.on_state_change = on_state_change  # Callback for state changes
        
        # Single source of truth for all items
        self.items: Dict[str, Dict[str, Any]] = {}  # key -> item data
        
        # Queues for each state (store keys, not full items)
        self.queued_keys: deque = deque()
        self.processing_keys: set = set()
        self.retrying_keys: set = set()
        self.completed_keys: deque = deque(maxlen=50)  # Keep last 50
        self.failed_keys: deque = deque(maxlen=20)     # Keep last 20
        
        # Load persisted state if available
        if self.state_file:
            self._load_state()
        
    def _make_key(self, item: SubtitleTranslate) -> str:
        """Generate unique key for an item."""
        return f"{item.video_id}_{item.to_language}_{item.base_subtitle.code2}"
    
    def add_item(self, item: SubtitleTranslate, priority: bool = False) -> bool:
        """
        Add item to queue. Returns True if added, False if already exists.
        """
        key = self._make_key(item)
        
        with self.lock:
            # Don't re-add if already exists in any state
            if key in self.items:
                current_state = self.items[key]['state']
                print(f"[QueueManager] Item {key} already exists in state: {current_state}")
                # Allow re-queueing failed items
                if current_state == 'failed':
                    self._transition_state(key, 'queued', priority=priority)
                    # Reset retry count for manual retry
                    self.items[key]['retry_count'] = 0
                    self.items[key]['subtitle'] = item
                    return True
                return False
            
            # Create new item entry
            print(f"[QueueManager] Adding new item: {key}")
            self.items[key] = {
                'state': 'queued',
                'subtitle': item,
                'retry_count': 0,
                'queued_at': time.time(),
                'started_at': None,
                'completed_at': None,
                'key': key
            }
            
            if priority:
                self.queued_keys.appendleft(key)
            else:
                self.queued_keys.append(key)
            
            self.not_empty.notify()
            return True
    
    def get_next_item(self, timeout: Optional[float] = None) -> Optional[SubtitleTranslate]:
        """
        Get next item from queue, blocking until available.
        Moves item from queued -> processing state.
        """
        with self.not_empty:
            while not self.queued_keys:
                if timeout:
                    self.not_empty.wait(timeout=timeout)
                    if not self.queued_keys:
                        return None
                else:
                    self.not_empty.wait()
            
            key = self.queued_keys.popleft()
            
            if key not in self.items:
                return None
            
            # Transition to processing
            self.processing_keys.add(key)
            item_data = self.items[key]
            item_data['state'] = 'processing'
            item_data['started_at'] = time.time()
            
            return item_data['subtitle']
    
    def _transition_state(self, key: str, new_state: str, priority: bool = False):
        """Internal method to transition item between states."""
        if key not in self.items:
            return
        
        old_state = self.items[key]['state']
        
        # Remove from old state
        if old_state == 'queued':
            try:
                self.queued_keys.remove(key)
            except ValueError:
                pass
        elif old_state == 'processing':
            self.processing_keys.discard(key)
        elif old_state == 'retrying':
            self.retrying_keys.discard(key)
        elif old_state == 'completed':
            try:
                self.completed_keys.remove(key)
            except ValueError:
                pass
        elif old_state == 'failed':
            try:
                self.failed_keys.remove(key)
            except ValueError:
                pass
        
        # Add to new state
        if new_state == 'queued':
            if priority:
                self.queued_keys.appendleft(key)
            else:
                self.queued_keys.append(key)
            self.not_empty.notify()
        elif new_state == 'processing':
            self.processing_keys.add(key)
        elif new_state == 'retrying':
            self.retrying_keys.add(key)
        elif new_state == 'completed':
            self.completed_keys.appendleft(key)  # Most recent first
            self.items[key]['completed_at'] = time.time()
        elif new_state == 'failed':
            self.failed_keys.appendleft(key)  # Most recent first
            self.items[key]['completed_at'] = time.time()
            self._save_state()  # Persist failed items
        
        self.items[key]['state'] = new_state
        
        # Trigger callback for state changes
        if self.on_state_change:
            try:
                self.on_state_change()
            except Exception as e:
                pass  # Don't let callback errors break state management
    
    def mark_completed(self, item: SubtitleTranslate, success: bool = True):
        """
        Mark item as completed (success) or retrying/failed.
        """
        key = self._make_key(item)
        
        with self.lock:
            if key not in self.items:
                return
            
            item_data = self.items[key]
            
            if success:
                self._transition_state(key, 'completed')
            else:
                # Failed - check if should retry
                item_data['retry_count'] += 1
                
                if item_data['retry_count'] < self.max_retries:
                    # Re-queue with priority for retry
                    self._transition_state(key, 'queued', priority=True)
                    item_data['queued_at'] = time.time()
                    item_data['started_at'] = None
                else:
                    # Permanently failed
                    self._transition_state(key, 'failed')
    
    def move_to_top(self, queue_position: int) -> bool:
        """Move item at queue_position to front of queue."""
        with self.lock:
            if 0 <= queue_position < len(self.queued_keys):
                # Get the key at this position
                keys_list = list(self.queued_keys)
                key = keys_list[queue_position]
                
                # Remove and re-add at front
                self.queued_keys.remove(key)
                self.queued_keys.appendleft(key)
                self.not_empty.notify()
                
                # Trigger state change callback to update UI
                if self.on_state_change:
                    self.on_state_change()
                
                return True
            return False
    
    def get_all_items(self, is_serie: Optional[bool] = None) -> List[Dict[str, Any]]:
        """
        Get all items ordered by state: processing -> queued -> retrying -> completed -> failed.
        Optionally filter by is_serie flag.
        """
        with self.lock:
            result = []
            
            # Helper to add items from a collection
            def add_items_from_keys(keys, add_queue_pos=False):
                for idx, key in enumerate(keys):
                    if key in self.items:
                        item_data = self.items[key].copy()
                        
                        # Skip items with missing subtitle data
                        subtitle = item_data.get('subtitle')
                        if subtitle is None:
                            continue
                        
                        # Filter by type if specified
                        if is_serie is not None:
                            if subtitle.is_serie != is_serie:
                                continue
                        
                        if add_queue_pos:
                            item_data['queue_position'] = idx
                        
                        result.append(self._serialize_item(item_data))
            
            # Add in order
            add_items_from_keys(self.processing_keys)
            add_items_from_keys(self.queued_keys, add_queue_pos=True)
            add_items_from_keys(self.retrying_keys)
            add_items_from_keys(self.completed_keys)
            add_items_from_keys(self.failed_keys)
            
            return result
    
    def _serialize_item(self, item_data: Dict[str, Any]) -> Dict[str, Any]:
        """Convert internal item data to API-friendly format."""
        subtitle = item_data.get('subtitle')
        
        # Handle items loaded from persistence (no subtitle object)
        if subtitle is None:
            return {
                'key': item_data['key'],
                'item_key': item_data['key'],
                'state': item_data['state'],
                'video_title': item_data.get('title', 'Unknown'),
                'video_id': item_data.get('video_id', 0),
                'is_serie': item_data.get('is_serie', False),
                'from_language': item_data.get('from_language', '?'),
                'to_language': item_data.get('to_language', '?'),
                'retry_count': item_data['retry_count'],
                'queue_time': 0,
                'translation_time': 0,
                'total_time': 0,
                'queue_position': item_data.get('queue_position')
            }
        
        queue_time = 0
        translation_time = 0
        total_time = 0
        
        if item_data['started_at']:
            queue_time = item_data['started_at'] - item_data['queued_at']
        
        if item_data['completed_at'] and item_data['started_at']:
            translation_time = item_data['completed_at'] - item_data['started_at']
            total_time = item_data['completed_at'] - item_data['queued_at']
        
        return {
            'key': item_data['key'],
            'item_key': item_data['key'],  # Legacy compatibility
            'state': item_data['state'],
            'video_title': subtitle.video_title,
            'video_id': subtitle.video_id,
            'is_serie': subtitle.is_serie,
            'from_language': subtitle.base_subtitle.code2,
            'to_language': subtitle.to_language,
            'retry_count': item_data['retry_count'],
            'queue_time': queue_time,
            'translation_time': translation_time,
            'total_time': total_time,
            'queue_position': item_data.get('queue_position')
        }
    
    def get_stats(self) -> Dict[str, int]:
        """Get count of items in each state."""
        with self.lock:
            return {
                'queued': len(self.queued_keys),
                'processing': len(self.processing_keys),
                'retrying': len(self.retrying_keys),
                'completed': len(self.completed_keys),
                'failed': len(self.failed_keys)
            }
    
    def retry_failed_item(self, key: str) -> bool:
        """Manually retry a failed item."""
        with self.lock:
            if key not in self.items or self.items[key]['state'] != 'failed':
                return False
            
            # Reset and re-queue
            self._transition_state(key, 'queued', priority=True)
            self.items[key]['retry_count'] = 0
            self.items[key]['queued_at'] = time.time()
            self.items[key]['started_at'] = None
            self.items[key]['completed_at'] = None
            self._save_state()  # Persist change
            return True
    
    def _load_state(self):
        """Load persisted state from disk."""
        if not self.state_file or not os.path.exists(self.state_file):
            return
        
        try:
            with open(self.state_file, 'r') as f:
                data = json.load(f)
            
            # Only restore failed items (don't restore completed/processing)
            # Queued items will be re-added by scanner
            failed_items = data.get('failed_items', {})
            
            for key, item_info in failed_items.items():
                # Store minimal info - we can't restore SubtitleTranslate objects
                self.items[key] = {
                    'state': 'failed',
                    'subtitle': None,  # Will be populated if item is retried
                    'retry_count': item_info.get('retry_count', self.max_retries),
                    'queued_at': item_info.get('queued_at', 0),
                    'started_at': item_info.get('started_at'),
                    'completed_at': item_info.get('completed_at'),
                    'key': key,
                    # Store metadata for display
                    'video_title': item_info.get('video_title', item_info.get('title', 'Unknown')),
                    'video_id': item_info.get('video_id', 0),
                    'is_serie': item_info.get('is_serie', False),
                    'from_language': item_info.get('from_language', '?'),
                    'to_language': item_info.get('to_language', '?')
                }
                self.failed_keys.appendleft(key)
            
            if failed_items:
                print(f"QueueManager: Restored {len(failed_items)} failed items from {self.state_file}")
                
        except Exception as e:
            print(f"QueueManager: Failed to load state from {self.state_file}: {e}")
    
    def _save_state(self):
        """Save current state to disk."""
        if not self.state_file:
            return
        
        try:
            # Only persist failed items (completed items can be discarded on restart)
            failed_items = {}
            
            for key in self.failed_keys:
                if key in self.items:
                    item_data = self.items[key]
                    subtitle = item_data.get('subtitle')
                    
                    # Use stored metadata if subtitle object not available
                    if subtitle:
                        failed_items[key] = {
                            'retry_count': item_data['retry_count'],
                            'queued_at': item_data['queued_at'],
                            'started_at': item_data['started_at'],
                            'completed_at': item_data['completed_at'],
                            'video_title': subtitle.video_title,
                            'video_id': subtitle.video_id,
                            'is_serie': subtitle.is_serie,
                            'from_language': subtitle.base_subtitle.code2,
                            'to_language': subtitle.to_language
                        }
                    else:
                        # Use metadata already stored
                        failed_items[key] = {
                            'retry_count': item_data['retry_count'],
                            'queued_at': item_data['queued_at'],
                            'started_at': item_data['started_at'],
                            'completed_at': item_data['completed_at'],
                            'video_title': item_data.get('video_title', 'Unknown'),
                            'video_id': item_data.get('video_id', 0),
                            'is_serie': item_data.get('is_serie', False),
                            'from_language': item_data.get('from_language', '?'),
                            'to_language': item_data.get('to_language', '?')
                        }
            
            # Write atomically
            temp_file = self.state_file + '.tmp'
            with open(temp_file, 'w') as f:
                json.dump({'failed_items': failed_items}, f, indent=2)
            os.replace(temp_file, self.state_file)
            
        except Exception as e:
            print(f"QueueManager: Failed to save state to {self.state_file}: {e}")

