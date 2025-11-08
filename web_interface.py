import asyncio
import threading
from flask import Flask, render_template, jsonify, request
from typing import List, Dict, Any
from collections import deque

# This will be set by main.py
app = Flask(__name__)
app_state = {
    'dual_queue_mode': False,
    'task_queue': None,
    'series_queue': None,
    'movies_queue': None,
    'trigger_scan': None,  # Function to trigger scan
    'trigger_scan_series': None,  # Function to trigger series scan
    'trigger_scan_movies': None,  # Function to trigger movies scan
    'seen_items': None,
    'seen_items_lock': None,
    'log_buffer': deque(maxlen=100),  # Store last 100 log messages
    'all_items': {},  # Track all items by key with their current state
    'items_order': deque(maxlen=50),  # Track order of items (most recent first)
    'failed_items': {}  # Store failed items by unique key for retry capability
}

class WebLogHandler:
    """Custom log handler that captures logs for web display."""
    def write(self, message):
        if message.strip():
            app_state['log_buffer'].append(message.strip())
    
    def flush(self):
        pass

def init_web_interface(dual_queue_mode, task_queue, series_queue, movies_queue, trigger_scan_fn, trigger_scan_series_fn, trigger_scan_movies_fn, seen_items, seen_items_lock):
    """Initialize the web interface with necessary state."""
    app_state['dual_queue_mode'] = dual_queue_mode
    app_state['task_queue'] = task_queue
    app_state['series_queue'] = series_queue
    app_state['movies_queue'] = movies_queue
    app_state['trigger_scan'] = trigger_scan_fn
    app_state['trigger_scan_series'] = trigger_scan_series_fn
    app_state['trigger_scan_movies'] = trigger_scan_movies_fn
    app_state['seen_items'] = seen_items
    app_state['seen_items_lock'] = seen_items_lock

def update_item_state(item_key, state, item_data):
    """Update or create an item with the given state.
    
    States: 'queued', 'processing', 'completed', 'retrying', 'failed'
    """
    import time
    item_data['state'] = state
    item_data['item_key'] = item_key
    item_data['updated_at'] = time.time()  # Always update timestamp
    
    # If item exists, remove it from order to re-add at front (most recent first)
    if item_key in app_state['all_items']:
        try:
            app_state['items_order'].remove(item_key)
        except ValueError:
            pass  # Item not in order list
    
    # Add to front of order
    app_state['items_order'].appendleft(item_key)
    app_state['all_items'][item_key] = item_data

def get_all_items() -> List[Dict[str, Any]]:
    """Get all tracked items in order: processing → queued → completed → failed."""
    items_by_state = {
        'processing': [],
        'queued': [],
        'retrying': [],
        'completed': [],
        'failed': []
    }
    
    # Categorize items by state
    for item_key in app_state['items_order']:
        if item_key in app_state['all_items']:
            item = app_state['all_items'][item_key]
            state = item.get('state', 'unknown')
            if state in items_by_state:
                items_by_state[state].append(item)
    
    # Add queue position to queued items
    for idx, item in enumerate(items_by_state['queued']):
        item['queue_position'] = idx
    
    # Build ordered result: processing → retrying → queued → completed → failed
    result = []
    result.extend(items_by_state['processing'])  # Currently processing (order doesn't matter, should be few)
    result.extend(items_by_state['retrying'])     # Retrying items
    result.extend(items_by_state['queued'])       # Queued items (in queue order)
    result.extend(items_by_state['completed'])    # Completed (newest first)
    result.extend(items_by_state['failed'])       # Failed items at bottom
    
    return result

def get_queue_items(queue, limit=100) -> List[Dict[str, Any]]:
    """Get items from a queue and convert to dict format."""
    if queue is None:
        return []
    
    items = queue.peek(limit)
    result = []
    for idx, item in enumerate(items):
        result.append({
            'index': idx,
            'title': item.video_title,
            'video_id': item.video_id,
            'is_serie': item.is_serie,
            'from_language': item.base_subtitle.code2,
            'to_language': item.to_language,
            'path': item.base_subtitle.path,
            'retry_count': item.retry_count
        })
    return result

@app.route('/')
def index():
    """Main page showing queues."""
    return render_template('index.html', dual_queue_mode=app_state['dual_queue_mode'])

@app.route('/api/queues')
def get_queues():
    """API endpoint to get current queue status."""
    if app_state['dual_queue_mode']:
        series_items = get_queue_items(app_state['series_queue'])
        movies_items = get_queue_items(app_state['movies_queue'])
        
        return jsonify({
            'dual_queue_mode': True,
            'series': {
                'size': app_state['series_queue'].qsize() if app_state['series_queue'] else 0,
                'items': series_items
            },
            'movies': {
                'size': app_state['movies_queue'].qsize() if app_state['movies_queue'] else 0,
                'items': movies_items
            }
        })
    else:
        items = get_queue_items(app_state['task_queue'])
        
        return jsonify({
            'dual_queue_mode': False,
            'combined': {
                'size': app_state['task_queue'].qsize() if app_state['task_queue'] else 0,
                'items': items
            }
        })

@app.route('/api/scan', methods=['POST'])
def trigger_scan():
    """Trigger a manual scan."""
    data = request.json or {}
    scan_type = data.get('type', 'all')  # 'all', 'series', or 'movies'
    
    if scan_type == 'series':
        if app_state['trigger_scan_series']:
            app_state['trigger_scan_series']()
            return jsonify({'status': 'success', 'message': 'Series scan triggered'})
        else:
            return jsonify({'status': 'error', 'message': 'Series scan function not available'}), 500
    elif scan_type == 'movies':
        if app_state['trigger_scan_movies']:
            app_state['trigger_scan_movies']()
            return jsonify({'status': 'success', 'message': 'Movies scan triggered'})
        else:
            return jsonify({'status': 'error', 'message': 'Movies scan function not available'}), 500
    else:  # 'all' or default
        if app_state['trigger_scan']:
            app_state['trigger_scan']()
            return jsonify({'status': 'success', 'message': 'Scan triggered'})
        else:
            return jsonify({'status': 'error', 'message': 'Scan function not available'}), 500

@app.route('/api/move-to-top', methods=['POST'])
def move_to_top():
    """Move an item to the top of the queue."""
    data = request.json
    queue_type = data.get('queue_type')  # 'series', 'movies', or 'combined'
    index = data.get('index')
    
    if index is None:
        return jsonify({'status': 'error', 'message': 'Index required'}), 400
    
    try:
        # Get the appropriate queue
        if app_state['dual_queue_mode']:
            if queue_type == 'series':
                queue = app_state['series_queue']
            elif queue_type == 'movies':
                queue = app_state['movies_queue']
            else:
                return jsonify({'status': 'error', 'message': 'Invalid queue type'}), 400
        else:
            queue = app_state['task_queue']
        
        if queue is None:
            return jsonify({'status': 'error', 'message': 'Queue not available'}), 500
        
        # Move item to top
        success = queue.move_to_top(index)
        
        if success:
            return jsonify({'status': 'success', 'message': 'Item moved to top'})
        else:
            return jsonify({'status': 'error', 'message': 'Failed to move item'}), 400
            
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/api/logs')
def get_logs():
    """API endpoint to get recent log messages."""
    return jsonify({
        'logs': list(app_state['log_buffer'])
    })

@app.route('/api/retry', methods=['POST'])
def retry_failed_item():
    """Retry a failed translation item."""
    data = request.json
    item_key = data.get('item_key')
    
    if not item_key:
        return jsonify({'status': 'error', 'message': 'Item key required'}), 400
    
    # Get the failed item
    failed_item = app_state['failed_items'].get(item_key)
    if not failed_item:
        return jsonify({'status': 'error', 'message': 'Failed item not found'}), 404
    
    try:
        # Reset retry count and re-queue
        failed_item.retry_count = 0
        failed_item.queued_at = 0.0
        failed_item.started_at = 0.0
        failed_item.completed_at = 0.0
        
        # Add to appropriate queue with priority
        if app_state['dual_queue_mode']:
            target_queue = app_state['series_queue'] if failed_item.is_serie else app_state['movies_queue']
        else:
            target_queue = app_state['task_queue']
        
        if target_queue:
            target_queue.put(failed_item, priority=True)
            # Remove from failed items
            del app_state['failed_items'][item_key]
            return jsonify({'status': 'success', 'message': 'Item queued for retry'})
        else:
            return jsonify({'status': 'error', 'message': 'Queue not available'}), 500
            
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/api/items')
def get_items():
    """API endpoint to get all tracked items."""
    items = get_all_items()
    
    if app_state['dual_queue_mode']:
        series_items = [item for item in items if item.get('is_serie')]
        movies_items = [item for item in items if not item.get('is_serie')]
        return jsonify({
            'dual_queue_mode': True,
            'series': series_items,
            'movies': movies_items
        })
    else:
        return jsonify({
            'dual_queue_mode': False,
            'items': items
        })

def add_processed_item(item_data):
    """Add an item to the processed items list (legacy - now updates state)."""
    item_key = item_data.get('item_key')
    if not item_key:
        return
    
    # Determine state based on item_data
    if item_data.get('failed'):
        state = 'failed'
    elif item_data.get('retrying'):
        state = 'retrying'
    else:
        state = 'completed'
    
    update_item_state(item_key, state, item_data)

def add_failed_item(item_key, subtitle_data):
    """Store a failed item for potential retry."""
    app_state['failed_items'][item_key] = subtitle_data

def get_failed_items():
    """Get the dictionary of failed items."""
    return app_state['failed_items']

def run_web_interface(host='0.0.0.0', port=5000):
    """Run the Flask web interface."""
    app.run(host=host, port=port, debug=False, use_reloader=False)
