import asyncio
import threading
from flask import Flask, render_template, jsonify, request
from flask_socketio import SocketIO, emit
from typing import List, Dict, Any
from collections import deque

# This will be set by main.py
app = Flask(__name__)
app.config['SECRET_KEY'] = 'bazarr-autotranslate-secret'
socketio = SocketIO(
    app, 
    cors_allowed_origins="*", 
    async_mode='threading',
    logger=False,
    engineio_logger=False,
    ping_timeout=60,
    ping_interval=25
)

app_state = {
    'dual_queue_mode': False,
    'queue_manager_series': None,  # QueueManager instance
    'queue_manager_movies': None,  # QueueManager instance
    'queue_manager': None,  # QueueManager instance for combined mode
    'trigger_scan': None,
    'trigger_scan_series': None,
    'trigger_scan_movies': None,
    'log_buffer': deque(maxlen=100),
}

class WebLogHandler:
    """Custom log handler that captures logs for web display and broadcasts immediately."""
    def write(self, message):
        if message.strip():
            app_state['log_buffer'].append(message.strip())
            # Broadcast immediately via WebSocket
            try:
                socketio.emit('log_update', {
                    'logs': list(app_state['log_buffer'])
                })
            except:
                pass  # Ignore errors if WebSocket not ready
    
    def flush(self):
        pass

def init_web_interface(dual_queue_mode, queue_manager, queue_manager_series, queue_manager_movies, 
                       trigger_scan_fn, trigger_scan_series_fn, trigger_scan_movies_fn):
    """Initialize the web interface with necessary state."""
    app_state['dual_queue_mode'] = dual_queue_mode
    app_state['queue_manager'] = queue_manager
    app_state['queue_manager_series'] = queue_manager_series
    app_state['queue_manager_movies'] = queue_manager_movies
    app_state['trigger_scan'] = trigger_scan_fn
    app_state['trigger_scan_series'] = trigger_scan_series_fn
    app_state['trigger_scan_movies'] = trigger_scan_movies_fn

def update_item_state(item_key, state, item_data):
    """Legacy function - no longer needed with QueueManager."""
    pass

def get_all_items() -> List[Dict[str, Any]]:
    """Get all tracked items - now delegates to QueueManager."""
    if app_state['dual_queue_mode']:
        # This shouldn't be called in dual mode, but return empty for safety
        return []
    else:
        qm = app_state['queue_manager']
        if qm:
            return qm.get_all_items()
        return []

def add_processed_item(item_data):
    """Legacy function - no longer needed with QueueManager."""
    pass

def add_failed_item(item_key, subtitle_data):
    """Legacy function - no longer needed with QueueManager."""
    pass

def get_failed_items():
    """Legacy function - returns empty dict."""
    return {}

@app.route('/')
def index():
    """Main page showing queues."""
    return render_template('index.html', dual_queue_mode=app_state['dual_queue_mode'])

@app.route('/test')
def test():
    """Test route to verify app is running."""
    return jsonify({
        'status': 'ok',
        'socketio_enabled': True,
        'dual_queue_mode': app_state['dual_queue_mode']
    })

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
    queue_position = data.get('index')
    
    if queue_position is None:
        return jsonify({'status': 'error', 'message': 'Index required'}), 400
    
    try:
        # Get the appropriate queue manager
        if app_state['dual_queue_mode']:
            if queue_type == 'series':
                qm = app_state['queue_manager_series']
            elif queue_type == 'movies':
                qm = app_state['queue_manager_movies']
            else:
                return jsonify({'status': 'error', 'message': 'Invalid queue type'}), 400
        else:
            qm = app_state['queue_manager']
        
        if qm is None:
            return jsonify({'status': 'error', 'message': 'Queue not available'}), 500
        
        # Move item to top
        success = qm.move_to_top(queue_position)
        
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
    
    try:
        # Determine which queue manager has this item
        success = False
        
        if app_state['dual_queue_mode']:
            series_qm = app_state['queue_manager_series']
            movies_qm = app_state['queue_manager_movies']
            
            if series_qm and series_qm.retry_failed_item(item_key):
                success = True
            elif movies_qm and movies_qm.retry_failed_item(item_key):
                success = True
        else:
            qm = app_state['queue_manager']
            if qm:
                success = qm.retry_failed_item(item_key)
        
        if success:
            return jsonify({'status': 'success', 'message': 'Item queued for retry'})
        else:
            return jsonify({'status': 'error', 'message': 'Item not found or not failed'}), 404
            
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/api/items')
def get_items():
    """API endpoint to get all tracked items."""
    if app_state['dual_queue_mode']:
        series_qm = app_state['queue_manager_series']
        movies_qm = app_state['queue_manager_movies']
        
        series_items = series_qm.get_all_items(is_serie=True) if series_qm else []
        movies_items = movies_qm.get_all_items(is_serie=False) if movies_qm else []
        
        return jsonify({
            'dual_queue_mode': True,
            'series': series_items,
            'movies': movies_items
        })
    else:
        qm = app_state['queue_manager']
        items = qm.get_all_items() if qm else []
        
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
    """Run the Flask web interface with SocketIO."""
    print(f"[WebSocket] Starting SocketIO server on {host}:{port}")
    print(f"[WebSocket] Dual queue mode: {app_state['dual_queue_mode']}")
    socketio.run(app, host=host, port=port, debug=False, use_reloader=False, allow_unsafe_werkzeug=True)

def broadcast_update():
    """Broadcast queue update to all connected clients."""
    try:
        if app_state['dual_queue_mode']:
            series_qm = app_state['queue_manager_series']
            movies_qm = app_state['queue_manager_movies']
            
            series_items = series_qm.get_all_items(is_serie=True) if series_qm else []
            movies_items = movies_qm.get_all_items(is_serie=False) if movies_qm else []
            
            print(f"[WebSocket] Broadcasting update: {len(series_items)} series, {len(movies_items)} movies")
            socketio.emit('queue_update', {
                'dual_queue_mode': True,
                'series': series_items,
                'movies': movies_items
            })
        else:
            qm = app_state['queue_manager']
            items = qm.get_all_items() if qm else []
            
            print(f"[WebSocket] Broadcasting update: {len(items)} items")
            socketio.emit('queue_update', {
                'dual_queue_mode': False,
                'items': items
            })
    except Exception as e:
        print(f"[WebSocket] Error broadcasting update: {e}")
        import traceback
        traceback.print_exc()

def broadcast_logs():
    """Broadcast logs to all connected clients."""
    try:
        socketio.emit('log_update', {
            'logs': list(app_state['log_buffer'])
        })
    except Exception as e:
        print(f"[WebSocket] Error broadcasting logs: {e}")

@socketio.on('connect')
def handle_connect(auth=None):
    """Handle client connection - send initial data."""
    print("[WebSocket] Client connected")
    try:
        # Send initial queue state
        if app_state['dual_queue_mode']:
            series_qm = app_state['queue_manager_series']
            movies_qm = app_state['queue_manager_movies']
            
            series_items = series_qm.get_all_items(is_serie=True) if series_qm else []
            movies_items = movies_qm.get_all_items(is_serie=False) if movies_qm else []
            
            print(f"[WebSocket] Sending dual queue data: {len(series_items)} series, {len(movies_items)} movies")
            emit('queue_update', {
                'dual_queue_mode': True,
                'series': series_items,
                'movies': movies_items
            })
        else:
            qm = app_state['queue_manager']
            items = qm.get_all_items() if qm else []
            
            print(f"[WebSocket] Sending combined queue data: {len(items)} items")
            emit('queue_update', {
                'dual_queue_mode': False,
                'items': items
            })
        
        # Send initial logs
        emit('log_update', {
            'logs': list(app_state['log_buffer'])
        })
    except Exception as e:
        print(f"[WebSocket] Error in handle_connect: {e}")
        import traceback
        traceback.print_exc()

@socketio.on('disconnect')
def handle_disconnect():
    """Handle client disconnection."""
    print("[WebSocket] Client disconnected")
