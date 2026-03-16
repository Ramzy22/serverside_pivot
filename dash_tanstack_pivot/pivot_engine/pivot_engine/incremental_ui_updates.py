"""
incremental_ui_updates.py - Client-side incremental UI updates for pivot tables
This would typically be JavaScript/TypeScript code, but for demonstration I'll create 
a Python equivalent showing the concepts
"""

class IncrementalPivotUI:
    """Simulates client-side incremental UI updates for pivot tables"""
    
    def __init__(self, container_id, pivot_engine):
        self.container_id = container_id
        self.pivot_engine = pivot_engine
        self.current_data = {}  # Path -> Data mapping
        self.rendered_nodes = {}  # For tracking DOM nodes
        self.row_cache = {}  # Cache for virtual scrolling
        
    async def update_node_incrementally(self, path, new_data):
        """Update only changed portions of the UI instead of full re-render"""
        path_key = str(path)
        
        # Find existing DOM node for this path
        existing_node = self.rendered_nodes.get(path_key)
        
        if existing_node:
            # Calculate differences between old and new data
            changes = self._calculate_differences(
                self.current_data.get(path_key, []), 
                new_data
            )
            
            # Apply minimal DOM updates
            self._apply_incremental_changes(existing_node, changes)
        else:
            # New node - render completely
            new_node = await self._create_node(path, new_data)
            await self._append_to_container(new_node)
            self.rendered_nodes[path_key] = new_node
        
        # Update data cache
        self.current_data[path_key] = new_data
    
    def _calculate_differences(self, old_data, new_data):
        """Calculate what actually changed between old and new data"""
        changes = {
            'added': [],
            'removed': [],
            'modified': [],
            'unchanged': []
        }
        
        old_dict = {self._get_row_key(row): row for row in old_data}
        new_dict = {self._get_row_key(row): row for row in new_data}
        
        for key, new_row in new_dict.items():
            if key not in old_dict:
                changes['added'].append({'key': key, 'data': new_row})
            elif not self._is_row_equal(old_dict[key], new_row):
                changes['modified'].append({
                    'key': key, 
                    'old': old_dict[key], 
                    'new': new_row
                })
            else:
                changes['unchanged'].append({'key': key, 'data': new_row})
        
        for key, old_row in old_dict.items():
            if key not in new_dict:
                changes['removed'].append({'key': key, 'data': old_row})
        
        return changes
    
    def _apply_incremental_changes(self, node, changes):
        """Apply only the actual changes to the DOM"""
        # Remove elements
        for change in changes['removed']:
            row_element = node.find(f"[data-row-key='{change['key']}']")
            if row_element:
                row_element.remove()
        
        # Add new elements
        for change in changes['added']:
            new_row_element = self._create_row_element(change['data'])
            node.appendChild(new_row_element)
        
        # Update modified elements
        for change in changes['modified']:
            row_element = node.find(f"[data-row-key='{change['key']}']")
            if row_element:
                self._update_row_element(row_element, change['new'])
            else:
                # If element was somehow missing, add it
                new_row_element = self._create_row_element(change['new'])
                node.appendChild(new_row_element)
    
    def _get_row_key(self, row):
        """Generate a unique key for a row (typically based on dimension values)"""
        if isinstance(row, dict):
            # Use the dimension values as key
            key_parts = []
            for k, v in row.items():
                if not k.startswith('_') and not k.endswith('_alias'):  # Skip computed fields
                    key_parts.append(f"{k}:{v}")
            return "|".join(key_parts)
        else:
            return str(row)
    
    def _is_row_equal(self, row1, row2):
        """Check if two rows are equal for update purposes"""
        if type(row1) != type(row2):
            return False
        
        if isinstance(row1, dict):
            # Compare non-computed fields
            for k, v in row1.items():
                if not k.startswith('_') and row2.get(k) != v:
                    return False
            for k, v in row2.items():
                if not k.startswith('_') and row1.get(k) != v:
                    return False
            return True
        
        return row1 == row2
    
    async def _create_node(self, path, data):
        """Create a UI node for a hierarchical path"""
        # This would create DOM elements in a real implementation
        node = {
            'path': path,
            'data': data,
            'element_id': f"pivot-node-{hash(str(path))}",
            'children': []
        }
        return node
    
    async def _append_to_container(self, node):
        """Append node to the UI container"""
        # This would manipulate the DOM in a real implementation
        print(f"Appending node {node['element_id']} to container {self.container_id}")
    
    def _create_row_element(self, row_data):
        """Create a row element for the UI"""
        # This would create a DOM element in a real implementation
        return {
            'data': row_data,
            'element_id': f"row-{hash(str(row_data))}",
            'html': self._render_row_html(row_data)
        }
    
    def _update_row_element(self, element, new_data):
        """Update an existing row element with new data"""
        element['data'] = new_data
        element['html'] = self._render_row_html(new_data)
    
    def _render_row_html(self, row_data):
        """Render HTML for a row (simplified)"""
        if isinstance(row_data, dict):
            cells = []
            for k, v in row_data.items():
                cells.append(f"<td data-field='{k}'>{v}</td>")
            return f"<tr>{''.join(cells)}</tr>"
        return str(row_data)


class RealTimePivotUpdates:
    """Simulates WebSocket-based real-time updates"""
    
    def __init__(self, websocket_connection):
        self.websocket = websocket_connection
        self.subscribers = {}  # subscription_id -> callback
        self.active_subscriptions = set()
        
    async def subscribe(self, path, callback, subscription_id=None):
        """Subscribe to real-time updates for a specific path"""
        if subscription_id is None:
            subscription_id = f"sub_{hash(str(path))}_{id(callback)}"
        
        self.subscribers[subscription_id] = {
            'callback': callback,
            'path': path,
            'filter': self._create_path_filter(path)
        }
        
        # Register with backend
        await self._send_subscription_message(subscription_id, path)
        self.active_subscriptions.add(subscription_id)
        
        return subscription_id
    
    async def _send_subscription_message(self, subscription_id, path):
        """Send subscription request to the backend"""
        message = {
            'type': 'subscribe',
            'subscription_id': subscription_id,
            'path': path,
            'timestamp': self._get_timestamp()
        }
        await self.websocket.send(message)
    
    async def unsubscribe(self, subscription_id):
        """Unsubscribe from updates"""
        if subscription_id in self.subscribers:
            del self.subscribers[subscription_id]
            self.active_subscriptions.discard(subscription_id)
            
            # Inform backend
            message = {
                'type': 'unsubscribe',
                'subscription_id': subscription_id,
                'timestamp': self._get_timestamp()
            }
            await self.websocket.send(message)
    
    async def handle_message(self, message):
        """Handle incoming WebSocket messages"""
        msg_type = message.get('type')
        
        if msg_type == 'data_update':
            await self._handle_data_update(message)
        elif msg_type == 'subscription_ack':
            await self._handle_subscription_ack(message)
        elif msg_type == 'error':
            await self._handle_error(message)
    
    async def _handle_data_update(self, message):
        """Handle data update messages"""
        subscription_id = message.get('subscription_id')
        data = message.get('data', {})
        changes = message.get('changes', {})
        
        if subscription_id in self.subscribers:
            callback = self.subscribers[subscription_id]['callback']
            try:
                await callback(data, changes, message.get('metadata', {}))
            except Exception as e:
                print(f"Error in subscription callback: {e}")
    
    async def _handle_subscription_ack(self, message):
        """Handle subscription acknowledgment"""
        subscription_id = message.get('subscription_id')
        status = message.get('status', 'unknown')
        
        print(f"Subscription {subscription_id} acknowledged with status: {status}")
    
    async def _handle_error(self, message):
        """Handle error messages"""
        error_code = message.get('error_code')
        error_msg = message.get('error_message', 'Unknown error')
        
        print(f"WebSocket error: {error_code} - {error_msg}")
    
    def _create_path_filter(self, path):
        """Create a filter function for the path"""
        def filter_func(data):
            # Check if data matches the subscription path
            if isinstance(data, dict):
                for i, path_segment in enumerate(path):
                    if i < len(data.get('path', [])) and data['path'][i] != path_segment:
                        return False
            return True
        return filter_func
    
    def _get_timestamp(self):
        """Get current timestamp"""
        import time
        return time.time()