"""
REST API for Distributed Cognitive Mesh

This module provides REST API endpoints for interacting with the distributed
cognitive mesh, allowing external systems to submit tasks, monitor status,
and manage nodes.
"""

from flask import Flask, request, jsonify, Response
from flask_socketio import SocketIO, emit, join_room, leave_room
import json
import logging
from typing import Dict, Any, Optional
import asyncio
from functools import wraps
import time
import threading

from .orchestrator import mesh_orchestrator, MeshNode, MeshNodeType, DistributedTask, TaskStatus
from ..core import cognitive_core
from ..ecan_attention.attention_kernel import ecan_system
from ..scheme_adapters.grammar_adapter import scheme_adapter

logger = logging.getLogger(__name__)

# Flask app for REST API
cognitive_api = Flask(__name__)
cognitive_api.config['SECRET_KEY'] = 'cognitive_mesh_secret_key'
socketio = SocketIO(cognitive_api, cors_allowed_origins="*")


def async_route(f):
    """Decorator to handle async functions in Flask routes"""
    @wraps(f)
    def wrapper(*args, **kwargs):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            return loop.run_until_complete(f(*args, **kwargs))
        finally:
            loop.close()
    return wrapper


# REST API Endpoints

@cognitive_api.route('/api/v1/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({
        'status': 'healthy',
        'timestamp': time.time(),
        'version': '1.0.0',
        'components': {
            'mesh_orchestrator': mesh_orchestrator.is_running,
            'cognitive_core': cognitive_core.is_running,
            'ecan_system': len(ecan_system.element_attention) > 0,
            'scheme_adapter': len(scheme_adapter.patterns) > 0
        }
    })


@cognitive_api.route('/api/v1/mesh/status', methods=['GET'])
def get_mesh_status():
    """Get comprehensive mesh status"""
    return jsonify(mesh_orchestrator.get_mesh_status())


@cognitive_api.route('/api/v1/mesh/nodes', methods=['GET'])
def get_mesh_nodes():
    """Get all mesh nodes"""
    nodes = {node_id: node.to_dict() for node_id, node in mesh_orchestrator.nodes.items()}
    return jsonify({
        'nodes': nodes,
        'count': len(nodes),
        'timestamp': time.time()
    })


@cognitive_api.route('/api/v1/mesh/nodes/<node_id>', methods=['GET'])
def get_mesh_node(node_id: str):
    """Get specific mesh node"""
    if node_id not in mesh_orchestrator.nodes:
        return jsonify({'error': 'Node not found'}), 404
    
    node = mesh_orchestrator.nodes[node_id]
    performance = mesh_orchestrator.get_node_performance(node_id)
    
    return jsonify({
        'node': node.to_dict(),
        'performance': performance,
        'timestamp': time.time()
    })


@cognitive_api.route('/api/v1/mesh/nodes', methods=['POST'])
def register_mesh_node():
    """Register a new mesh node"""
    try:
        data = request.get_json()
        
        # Create node from request data
        node = MeshNode(
            node_type=MeshNodeType(data.get('node_type', 'agent')),
            capabilities=set(data.get('capabilities', [])),
            max_load=data.get('max_load', 1.0),
            metadata=data.get('metadata', {})
        )
        
        node_id = mesh_orchestrator.register_node(node)
        
        return jsonify({
            'node_id': node_id,
            'node': node.to_dict(),
            'timestamp': time.time()
        }), 201
        
    except Exception as e:
        logger.error("Error occurred while registering mesh node: %s", str(e), exc_info=True)
        return jsonify({'error': 'An internal error occurred while processing your request.'}), 400


@cognitive_api.route('/api/v1/mesh/nodes/<node_id>', methods=['DELETE'])
def unregister_mesh_node(node_id: str):
    """Unregister a mesh node"""
    if mesh_orchestrator.unregister_node(node_id):
        return jsonify({'message': f'Node {node_id} unregistered successfully'})
    else:
        return jsonify({'error': 'Node not found'}), 404


@cognitive_api.route('/api/v1/mesh/tasks', methods=['GET'])
def get_mesh_tasks():
    """Get all mesh tasks"""
    tasks = {task_id: task.to_dict() for task_id, task in mesh_orchestrator.tasks.items()}
    return jsonify({
        'tasks': tasks,
        'count': len(tasks),
        'pending': len(mesh_orchestrator.task_queue),
        'completed': len(mesh_orchestrator.completed_tasks),
        'timestamp': time.time()
    })


@cognitive_api.route('/api/v1/mesh/tasks', methods=['POST'])
def submit_mesh_task():
    """Submit a new task to the mesh"""
    try:
        data = request.get_json()
        
        # Create task from request data
        task = DistributedTask(
            task_type=data.get('task_type', 'general'),
            payload=data.get('payload', {}),
            priority=data.get('priority', 5),
            timeout=data.get('timeout', 300.0),
            requester_node=data.get('requester_node')
        )
        
        task_id = mesh_orchestrator.submit_task(task)
        
        return jsonify({
            'task_id': task_id,
            'task': task.to_dict(),
            'timestamp': time.time()
        }), 201
        
    except Exception as e:
        logger.error("Error occurred while submitting mesh task: %s", str(e), exc_info=True)
        return jsonify({'error': 'An internal error occurred while processing your request.'}), 400


@cognitive_api.route('/api/v1/mesh/tasks/<task_id>', methods=['GET'])
def get_mesh_task(task_id: str):
    """Get specific mesh task"""
    task_status = mesh_orchestrator.get_task_status(task_id)
    
    if task_status is None:
        return jsonify({'error': 'Task not found'}), 404
    
    return jsonify({
        'task': task_status,
        'timestamp': time.time()
    })


@cognitive_api.route('/api/v1/mesh/tasks/<task_id>', methods=['DELETE'])
def cancel_mesh_task(task_id: str):
    """Cancel a mesh task"""
    if mesh_orchestrator.cancel_task(task_id):
        return jsonify({'message': f'Task {task_id} cancelled successfully'})
    else:
        return jsonify({'error': 'Task not found or cannot be cancelled'}), 404


@cognitive_api.route('/api/v1/cognitive/agents', methods=['GET'])
def get_cognitive_agents():
    """Get all cognitive agents"""
    agents = {agent_id: agent.to_dict() for agent_id, agent in cognitive_core.agents.items()}
    return jsonify({
        'agents': agents,
        'count': len(agents),
        'timestamp': time.time()
    })


@cognitive_api.route('/api/v1/cognitive/agents', methods=['POST'])
def create_cognitive_agent():
    """Create a new cognitive agent"""
    try:
        from ..core import CognitiveAgent, TensorShape
        
        data = request.get_json()
        
        # Create tensor shape from request data
        tensor_shape = TensorShape(
            modality=data.get('modality', 512),
            depth=data.get('depth', 64),
            context=data.get('context', 2048),
            salience=data.get('salience', 128),
            autonomy_index=data.get('autonomy_index', 32)
        )
        
        # Create agent
        agent = CognitiveAgent(tensor_shape=tensor_shape)
        agent_id = cognitive_core.register_agent(agent)
        
        return jsonify({
            'agent_id': agent_id,
            'agent': agent.to_dict(),
            'timestamp': time.time()
        }), 201
        
    except Exception as e:
        logger.error("An error occurred while creating a cognitive agent: %s", str(e), exc_info=True)
        return jsonify({'error': 'An internal error occurred while processing your request.'}), 400


@cognitive_api.route('/api/v1/cognitive/agents/<agent_id>', methods=['GET'])
def get_cognitive_agent(agent_id: str):
    """Get specific cognitive agent"""
    agent_state = cognitive_core.get_agent_state(agent_id)
    
    if agent_state is None:
        return jsonify({'error': 'Agent not found'}), 404
    
    return jsonify({
        'agent': agent_state,
        'timestamp': time.time()
    })


@cognitive_api.route('/api/v1/cognitive/hypergraph', methods=['GET'])
def get_hypergraph():
    """Get global hypergraph representation"""
    return jsonify(cognitive_core.get_global_hypergraph())


@cognitive_api.route('/api/v1/attention/status', methods=['GET'])
def get_attention_status():
    """Get ECAN attention system status"""
    return jsonify(ecan_system.get_attention_statistics())


@cognitive_api.route('/api/v1/attention/elements', methods=['POST'])
def register_attention_element():
    """Register a new cognitive element for attention tracking"""
    try:
        from ..ecan_attention.attention_kernel import AttentionValue
        
        data = request.get_json()
        element_id = data.get('element_id')
        
        if not element_id:
            return jsonify({'error': 'element_id is required'}), 400
        
        # Create attention value from request data
        attention_value = AttentionValue(
            sti=data.get('sti', 0.0),
            lti=data.get('lti', 0.0),
            vlti=data.get('vlti', 0.0),
            urgency=data.get('urgency', 0.0),
            novelty=data.get('novelty', 0.0),
            confidence=data.get('confidence', 0.0)
        )
        
        ecan_system.register_cognitive_element(element_id, attention_value)
        
        return jsonify({
            'element_id': element_id,
            'attention_value': attention_value.to_dict(),
            'timestamp': time.time()
        }), 201
        
    except Exception as e:
        return jsonify({'error': str(e)}), 400


@cognitive_api.route('/api/v1/attention/elements/<element_id>/urgency', methods=['PUT'])
def update_urgency(element_id: str):
    """Update urgency for a cognitive element"""
    try:
        data = request.get_json()
        urgency = data.get('urgency', 0.0)
        
        ecan_system.update_urgency(element_id, urgency)
        
        return jsonify({
            'element_id': element_id,
            'urgency': urgency,
            'timestamp': time.time()
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 400


@cognitive_api.route('/api/v1/attention/elements/<element_id>/novelty', methods=['PUT'])
def update_novelty(element_id: str):
    """Update novelty for a cognitive element"""
    try:
        data = request.get_json()
        novelty = data.get('novelty', 0.0)
        
        ecan_system.update_novelty_detection(element_id, novelty)
        
        return jsonify({
            'element_id': element_id,
            'novelty': novelty,
            'timestamp': time.time()
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 400


@cognitive_api.route('/api/v1/scheme/patterns', methods=['GET'])
def get_scheme_patterns():
    """Get all registered Scheme patterns"""
    return jsonify(scheme_adapter.get_pattern_statistics())


@cognitive_api.route('/api/v1/scheme/patterns', methods=['POST'])
def register_scheme_pattern():
    """Register a new Scheme pattern"""
    try:
        data = request.get_json()
        
        name = data.get('name')
        pattern = data.get('pattern')
        confidence = data.get('confidence', 1.0)
        
        if not name or not pattern:
            return jsonify({'error': 'name and pattern are required'}), 400
        
        pattern_id = scheme_adapter.register_pattern(name, pattern, confidence)
        
        return jsonify({
            'pattern_id': pattern_id,
            'name': name,
            'pattern': pattern,
            'confidence': confidence,
            'timestamp': time.time()
        }), 201
        
    except Exception as e:
        return jsonify({'error': str(e)}), 400


@cognitive_api.route('/api/v1/scheme/translate/kobold-to-atomspace', methods=['POST'])
def translate_kobold_to_atomspace():
    """Translate KoboldAI text to AtomSpace patterns"""
    try:
        data = request.get_json()
        text = data.get('text', '')
        
        if not text:
            return jsonify({'error': 'text is required'}), 400
        
        atomspace_patterns = scheme_adapter.translate_kobold_to_atomspace(text)
        
        return jsonify({
            'original_text': text,
            'atomspace_patterns': atomspace_patterns,
            'pattern_count': len(atomspace_patterns),
            'timestamp': time.time()
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 400


@cognitive_api.route('/api/v1/scheme/translate/atomspace-to-kobold', methods=['POST'])
def translate_atomspace_to_kobold():
    """Translate AtomSpace patterns to KoboldAI text"""
    try:
        data = request.get_json()
        patterns = data.get('patterns', [])
        
        if not patterns:
            return jsonify({'error': 'patterns are required'}), 400
        
        translated_text = scheme_adapter.translate_atomspace_to_kobold(patterns)
        
        return jsonify({
            'atomspace_patterns': patterns,
            'translated_text': translated_text,
            'pattern_count': len(patterns),
            'timestamp': time.time()
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 400


@cognitive_api.route('/api/v1/scheme/translate/batch', methods=['POST'])
@async_route
async def batch_translate():
    """Process a batch of translations"""
    try:
        data = request.get_json()
        texts = data.get('texts', [])
        
        if not texts:
            return jsonify({'error': 'texts are required'}), 400
        
        results = await scheme_adapter.process_translation_batch(texts)
        
        return jsonify({
            'results': results,
            'batch_size': len(texts),
            'timestamp': time.time()
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 400


# WebSocket Events

@socketio.on('connect')
def handle_connect():
    """Handle WebSocket connection"""
    logger.info(f"Client connected: {request.sid}")
    emit('connected', {'message': 'Connected to cognitive mesh'})


@socketio.on('disconnect')
def handle_disconnect():
    """Handle WebSocket disconnection"""
    logger.info(f"Client disconnected: {request.sid}")


@socketio.on('join_mesh')
def handle_join_mesh(data):
    """Handle node joining mesh via WebSocket"""
    try:
        node_id = data.get('node_id')
        node_type = data.get('node_type', 'agent')
        capabilities = data.get('capabilities', [])
        
        if not node_id:
            emit('error', {'message': 'node_id is required'})
            return
        
        # Update WebSocket connection for existing node or create new one
        if node_id in mesh_orchestrator.nodes:
            node = mesh_orchestrator.nodes[node_id]
            node.update_heartbeat()
        else:
            node = MeshNode(
                node_id=node_id,
                node_type=MeshNodeType(node_type),
                capabilities=set(capabilities)
            )
            mesh_orchestrator.register_node(node)
        
        # Store WebSocket connection
        mesh_orchestrator.websocket_connections[node_id] = request.sid
        
        # Join room for node-specific messages
        join_room(node_id)
        
        emit('mesh_joined', {
            'node_id': node_id,
            'mesh_status': mesh_orchestrator.get_mesh_status()
        })
        
    except Exception as e:
        emit('error', {'message': str(e)})


@socketio.on('leave_mesh')
def handle_leave_mesh(data):
    """Handle node leaving mesh via WebSocket"""
    try:
        node_id = data.get('node_id')
        
        if node_id:
            mesh_orchestrator.unregister_node(node_id)
            
            if node_id in mesh_orchestrator.websocket_connections:
                del mesh_orchestrator.websocket_connections[node_id]
            
            leave_room(node_id)
            
            emit('mesh_left', {'node_id': node_id})
        
    except Exception as e:
        emit('error', {'message': str(e)})


@socketio.on('heartbeat')
def handle_heartbeat(data):
    """Handle node heartbeat via WebSocket"""
    try:
        node_id = data.get('node_id')
        
        if node_id in mesh_orchestrator.nodes:
            node = mesh_orchestrator.nodes[node_id]
            node.update_heartbeat()
            
            # Update load if provided
            if 'current_load' in data:
                node.current_load = data['current_load']
            
            emit('heartbeat_ack', {'node_id': node_id, 'timestamp': time.time()})
        
    except Exception as e:
        emit('error', {'message': str(e)})


@socketio.on('task_completed')
def handle_task_completed(data):
    """Handle task completion via WebSocket"""
    try:
        task_id = data.get('task_id')
        result = data.get('result', {})
        node_id = data.get('node_id')
        
        if task_id and node_id:
            mesh_orchestrator.handle_task_completion(task_id, result, node_id)
            emit('task_completion_ack', {'task_id': task_id})
        
    except Exception as e:
        emit('error', {'message': str(e)})


@socketio.on('task_failed')
def handle_task_failed(data):
    """Handle task failure via WebSocket"""
    try:
        task_id = data.get('task_id')
        error = data.get('error', 'Unknown error')
        node_id = data.get('node_id')
        
        if task_id and node_id:
            mesh_orchestrator.handle_task_failure(task_id, error, node_id)
            emit('task_failure_ack', {'task_id': task_id})
        
    except Exception as e:
        emit('error', {'message': str(e)})


@socketio.on('get_mesh_status')
def handle_get_mesh_status():
    """Handle mesh status request via WebSocket"""
    try:
        status = mesh_orchestrator.get_mesh_status()
        emit('mesh_status', status)
    except Exception as e:
        emit('error', {'message': str(e)})


def start_cognitive_api_server(host='0.0.0.0', port=5001, debug=False):
    """Start the cognitive API server"""
    logger.info(f"Starting Cognitive API server on {host}:{port}")
    socketio.run(cognitive_api, host=host, port=port, debug=debug, allow_unsafe_werkzeug=True)


if __name__ == '__main__':
    start_cognitive_api_server(debug=True)