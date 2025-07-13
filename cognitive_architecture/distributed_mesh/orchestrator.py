"""
Enhanced Distributed Cognitive Mesh Orchestrator

This module implements advanced distributed agent orchestration with discovery,
fault tolerance, auto-recovery, and comprehensive load testing capabilities.
"""

import asyncio
import json
import logging
from typing import Dict, Any, List, Optional, Set, Callable
from dataclasses import dataclass, field, asdict
from enum import Enum
import time
import uuid
import websockets
import threading
from concurrent.futures import ThreadPoolExecutor
import weakref

logger = logging.getLogger(__name__)


class MeshNodeType(Enum):
    """Types of nodes in the cognitive mesh"""
    AGENT = "agent"
    PROCESSOR = "processor"
    COORDINATOR = "coordinator"
    OBSERVER = "observer"
    GATEWAY = "gateway"


class TaskStatus(Enum):
    """Status of distributed tasks"""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass
class MeshNode:
    """Represents a node in the distributed cognitive mesh"""
    node_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    node_type: MeshNodeType = MeshNodeType.AGENT
    capabilities: Set[str] = field(default_factory=set)
    current_load: float = 0.0
    max_load: float = 1.0
    status: str = "online"
    metadata: Dict[str, Any] = field(default_factory=dict)
    created_at: float = field(default_factory=time.time)
    last_heartbeat: float = field(default_factory=time.time)
    websocket_connection: Optional[Any] = None
    
    def __post_init__(self):
        self.metadata['version'] = '1.0.0'
        self.metadata['runtime'] = 'python'
    
    def is_available(self) -> bool:
        """Check if node is available for new tasks"""
        return (self.status == "online" and 
                self.current_load < self.max_load and
                time.time() - self.last_heartbeat < 30)  # 30 second heartbeat timeout
    
    def can_handle_task(self, task_type: str) -> bool:
        """Check if node can handle a specific task type"""
        return task_type in self.capabilities
    
    def update_heartbeat(self):
        """Update the last heartbeat timestamp"""
        self.last_heartbeat = time.time()
        if self.status == "offline":
            self.status = "online"
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary representation"""
        return {
            'node_id': self.node_id,
            'node_type': self.node_type.value,
            'capabilities': list(self.capabilities),
            'current_load': self.current_load,
            'max_load': self.max_load,
            'status': self.status,
            'metadata': self.metadata,
            'created_at': self.created_at,
            'last_heartbeat': self.last_heartbeat,
            'is_available': self.is_available()
        }


@dataclass
class DistributedTask:
    """Represents a task in the distributed cognitive mesh"""
    task_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    task_type: str = "general"
    payload: Dict[str, Any] = field(default_factory=dict)
    status: TaskStatus = TaskStatus.PENDING
    assigned_node: Optional[str] = None
    requester_node: Optional[str] = None
    created_at: float = field(default_factory=time.time)
    started_at: Optional[float] = None
    completed_at: Optional[float] = None
    result: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    priority: int = 5  # 1-10, higher is more urgent
    timeout: float = 300.0  # 5 minutes default timeout
    
    def start_execution(self, node_id: str):
        """Mark task as started"""
        self.status = TaskStatus.RUNNING
        self.assigned_node = node_id
        self.started_at = time.time()
    
    def complete_execution(self, result: Dict[str, Any]):
        """Mark task as completed"""
        self.status = TaskStatus.COMPLETED
        self.result = result
        self.completed_at = time.time()
    
    def fail_execution(self, error: str):
        """Mark task as failed"""
        self.status = TaskStatus.FAILED
        self.error = error
        self.completed_at = time.time()
    
    def is_expired(self) -> bool:
        """Check if task has expired"""
        if self.started_at is None:
            return time.time() - self.created_at > self.timeout
        return time.time() - self.started_at > self.timeout
    
    def get_execution_time(self) -> Optional[float]:
        """Get task execution time"""
        if self.started_at and self.completed_at:
            return self.completed_at - self.started_at
        return None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary representation"""
        return {
            'task_id': self.task_id,
            'task_type': self.task_type,
            'payload': self.payload,
            'status': self.status.value,
            'assigned_node': self.assigned_node,
            'requester_node': self.requester_node,
            'created_at': self.created_at,
            'started_at': self.started_at,
            'completed_at': self.completed_at,
            'result': self.result,
            'error': self.error,
            'priority': self.priority,
            'timeout': self.timeout,
            'execution_time': self.get_execution_time(),
            'is_expired': self.is_expired()
        }


class CognitiveMeshOrchestrator:
    """Enhanced orchestrator for distributed cognitive tasks with discovery, fault tolerance, and auto-recovery"""
    
    def __init__(self, ecan_system=None):
        self.nodes: Dict[str, MeshNode] = {}
        self.tasks: Dict[str, DistributedTask] = {}
        self.task_queue: List[DistributedTask] = []
        self.completed_tasks: List[DistributedTask] = []
        self.websocket_connections: Dict[str, Any] = {}
        self.event_handlers: Dict[str, List[Callable]] = {}
        self.is_running = False
        
        # ECAN integration
        self.ecan_system = ecan_system
        self.attention_based_scheduling = True
        
        # Phase 3 enhancements: Discovery, fault tolerance, and load testing
        self.discovery_service = None
        self.fault_tolerance_manager = None
        self.load_testing_framework = None
        
        self.orchestration_stats = {
            'tasks_completed': 0,
            'tasks_failed': 0,
            'total_processing_time': 0.0,
            'average_processing_time': 0.0,
            'nodes_online': 0,
            'mesh_load': 0.0,
            'attention_influenced_tasks': 0,
            'average_attention_priority': 5.0,
            # Phase 3 stats
            'nodes_discovered': 0,
            'auto_recoveries': 0,
            'fault_detections': 0,
            'load_tests_completed': 0
        }
        
        # Start background tasks
        self.executor = ThreadPoolExecutor(max_workers=10)
        # Don't start async tasks at module import time
        # self._start_background_tasks()
    
    def setup_phase3_components(self, discovery_service=None, fault_tolerance_manager=None, load_testing_framework=None):
        """Setup Phase 3 components for enhanced mesh functionality"""
        if discovery_service:
            self.discovery_service = discovery_service
            # Register discovery callback
            self.discovery_service.add_discovery_callback(self._on_node_discovered)
            logger.info("Discovery service integrated")
        
        if fault_tolerance_manager:
            self.fault_tolerance_manager = fault_tolerance_manager
            logger.info("Fault tolerance manager integrated")
        
        if load_testing_framework:
            self.load_testing_framework = load_testing_framework
            # Set references for load testing
            self.load_testing_framework.mesh_orchestrator = self
            self.load_testing_framework.fault_manager = self.fault_tolerance_manager
            self.load_testing_framework.discovery_service = self.discovery_service
            logger.info("Load testing framework integrated")
    
    def _on_node_discovered(self, node_advertisement):
        """Callback for when a new node is discovered"""
        from .discovery import NodeAdvertisement
        
        # Convert discovery advertisement to mesh node
        mesh_node = MeshNode(
            node_id=node_advertisement.node_id,
            node_type=MeshNodeType(node_advertisement.node_type),
            capabilities=node_advertisement.capabilities,
            current_load=node_advertisement.current_load,
            max_load=node_advertisement.load_capacity,
            metadata=node_advertisement.metadata
        )
        
        # Register the discovered node
        self.register_node(mesh_node)
        self.orchestration_stats['nodes_discovered'] += 1
        
        logger.info(f"Auto-registered discovered node: {node_advertisement.node_id}")
    
    async def start_enhanced_orchestration(self):
        """Start enhanced orchestration with Phase 3 features"""
        await self.start_orchestration()
        
        # Start discovery service
        if self.discovery_service:
            await self.discovery_service.start_discovery()
        
        # Start fault tolerance monitoring
        if self.fault_tolerance_manager:
            await self.fault_tolerance_manager.start_monitoring()
            
            # Register existing nodes for health monitoring
            for node in self.nodes.values():
                self.fault_tolerance_manager.register_node_health(node.node_id)
        
        logger.info("Enhanced orchestration started with Phase 3 features")
    
    async def stop_enhanced_orchestration(self):
        """Stop enhanced orchestration"""
        # Stop discovery service
        if self.discovery_service:
            await self.discovery_service.stop_discovery()
        
        # Stop fault tolerance monitoring
        if self.fault_tolerance_manager:
            await self.fault_tolerance_manager.stop_monitoring()
        
        # Stop standard orchestration
        self.shutdown()
        
        logger.info("Enhanced orchestration stopped")
    
    def find_optimal_nodes_for_task(self, task: DistributedTask, max_nodes: int = 3) -> List[MeshNode]:
        """Find optimal nodes for task execution using enhanced capability matching"""
        if not self.discovery_service:
            # Fallback to original node selection
            return self._find_suitable_nodes_fallback(task, max_nodes)
        
        # Get required capabilities from task
        required_capabilities = set()
        if task.task_type in ['text_processing', 'dialogue']:
            required_capabilities.add('text_processing')
        elif task.task_type in ['neural_inference', 'reasoning']:
            required_capabilities.update(['neural_inference', 'reasoning'])
        elif task.task_type in ['attention_allocation']:
            required_capabilities.add('attention_allocation')
        else:
            required_capabilities.add('general_processing')
        
        # Use discovery service for advanced capability matching
        from .discovery import NodeAdvertisement
        
        # Convert mesh nodes to advertisements for capability matching
        node_advertisements = []
        for node in self.nodes.values():
            if node.is_available():
                advertisement = NodeAdvertisement(
                    node_id=node.node_id,
                    node_type=node.node_type.value,
                    capabilities=node.capabilities,
                    endpoint="",  # Not used for matching
                    port=0,      # Not used for matching
                    load_capacity=node.max_load,
                    current_load=node.current_load
                )
                node_advertisements.append(advertisement)
        
        # Find optimal nodes
        optimal_advertisements = self.discovery_service.find_nodes_for_capabilities(
            required_capabilities, task.priority / 10.0, max_nodes
        )
        
        # Convert back to mesh nodes
        optimal_nodes = []
        for advertisement in optimal_advertisements:
            if advertisement.node_id in self.nodes:
                optimal_nodes.append(self.nodes[advertisement.node_id])
        
        return optimal_nodes
    
    def _find_suitable_nodes_fallback(self, task: DistributedTask, max_nodes: int) -> List[MeshNode]:
        """Fallback node selection when discovery service is not available"""
        available_nodes = [
            node for node in self.nodes.values()
            if node.is_available() and node.can_handle_task(task.task_type)
        ]
        
        if not available_nodes:
            return []
        
        # Sort by load and capabilities
        available_nodes.sort(key=lambda n: (n.current_load, -len(n.capabilities)))
        return available_nodes[:max_nodes]
    
    def update_node_health_metrics(self, node_id: str, **metrics):
        """Update health metrics for a node"""
        if self.fault_tolerance_manager:
            self.fault_tolerance_manager.update_node_health(node_id, **metrics)
        
        # Update node load based on health
        if node_id in self.nodes:
            node = self.nodes[node_id]
            node.update_heartbeat()
            
            # Adjust load based on health metrics
            if 'cpu_usage' in metrics and metrics['cpu_usage'] > 0.9:
                node.current_load = min(node.max_load, node.current_load + 0.1)
            elif 'cpu_usage' in metrics and metrics['cpu_usage'] < 0.3:
                node.current_load = max(0.0, node.current_load - 0.05)
    
    async def run_load_test(self, config_name: str = 'quick_throughput') -> Dict[str, Any]:
        """Run a load test on the mesh"""
        if not self.load_testing_framework:
            logger.error("Load testing framework not available")
            return {'error': 'Load testing framework not configured'}
        
        from .load_testing import PREDEFINED_CONFIGS
        
        if config_name not in PREDEFINED_CONFIGS:
            logger.error(f"Unknown load test config: {config_name}")
            return {'error': f'Unknown config: {config_name}'}
        
        config = PREDEFINED_CONFIGS[config_name]
        logger.info(f"Starting load test: {config.test_name}")
        
        try:
            result = await self.load_testing_framework.run_load_test(config)
            self.orchestration_stats['load_tests_completed'] += 1
            return result
        except Exception as e:
            logger.error(f"Load test failed: {e}")
            return {'error': str(e)}
    
    def get_enhanced_mesh_status(self) -> Dict[str, Any]:
        """Get comprehensive mesh status including Phase 3 enhancements"""
        base_status = self.get_mesh_status()
        
        # Add discovery service status
        if self.discovery_service:
            discovery_stats = self.discovery_service.get_discovery_statistics()
            base_status['discovery'] = discovery_stats
        
        # Add fault tolerance status  
        if self.fault_tolerance_manager:
            health_summary = self.fault_tolerance_manager.get_health_summary()
            failure_stats = self.fault_tolerance_manager.get_failure_statistics()
            base_status['health'] = health_summary
            base_status['fault_tolerance'] = failure_stats
        
        # Add load testing status
        if self.load_testing_framework:
            load_test_summary = self.load_testing_framework.get_test_summary()
            base_status['load_testing'] = load_test_summary
        
        # Enhanced statistics
        base_status['enhanced_statistics'] = self.orchestration_stats
        
        return base_status
    
    async def start_orchestration(self):
        """Start standard orchestration (called by enhanced orchestration)"""
        if not self.is_running:
            self.is_running = True
            try:
                asyncio.create_task(self._orchestration_loop())
                asyncio.create_task(self._heartbeat_monitor())
                logger.info("Standard orchestration started")
            except RuntimeError:
                # No event loop running, tasks will be started manually
                pass
    
    def _start_background_tasks(self):
        """Start background orchestration tasks"""
        if not self.is_running:
            self.is_running = True
            # Only start tasks if we have an event loop
            try:
                loop = asyncio.get_running_loop()
                asyncio.create_task(self._orchestration_loop())
                asyncio.create_task(self._heartbeat_monitor())
            except RuntimeError:
                # No event loop running, tasks will be started manually
                pass
    
    def register_node(self, node: MeshNode) -> str:
        """Register a new node in the mesh"""
        self.nodes[node.node_id] = node
        self.orchestration_stats['nodes_online'] = len([n for n in self.nodes.values() if n.is_available()])
        
        # Notify other nodes about new node
        self._broadcast_event("node_joined", node.to_dict())
        
        logger.info(f"Registered mesh node: {node.node_id} ({node.node_type.value})")
        return node.node_id
    
    def unregister_node(self, node_id: str) -> bool:
        """Unregister a node from the mesh"""
        if node_id in self.nodes:
            node = self.nodes[node_id]
            node.status = "offline"
            
            # Reassign any pending tasks
            self._reassign_node_tasks(node_id)
            
            # Remove from active nodes
            del self.nodes[node_id]
            
            self.orchestration_stats['nodes_online'] = len([n for n in self.nodes.values() if n.is_available()])
            
            # Notify other nodes
            self._broadcast_event("node_left", {"node_id": node_id})
            
            logger.info(f"Unregistered mesh node: {node_id}")
            return True
        
        return False
    
    def submit_task(self, task: DistributedTask) -> str:
        """Submit a new task to the mesh"""
        self.tasks[task.task_id] = task
        
        # Integrate with ECAN attention system if available
        if self.ecan_system and self.attention_based_scheduling:
            # Register task with ECAN for attention-based scheduling
            element_id = f"task_{task.task_type}_{task.task_id}"
            
            # Register element if not already registered
            if element_id not in self.ecan_system.element_attention:
                self.ecan_system.register_cognitive_element(element_id)
            
            # Register task-attention mapping
            self.ecan_system.register_task_attention_mapping(task.task_id, element_id)
            
            # Update task priority based on attention
            attention_priority = self.ecan_system.get_task_attention_priority(task.task_id)
            task.priority = max(task.priority, int(attention_priority))
            
            self.orchestration_stats['attention_influenced_tasks'] += 1
            
            # Register AtomSpace patterns if payload contains text
            if 'text' in task.payload:
                # Create simple AtomSpace patterns from task text
                patterns = [
                    f'(ConceptNode "Task_{task.task_type}")',
                    f'(PredicateNode "execute")',
                    f'(EvaluationLink (PredicateNode "has_payload") (ConceptNode "Task_{task.task_id}"))'
                ]
                for pattern in patterns:
                    self.ecan_system.register_atomspace_pattern(element_id, pattern)
        
        self.task_queue.append(task)
        
        # Sort by priority (higher priority first), with attention-based priorities taking precedence
        self.task_queue.sort(key=lambda t: (
            self.ecan_system.get_task_attention_priority(t.task_id) if self.ecan_system else t.priority,
            t.priority
        ), reverse=True)
        
        logger.info(f"Submitted task: {task.task_id} (type: {task.task_type}, priority: {task.priority})")
        return task.task_id
    
    def get_task_status(self, task_id: str) -> Optional[Dict[str, Any]]:
        """Get status of a specific task"""
        if task_id in self.tasks:
            return self.tasks[task_id].to_dict()
        return None
    
    def cancel_task(self, task_id: str) -> bool:
        """Cancel a pending or running task"""
        if task_id in self.tasks:
            task = self.tasks[task_id]
            if task.status in [TaskStatus.PENDING, TaskStatus.RUNNING]:
                task.status = TaskStatus.CANCELLED
                task.completed_at = time.time()
                
                # Remove from queue if pending
                if task in self.task_queue:
                    self.task_queue.remove(task)
                
                # Notify assigned node if running
                if task.assigned_node and task.assigned_node in self.nodes:
                    self._send_message_to_node(task.assigned_node, {
                        'type': 'task_cancelled',
                        'task_id': task_id
                    })
                
                logger.info(f"Cancelled task: {task_id}")
                return True
        
        return False
    
    def _find_suitable_node(self, task: DistributedTask) -> Optional[MeshNode]:
        """Find a suitable node for task execution using enhanced capabilities"""
        optimal_nodes = self.find_optimal_nodes_for_task(task, max_nodes=1)
        return optimal_nodes[0] if optimal_nodes else None
    
    def _assign_task_to_node(self, task: DistributedTask, node: MeshNode):
        """Assign a task to a specific node"""
        task.start_execution(node.node_id)
        node.current_load = min(node.max_load, node.current_load + 0.1)
        
        # Send task to node
        message = {
            'type': 'task_assignment',
            'task': task.to_dict()
        }
        
        self._send_message_to_node(node.node_id, message)
        
        logger.info(f"Assigned task {task.task_id} to node {node.node_id}")
    
    def _reassign_node_tasks(self, node_id: str):
        """Reassign tasks from a failed node"""
        for task in list(self.tasks.values()):
            if task.assigned_node == node_id and task.status == TaskStatus.RUNNING:
                task.status = TaskStatus.PENDING
                task.assigned_node = None
                task.started_at = None
                
                # Re-add to queue
                self.task_queue.append(task)
                self.task_queue.sort(key=lambda t: t.priority, reverse=True)
                
                logger.info(f"Reassigned task {task.task_id} from failed node {node_id}")
    
    def _send_message_to_node(self, node_id: str, message: Dict[str, Any]):
        """Send a message to a specific node"""
        if node_id in self.websocket_connections:
            try:
                websocket = self.websocket_connections[node_id]
                asyncio.create_task(websocket.send(json.dumps(message)))
            except Exception as e:
                logger.error(f"Failed to send message to node {node_id}: {e}")
    
    def _broadcast_event(self, event_type: str, data: Dict[str, Any]):
        """Broadcast an event to all connected nodes"""
        message = {
            'type': 'event',
            'event_type': event_type,
            'data': data,
            'timestamp': time.time()
        }
        
        for node_id in list(self.websocket_connections.keys()):
            self._send_message_to_node(node_id, message)
    
    async def _orchestration_loop(self):
        """Main orchestration loop"""
        while self.is_running:
            try:
                # Process pending tasks
                if self.task_queue:
                    task = self.task_queue[0]
                    
                    # Check if task is expired
                    if task.is_expired():
                        task.fail_execution("Task expired")
                        self.task_queue.remove(task)
                        self.completed_tasks.append(task)
                        self.orchestration_stats['tasks_failed'] += 1
                        continue
                    
                    # Find suitable node
                    suitable_node = self._find_suitable_node(task)
                    
                    if suitable_node:
                        self.task_queue.remove(task)
                        self._assign_task_to_node(task, suitable_node)
                
                # Update mesh statistics
                self._update_mesh_statistics()
                
                await asyncio.sleep(0.1)  # 100ms orchestration cycle
                
            except Exception as e:
                logger.error(f"Error in orchestration loop: {e}")
                await asyncio.sleep(1.0)
    
    async def _heartbeat_monitor(self):
        """Monitor node heartbeats and handle failures"""
        while self.is_running:
            try:
                current_time = time.time()
                
                for node_id, node in list(self.nodes.items()):
                    if current_time - node.last_heartbeat > 30:  # 30 second timeout
                        logger.warning(f"Node {node_id} heartbeat timeout")
                        node.status = "offline"
                        self._reassign_node_tasks(node_id)
                
                await asyncio.sleep(10)  # Check every 10 seconds
                
            except Exception as e:
                logger.error(f"Error in heartbeat monitor: {e}")
                await asyncio.sleep(10)
    
    def _update_mesh_statistics(self):
        """Update mesh performance statistics"""
        active_nodes = [n for n in self.nodes.values() if n.is_available()]
        self.orchestration_stats['nodes_online'] = len(active_nodes)
        
        if active_nodes:
            total_load = sum(n.current_load for n in active_nodes)
            self.orchestration_stats['mesh_load'] = total_load / len(active_nodes)
        
        # Update task statistics
        completed_tasks = [t for t in self.tasks.values() if t.status == TaskStatus.COMPLETED]
        failed_tasks = [t for t in self.tasks.values() if t.status == TaskStatus.FAILED]
        
        self.orchestration_stats['tasks_completed'] = len(completed_tasks)
        self.orchestration_stats['tasks_failed'] = len(failed_tasks)
        
        if completed_tasks:
            execution_times = [t.get_execution_time() for t in completed_tasks if t.get_execution_time()]
            if execution_times:
                self.orchestration_stats['total_processing_time'] = sum(execution_times)
                self.orchestration_stats['average_processing_time'] = sum(execution_times) / len(execution_times)
    
    def handle_task_completion(self, task_id: str, result: Dict[str, Any], node_id: str):
        """Handle task completion from a node"""
        if task_id in self.tasks:
            task = self.tasks[task_id]
            execution_time = time.time() - (task.started_at or time.time())
            task.complete_execution(result)
            
            # Update ECAN attention based on task completion
            if self.ecan_system:
                self.ecan_system.update_task_attention_from_completion(task_id, True, execution_time)
                
                # Update urgency for related tasks of the same type
                for other_task_id, other_task in self.tasks.items():
                    if (other_task.task_type == task.task_type and 
                        other_task.status in [TaskStatus.PENDING, TaskStatus.RUNNING]):
                        element_id = f"task_{other_task.task_type}_{other_task_id}"
                        if element_id in self.ecan_system.element_attention:
                            # Successful completion reduces urgency for similar pending tasks
                            self.ecan_system.element_attention[element_id].urgency *= 0.9
            
            # Update node load
            if node_id in self.nodes:
                node = self.nodes[node_id]
                node.current_load = max(0.0, node.current_load - 0.1)
            
            # Move to completed tasks
            self.completed_tasks.append(task)
            
            # Update statistics
            self.orchestration_stats['tasks_completed'] += 1
            if execution_time > 0:
                total_time = self.orchestration_stats['total_processing_time']
                completed_count = self.orchestration_stats['tasks_completed']
                self.orchestration_stats['total_processing_time'] = total_time + execution_time
                self.orchestration_stats['average_processing_time'] = (
                    self.orchestration_stats['total_processing_time'] / completed_count
                )
            
            # Notify requester if needed
            if task.requester_node and task.requester_node in self.nodes:
                self._send_message_to_node(task.requester_node, {
                    'type': 'task_completed',
                    'task_id': task_id,
                    'result': result,
                    'execution_time': execution_time
                })
            
            logger.info(f"Task {task_id} completed by node {node_id} in {execution_time:.2f}s")
    
    def handle_task_failure(self, task_id: str, error: str, node_id: str):
        """Handle task failure from a node"""
        if task_id in self.tasks:
            task = self.tasks[task_id]
            execution_time = time.time() - (task.started_at or time.time())
            task.fail_execution(error)
            
            # Update ECAN attention based on task failure
            if self.ecan_system:
                self.ecan_system.update_task_attention_from_completion(task_id, False, execution_time)
                
                # Increase urgency for related tasks of the same type
                for other_task_id, other_task in self.tasks.items():
                    if (other_task.task_type == task.task_type and 
                        other_task.status in [TaskStatus.PENDING, TaskStatus.RUNNING]):
                        element_id = f"task_{other_task.task_type}_{other_task_id}"
                        if element_id in self.ecan_system.element_attention:
                            # Failure increases urgency for similar pending tasks
                            self.ecan_system.element_attention[element_id].urgency = min(1.0,
                                self.ecan_system.element_attention[element_id].urgency + 0.2)
            
            # Update node load
            if node_id in self.nodes:
                node = self.nodes[node_id]
                node.current_load = max(0.0, node.current_load - 0.1)
            
            # Move to completed tasks
            self.completed_tasks.append(task)
            
            # Update statistics
            self.orchestration_stats['tasks_failed'] += 1
            
            # Notify requester if needed
            if task.requester_node and task.requester_node in self.nodes:
                self._send_message_to_node(task.requester_node, {
                    'type': 'task_failed',
                    'task_id': task_id,
                    'error': error,
                    'execution_time': execution_time
                })
            
            logger.error(f"Task {task_id} failed on node {node_id} after {execution_time:.2f}s: {error}")
    
    def get_mesh_status(self) -> Dict[str, Any]:
        """Get comprehensive mesh status"""
        # Update attention-based statistics if ECAN is available
        if self.ecan_system:
            attention_priorities = []
            for task_id in self.task_attention_mapping if hasattr(self, 'task_attention_mapping') else []:
                priority = self.ecan_system.get_task_attention_priority(task_id)
                attention_priorities.append(priority)
            
            if attention_priorities:
                avg_attention_priority = sum(attention_priorities) / len(attention_priorities)
                self.orchestration_stats['average_attention_priority'] = avg_attention_priority
        
        status = {
            'nodes': {node_id: node.to_dict() for node_id, node in self.nodes.items()},
            'tasks': {
                'pending': len(self.task_queue),
                'running': len([t for t in self.tasks.values() if t.status == TaskStatus.RUNNING]),
                'completed': len(self.completed_tasks),
                'failed': len([t for t in self.tasks.values() if t.status == TaskStatus.FAILED])
            },
            'statistics': self.orchestration_stats,
            'timestamp': time.time()
        }
        
        # Add ECAN integration status if available
        if self.ecan_system:
            ecan_stats = self.ecan_system.get_attention_statistics()
            status['ecan_integration'] = {
                'attention_based_scheduling': self.attention_based_scheduling,
                'total_attention_elements': ecan_stats['total_elements'],
                'total_attention_patterns': ecan_stats['total_patterns'],
                'total_attention_tasks': ecan_stats['total_tasks'],
                'attention_allocation_rounds': ecan_stats['allocation_rounds'],
                'average_sti': ecan_stats['average_sti'],
                'performance_metrics': ecan_stats['performance_metrics']
            }
        
        return status
    
    def get_node_performance(self, node_id: str) -> Optional[Dict[str, Any]]:
        """Get performance metrics for a specific node"""
        if node_id not in self.nodes:
            return None
        
        node = self.nodes[node_id]
        node_tasks = [t for t in self.tasks.values() if t.assigned_node == node_id]
        
        completed_tasks = [t for t in node_tasks if t.status == TaskStatus.COMPLETED]
        failed_tasks = [t for t in node_tasks if t.status == TaskStatus.FAILED]
        
        execution_times = [t.get_execution_time() for t in completed_tasks if t.get_execution_time()]
        
        return {
            'node_info': node.to_dict(),
            'tasks_completed': len(completed_tasks),
            'tasks_failed': len(failed_tasks),
            'success_rate': len(completed_tasks) / len(node_tasks) if node_tasks else 0,
            'average_execution_time': sum(execution_times) / len(execution_times) if execution_times else 0,
            'total_execution_time': sum(execution_times),
            'current_load': node.current_load
        }
    
    def shutdown(self):
        """Shutdown the orchestrator"""
        self.is_running = False
        
        # Cancel all pending tasks
        for task in self.task_queue:
            task.status = TaskStatus.CANCELLED
            task.completed_at = time.time()
        
        # Notify all nodes
        self._broadcast_event("orchestrator_shutdown", {})
        
        # Close WebSocket connections
        for websocket in self.websocket_connections.values():
            try:
                asyncio.create_task(websocket.close())
            except Exception:
                pass
        
        self.executor.shutdown(wait=True)
        logger.info("Cognitive mesh orchestrator shutdown complete")


# Global mesh orchestrator instance
mesh_orchestrator = CognitiveMeshOrchestrator()

# Set up ECAN integration - will be connected when both systems are initialized
def setup_ecan_integration():
    """Set up ECAN integration for the mesh orchestrator"""
    from ..ecan_attention.attention_kernel import ecan_system
    mesh_orchestrator.ecan_system = ecan_system
    logger.info("ECAN integration configured for mesh orchestrator")

def setup_phase3_integration():
    """Set up Phase 3 enhancements for the mesh orchestrator"""
    from .discovery import discovery_service
    from .fault_tolerance import fault_tolerance_manager
    from .load_testing import load_testing_framework
    
    mesh_orchestrator.setup_phase3_components(
        discovery_service=discovery_service,
        fault_tolerance_manager=fault_tolerance_manager,
        load_testing_framework=load_testing_framework
    )
    logger.info("Phase 3 enhancements configured for mesh orchestrator")

# Create some default nodes for testing
default_agent = MeshNode(
    node_type=MeshNodeType.AGENT,
    capabilities={"text_processing", "reasoning", "dialogue"},
    max_load=0.8
)

default_processor = MeshNode(
    node_type=MeshNodeType.PROCESSOR,
    capabilities={"neural_inference", "attention_allocation", "memory_management"},
    max_load=1.0
)

mesh_orchestrator.register_node(default_agent)
mesh_orchestrator.register_node(default_processor)