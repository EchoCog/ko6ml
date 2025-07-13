"""
Fault Tolerance and Auto-Recovery for Distributed Mesh

This module provides advanced fault tolerance, auto-recovery mechanisms,
and resilience features for the distributed cognitive mesh network.
"""

import asyncio
import json
import logging
import time
import uuid
from typing import Dict, Any, List, Optional, Set, Callable
from dataclasses import dataclass, field
from enum import Enum
import random
import statistics
from collections import defaultdict, deque

logger = logging.getLogger(__name__)


class FailureType(Enum):
    """Types of failures that can occur in the mesh"""
    NODE_CRASH = "node_crash"
    NODE_UNRESPONSIVE = "node_unresponsive"
    NETWORK_PARTITION = "network_partition"
    TASK_TIMEOUT = "task_timeout"
    TASK_FAILURE = "task_failure"
    RESOURCE_EXHAUSTION = "resource_exhaustion"
    COMMUNICATION_FAILURE = "communication_failure"


class RecoveryStrategy(Enum):
    """Recovery strategies for different failure types"""
    RESTART = "restart"
    REDISTRIBUTE = "redistribute"
    REPLICATE = "replicate"
    FALLBACK = "fallback"
    CIRCUIT_BREAKER = "circuit_breaker"
    GRACEFUL_DEGRADATION = "graceful_degradation"


@dataclass
class FailureEvent:
    """Represents a failure event in the mesh"""
    failure_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    failure_type: FailureType = FailureType.NODE_CRASH
    affected_node_id: Optional[str] = None
    affected_task_id: Optional[str] = None
    timestamp: float = field(default_factory=time.time)
    severity: float = 1.0  # 0.0 (minor) to 1.0 (critical)
    description: str = ""
    metadata: Dict[str, Any] = field(default_factory=dict)
    recovery_strategy: Optional[RecoveryStrategy] = None
    recovery_status: str = "pending"  # pending, in_progress, completed, failed
    recovery_start_time: Optional[float] = None
    recovery_end_time: Optional[float] = None
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'failure_id': self.failure_id,
            'failure_type': self.failure_type.value,
            'affected_node_id': self.affected_node_id,
            'affected_task_id': self.affected_task_id,
            'timestamp': self.timestamp,
            'severity': self.severity,
            'description': self.description,
            'metadata': self.metadata,
            'recovery_strategy': self.recovery_strategy.value if self.recovery_strategy else None,
            'recovery_status': self.recovery_status,
            'recovery_start_time': self.recovery_start_time,
            'recovery_end_time': self.recovery_end_time,
            'recovery_duration': self.get_recovery_duration()
        }
    
    def get_recovery_duration(self) -> Optional[float]:
        """Get recovery duration if completed"""
        if self.recovery_start_time and self.recovery_end_time:
            return self.recovery_end_time - self.recovery_start_time
        return None


@dataclass
class HealthMetrics:
    """Health metrics for a mesh node"""
    node_id: str
    cpu_usage: float = 0.0
    memory_usage: float = 0.0
    network_latency: float = 0.0
    task_success_rate: float = 1.0
    task_completion_time: float = 0.0
    error_rate: float = 0.0
    uptime: float = 0.0
    last_heartbeat: float = field(default_factory=time.time)
    health_score: float = 1.0  # 0.0 (unhealthy) to 1.0 (healthy)
    
    def update_health_score(self):
        """Calculate health score based on metrics"""
        # Weighted health score calculation
        cpu_factor = max(0.0, 1.0 - self.cpu_usage)
        memory_factor = max(0.0, 1.0 - self.memory_usage)
        latency_factor = max(0.0, 1.0 - min(1.0, self.network_latency / 1000.0))  # Normalize to 1 second
        success_factor = self.task_success_rate
        error_factor = max(0.0, 1.0 - self.error_rate)
        
        self.health_score = (
            cpu_factor * 0.2 +
            memory_factor * 0.2 +
            latency_factor * 0.2 +
            success_factor * 0.25 +
            error_factor * 0.15
        )
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'node_id': self.node_id,
            'cpu_usage': self.cpu_usage,
            'memory_usage': self.memory_usage,
            'network_latency': self.network_latency,
            'task_success_rate': self.task_success_rate,
            'task_completion_time': self.task_completion_time,
            'error_rate': self.error_rate,
            'uptime': self.uptime,
            'last_heartbeat': self.last_heartbeat,
            'health_score': self.health_score,
            'heartbeat_age': time.time() - self.last_heartbeat
        }


class CircuitBreaker:
    """Circuit breaker pattern for fault tolerance"""
    
    def __init__(self, failure_threshold: int = 5, recovery_timeout: float = 60.0):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.failure_count = 0
        self.last_failure_time = 0.0
        self.state = "closed"  # closed, open, half_open
    
    def call_service(self, service_func: Callable, *args, **kwargs):
        """Call service through circuit breaker"""
        if self.state == "open":
            if time.time() - self.last_failure_time >= self.recovery_timeout:
                self.state = "half_open"
            else:
                raise Exception("Circuit breaker is OPEN")
        
        try:
            result = service_func(*args, **kwargs)
            
            if self.state == "half_open":
                self.state = "closed"
                self.failure_count = 0
            
            return result
            
        except Exception as e:
            self.failure_count += 1
            self.last_failure_time = time.time()
            
            if self.failure_count >= self.failure_threshold:
                self.state = "open"
            
            raise e
    
    def get_state(self) -> Dict[str, Any]:
        return {
            'state': self.state,
            'failure_count': self.failure_count,
            'failure_threshold': self.failure_threshold,
            'last_failure_time': self.last_failure_time,
            'recovery_timeout': self.recovery_timeout
        }


class FaultToleranceManager:
    """Manages fault tolerance and auto-recovery for the mesh"""
    
    def __init__(self):
        self.health_metrics: Dict[str, HealthMetrics] = {}
        self.failure_history: deque = deque(maxlen=1000)
        self.recovery_handlers: Dict[FailureType, Callable] = {}
        self.circuit_breakers: Dict[str, CircuitBreaker] = {}
        self.is_monitoring = False
        
        # Configuration
        self.health_check_interval = 10.0  # seconds
        self.failure_detection_threshold = 30.0  # seconds for heartbeat timeout
        self.max_recovery_attempts = 3
        self.recovery_backoff_base = 2.0  # exponential backoff base
        
        # Recovery statistics
        self.recovery_stats = {
            'total_failures': 0,
            'successful_recoveries': 0,
            'failed_recoveries': 0,
            'average_recovery_time': 0.0,
            'failure_types': defaultdict(int),
            'recovery_strategies': defaultdict(int)
        }
        
        # Setup default recovery handlers
        self._setup_default_recovery_handlers()
    
    def _setup_default_recovery_handlers(self):
        """Setup default recovery handlers for different failure types"""
        self.recovery_handlers[FailureType.NODE_CRASH] = self._handle_node_crash
        self.recovery_handlers[FailureType.NODE_UNRESPONSIVE] = self._handle_node_unresponsive
        self.recovery_handlers[FailureType.TASK_TIMEOUT] = self._handle_task_timeout
        self.recovery_handlers[FailureType.TASK_FAILURE] = self._handle_task_failure
        self.recovery_handlers[FailureType.COMMUNICATION_FAILURE] = self._handle_communication_failure
        self.recovery_handlers[FailureType.RESOURCE_EXHAUSTION] = self._handle_resource_exhaustion
    
    def register_node_health(self, node_id: str) -> HealthMetrics:
        """Register a node for health monitoring"""
        if node_id not in self.health_metrics:
            self.health_metrics[node_id] = HealthMetrics(node_id=node_id)
        return self.health_metrics[node_id]
    
    def update_node_health(self, node_id: str, **metrics):
        """Update health metrics for a node"""
        if node_id not in self.health_metrics:
            self.register_node_health(node_id)
        
        health = self.health_metrics[node_id]
        for key, value in metrics.items():
            if hasattr(health, key):
                setattr(health, key, value)
        
        health.last_heartbeat = time.time()
        health.update_health_score()
    
    def get_circuit_breaker(self, service_name: str) -> CircuitBreaker:
        """Get or create circuit breaker for a service"""
        if service_name not in self.circuit_breakers:
            self.circuit_breakers[service_name] = CircuitBreaker()
        return self.circuit_breakers[service_name]
    
    async def start_monitoring(self):
        """Start health monitoring and fault detection"""
        if self.is_monitoring:
            return
        
        self.is_monitoring = True
        logger.info("Starting fault tolerance monitoring")
        
        # Start monitoring tasks
        asyncio.create_task(self._health_monitor_loop())
        asyncio.create_task(self._failure_detection_loop())
    
    async def stop_monitoring(self):
        """Stop health monitoring"""
        self.is_monitoring = False
        logger.info("Stopping fault tolerance monitoring")
    
    async def _health_monitor_loop(self):
        """Main health monitoring loop"""
        while self.is_monitoring:
            try:
                await self._collect_health_metrics()
                await asyncio.sleep(self.health_check_interval)
            except Exception as e:
                logger.error(f"Error in health monitor loop: {e}")
                await asyncio.sleep(self.health_check_interval)
    
    async def _failure_detection_loop(self):
        """Detect failures based on health metrics"""
        while self.is_monitoring:
            try:
                await self._detect_failures()
                await asyncio.sleep(5.0)  # Check for failures every 5 seconds
            except Exception as e:
                logger.error(f"Error in failure detection loop: {e}")
                await asyncio.sleep(5.0)
    
    async def _collect_health_metrics(self):
        """Collect health metrics from all nodes"""
        current_time = time.time()
        
        for node_id, health in self.health_metrics.items():
            # Update uptime
            health.uptime = current_time - (health.last_heartbeat - health.uptime)
            
            # Simulate some metric collection (in real implementation, this would query nodes)
            # For demonstration, we'll add some variation to existing metrics
            if random.random() < 0.1:  # 10% chance to update metrics
                health.cpu_usage = max(0.0, min(1.0, health.cpu_usage + random.uniform(-0.1, 0.1)))
                health.memory_usage = max(0.0, min(1.0, health.memory_usage + random.uniform(-0.05, 0.05)))
                health.network_latency = max(0.0, health.network_latency + random.uniform(-10, 10))
                health.update_health_score()
    
    async def _detect_failures(self):
        """Detect failures based on health metrics and patterns"""
        current_time = time.time()
        
        for node_id, health in self.health_metrics.items():
            # Check for heartbeat timeout (node unresponsive)
            heartbeat_age = current_time - health.last_heartbeat
            
            if heartbeat_age > self.failure_detection_threshold:
                await self._report_failure(
                    FailureType.NODE_UNRESPONSIVE,
                    affected_node_id=node_id,
                    severity=min(1.0, heartbeat_age / (self.failure_detection_threshold * 2)),
                    description=f"Node {node_id} has not sent heartbeat for {heartbeat_age:.1f} seconds"
                )
            
            # Check for resource exhaustion
            if health.cpu_usage > 0.95 or health.memory_usage > 0.95:
                await self._report_failure(
                    FailureType.RESOURCE_EXHAUSTION,
                    affected_node_id=node_id,
                    severity=max(health.cpu_usage, health.memory_usage),
                    description=f"Node {node_id} has high resource usage (CPU: {health.cpu_usage:.1%}, Memory: {health.memory_usage:.1%})"
                )
            
            # Check for high error rate
            if health.error_rate > 0.5:
                await self._report_failure(
                    FailureType.COMMUNICATION_FAILURE,
                    affected_node_id=node_id,
                    severity=health.error_rate,
                    description=f"Node {node_id} has high error rate: {health.error_rate:.1%}"
                )
    
    async def _report_failure(self, failure_type: FailureType, 
                            affected_node_id: str = None,
                            affected_task_id: str = None,
                            severity: float = 1.0,
                            description: str = "",
                            metadata: Dict[str, Any] = None):
        """Report a failure and initiate recovery"""
        failure_event = FailureEvent(
            failure_type=failure_type,
            affected_node_id=affected_node_id,
            affected_task_id=affected_task_id,
            severity=severity,
            description=description,
            metadata=metadata or {}
        )
        
        # Check if this is a duplicate failure (within last 60 seconds)
        recent_failures = [
            f for f in self.failure_history
            if (time.time() - f.timestamp < 60.0 and
                f.failure_type == failure_type and
                f.affected_node_id == affected_node_id and
                f.affected_task_id == affected_task_id)
        ]
        
        if recent_failures:
            logger.debug(f"Duplicate failure detected for {failure_type.value}, skipping")
            return
        
        self.failure_history.append(failure_event)
        self.recovery_stats['total_failures'] += 1
        self.recovery_stats['failure_types'][failure_type.value] += 1
        
        logger.warning(f"Failure detected: {failure_type.value} - {description}")
        
        # Initiate recovery
        await self._initiate_recovery(failure_event)
    
    async def _initiate_recovery(self, failure_event: FailureEvent):
        """Initiate recovery for a failure event"""
        # Determine recovery strategy
        strategy = self._determine_recovery_strategy(failure_event)
        failure_event.recovery_strategy = strategy
        failure_event.recovery_status = "in_progress"
        failure_event.recovery_start_time = time.time()
        
        self.recovery_stats['recovery_strategies'][strategy.value] += 1
        
        logger.info(f"Initiating recovery for {failure_event.failure_id} using strategy: {strategy.value}")
        
        try:
            # Get recovery handler
            handler = self.recovery_handlers.get(failure_event.failure_type)
            if handler:
                success = await handler(failure_event)
                
                if success:
                    failure_event.recovery_status = "completed"
                    self.recovery_stats['successful_recoveries'] += 1
                    logger.info(f"Recovery completed successfully for {failure_event.failure_id}")
                else:
                    failure_event.recovery_status = "failed"
                    self.recovery_stats['failed_recoveries'] += 1
                    logger.error(f"Recovery failed for {failure_event.failure_id}")
            else:
                failure_event.recovery_status = "failed"
                logger.error(f"No recovery handler for failure type: {failure_event.failure_type.value}")
        
        except Exception as e:
            failure_event.recovery_status = "failed"
            self.recovery_stats['failed_recoveries'] += 1
            logger.error(f"Exception during recovery for {failure_event.failure_id}: {e}")
        
        finally:
            failure_event.recovery_end_time = time.time()
            
            # Update average recovery time
            if failure_event.get_recovery_duration():
                total_recoveries = self.recovery_stats['successful_recoveries'] + self.recovery_stats['failed_recoveries']
                old_avg = self.recovery_stats['average_recovery_time']
                new_duration = failure_event.get_recovery_duration()
                self.recovery_stats['average_recovery_time'] = (old_avg * (total_recoveries - 1) + new_duration) / total_recoveries
    
    def _determine_recovery_strategy(self, failure_event: FailureEvent) -> RecoveryStrategy:
        """Determine the best recovery strategy for a failure"""
        failure_type = failure_event.failure_type
        severity = failure_event.severity
        
        # Strategy selection based on failure type and severity
        if failure_type == FailureType.NODE_CRASH:
            return RecoveryStrategy.RESTART if severity < 0.8 else RecoveryStrategy.REDISTRIBUTE
        elif failure_type == FailureType.NODE_UNRESPONSIVE:
            return RecoveryStrategy.RESTART if severity < 0.5 else RecoveryStrategy.REDISTRIBUTE
        elif failure_type == FailureType.TASK_TIMEOUT:
            return RecoveryStrategy.REDISTRIBUTE
        elif failure_type == FailureType.TASK_FAILURE:
            return RecoveryStrategy.REPLICATE if severity < 0.6 else RecoveryStrategy.REDISTRIBUTE
        elif failure_type == FailureType.RESOURCE_EXHAUSTION:
            return RecoveryStrategy.REDISTRIBUTE
        elif failure_type == FailureType.COMMUNICATION_FAILURE:
            return RecoveryStrategy.CIRCUIT_BREAKER if severity < 0.7 else RecoveryStrategy.FALLBACK
        else:
            return RecoveryStrategy.GRACEFUL_DEGRADATION
    
    async def _handle_node_crash(self, failure_event: FailureEvent) -> bool:
        """Handle node crash recovery"""
        node_id = failure_event.affected_node_id
        
        logger.info(f"Handling node crash for {node_id}")
        
        # In a real implementation, this would:
        # 1. Attempt to restart the node
        # 2. Redistribute tasks from the crashed node
        # 3. Update mesh topology
        
        # For simulation, we'll mark it as recovered after a delay
        await asyncio.sleep(2.0)
        
        # Remove from health metrics if still unresponsive
        if node_id in self.health_metrics:
            health = self.health_metrics[node_id]
            if time.time() - health.last_heartbeat > self.failure_detection_threshold * 2:
                del self.health_metrics[node_id]
                logger.info(f"Removed crashed node {node_id} from health monitoring")
        
        return True
    
    async def _handle_node_unresponsive(self, failure_event: FailureEvent) -> bool:
        """Handle unresponsive node recovery"""
        node_id = failure_event.affected_node_id
        
        logger.info(f"Handling unresponsive node: {node_id}")
        
        # Attempt to ping the node or restart it
        await asyncio.sleep(1.0)
        
        # Check if node has recovered
        if node_id in self.health_metrics:
            health = self.health_metrics[node_id]
            if time.time() - health.last_heartbeat < 30.0:
                logger.info(f"Node {node_id} has recovered")
                return True
        
        return False
    
    async def _handle_task_timeout(self, failure_event: FailureEvent) -> bool:
        """Handle task timeout recovery"""
        task_id = failure_event.affected_task_id
        
        logger.info(f"Handling task timeout for task {task_id}")
        
        # In a real implementation, this would:
        # 1. Cancel the timed-out task
        # 2. Redistribute the task to another node
        # 3. Update task priority
        
        await asyncio.sleep(0.5)
        return True
    
    async def _handle_task_failure(self, failure_event: FailureEvent) -> bool:
        """Handle task failure recovery"""
        task_id = failure_event.affected_task_id
        
        logger.info(f"Handling task failure for task {task_id}")
        
        # In a real implementation, this would:
        # 1. Analyze the failure cause
        # 2. Retry on the same node or redistribute
        # 3. Update error statistics
        
        await asyncio.sleep(0.5)
        return True
    
    async def _handle_communication_failure(self, failure_event: FailureEvent) -> bool:
        """Handle communication failure recovery"""
        node_id = failure_event.affected_node_id
        
        logger.info(f"Handling communication failure for {node_id}")
        
        # Activate circuit breaker for this node
        cb = self.get_circuit_breaker(f"node_{node_id}")
        cb.state = "open"
        cb.last_failure_time = time.time()
        
        await asyncio.sleep(1.0)
        return True
    
    async def _handle_resource_exhaustion(self, failure_event: FailureEvent) -> bool:
        """Handle resource exhaustion recovery"""
        node_id = failure_event.affected_node_id
        
        logger.info(f"Handling resource exhaustion for {node_id}")
        
        # In a real implementation, this would:
        # 1. Throttle task assignment to this node
        # 2. Redistribute some tasks to other nodes
        # 3. Trigger garbage collection or resource cleanup
        
        await asyncio.sleep(1.0)
        
        # Simulate resource cleanup
        if node_id in self.health_metrics:
            health = self.health_metrics[node_id]
            health.cpu_usage *= 0.8  # Reduce CPU usage
            health.memory_usage *= 0.8  # Reduce memory usage
            health.update_health_score()
        
        return True
    
    def get_health_summary(self) -> Dict[str, Any]:
        """Get health summary for all nodes"""
        if not self.health_metrics:
            return {'nodes': {}, 'summary': {'total_nodes': 0, 'healthy_nodes': 0, 'average_health': 0.0}}
        
        healthy_nodes = sum(1 for h in self.health_metrics.values() if h.health_score > 0.7)
        total_health = sum(h.health_score for h in self.health_metrics.values())
        average_health = total_health / len(self.health_metrics)
        
        return {
            'nodes': {node_id: health.to_dict() for node_id, health in self.health_metrics.items()},
            'summary': {
                'total_nodes': len(self.health_metrics),
                'healthy_nodes': healthy_nodes,
                'unhealthy_nodes': len(self.health_metrics) - healthy_nodes,
                'average_health': average_health,
                'health_score_distribution': self._get_health_distribution()
            }
        }
    
    def _get_health_distribution(self) -> Dict[str, int]:
        """Get distribution of health scores"""
        distribution = {'excellent': 0, 'good': 0, 'fair': 0, 'poor': 0, 'critical': 0}
        
        for health in self.health_metrics.values():
            score = health.health_score
            if score >= 0.9:
                distribution['excellent'] += 1
            elif score >= 0.7:
                distribution['good'] += 1
            elif score >= 0.5:
                distribution['fair'] += 1
            elif score >= 0.3:
                distribution['poor'] += 1
            else:
                distribution['critical'] += 1
        
        return distribution
    
    def get_failure_statistics(self) -> Dict[str, Any]:
        """Get comprehensive failure and recovery statistics"""
        recent_failures = [f for f in self.failure_history if time.time() - f.timestamp < 3600]  # Last hour
        
        return {
            'recovery_stats': self.recovery_stats,
            'recent_failures': {
                'count': len(recent_failures),
                'by_type': defaultdict(int, {
                    ft.value: sum(1 for f in recent_failures if f.failure_type == ft)
                    for ft in FailureType
                }),
                'average_severity': statistics.mean([f.severity for f in recent_failures]) if recent_failures else 0.0
            },
            'circuit_breakers': {
                name: cb.get_state() for name, cb in self.circuit_breakers.items()
            },
            'monitoring_status': {
                'is_monitoring': self.is_monitoring,
                'health_check_interval': self.health_check_interval,
                'failure_detection_threshold': self.failure_detection_threshold,
                'total_nodes_monitored': len(self.health_metrics)
            }
        }


# Global fault tolerance manager instance
fault_tolerance_manager = FaultToleranceManager()