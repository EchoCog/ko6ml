"""
Load Testing and Resilience Validation for Distributed Mesh

This module provides comprehensive load testing, scalability validation,
and resilience testing for the distributed cognitive mesh network.
"""

import asyncio
import json
import logging
import time
import uuid
import random
import statistics
from typing import Dict, Any, List, Optional, Set, Callable, Tuple
from dataclasses import dataclass, field
from enum import Enum
import numpy as np
from concurrent.futures import ThreadPoolExecutor
import threading

logger = logging.getLogger(__name__)


class LoadTestType(Enum):
    """Types of load tests"""
    THROUGHPUT = "throughput"
    LATENCY = "latency"
    SCALABILITY = "scalability"
    STRESS = "stress"
    ENDURANCE = "endurance"
    CHAOS = "chaos"


class ResilienceTestType(Enum):
    """Types of resilience tests"""
    NODE_FAILURE = "node_failure"
    NETWORK_PARTITION = "network_partition"
    TASK_OVERLOAD = "task_overload"
    RESOURCE_EXHAUSTION = "resource_exhaustion"
    CASCADING_FAILURE = "cascading_failure"
    BYZANTINE_FAILURE = "byzantine_failure"


@dataclass
class LoadTestConfig:
    """Configuration for load testing"""
    test_name: str = "default_load_test"
    test_type: LoadTestType = LoadTestType.THROUGHPUT
    duration: float = 60.0  # seconds
    concurrent_tasks: int = 100
    tasks_per_second: float = 10.0
    ramp_up_time: float = 10.0  # seconds
    ramp_down_time: float = 10.0  # seconds
    task_complexity: str = "medium"  # low, medium, high
    target_nodes: int = 5
    failure_injection_rate: float = 0.0  # 0.0 to 1.0
    chaos_monkey_enabled: bool = False
    metrics_interval: float = 1.0  # seconds
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'test_name': self.test_name,
            'test_type': self.test_type.value,
            'duration': self.duration,
            'concurrent_tasks': self.concurrent_tasks,
            'tasks_per_second': self.tasks_per_second,
            'ramp_up_time': self.ramp_up_time,
            'ramp_down_time': self.ramp_down_time,
            'task_complexity': self.task_complexity,
            'target_nodes': self.target_nodes,
            'failure_injection_rate': self.failure_injection_rate,
            'chaos_monkey_enabled': self.chaos_monkey_enabled,
            'metrics_interval': self.metrics_interval
        }


@dataclass
class TestMetrics:
    """Metrics collected during testing"""
    timestamp: float = field(default_factory=time.time)
    tasks_submitted: int = 0
    tasks_completed: int = 0
    tasks_failed: int = 0
    average_latency: float = 0.0
    min_latency: float = float('inf')
    max_latency: float = 0.0
    p95_latency: float = 0.0
    p99_latency: float = 0.0
    throughput: float = 0.0  # tasks per second
    error_rate: float = 0.0
    active_nodes: int = 0
    total_cpu_usage: float = 0.0
    total_memory_usage: float = 0.0
    network_latency: float = 0.0
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'timestamp': self.timestamp,
            'tasks_submitted': self.tasks_submitted,
            'tasks_completed': self.tasks_completed,
            'tasks_failed': self.tasks_failed,
            'success_rate': (self.tasks_completed / self.tasks_submitted) if self.tasks_submitted > 0 else 0.0,
            'average_latency': self.average_latency,
            'min_latency': self.min_latency if self.min_latency != float('inf') else 0.0,
            'max_latency': self.max_latency,
            'p95_latency': self.p95_latency,
            'p99_latency': self.p99_latency,
            'throughput': self.throughput,
            'error_rate': self.error_rate,
            'active_nodes': self.active_nodes,
            'total_cpu_usage': self.total_cpu_usage,
            'total_memory_usage': self.total_memory_usage,
            'network_latency': self.network_latency
        }


@dataclass
class TestTask:
    """A test task for load testing"""
    task_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    task_type: str = "load_test"
    complexity: str = "medium"
    payload_size: int = 1024  # bytes
    expected_duration: float = 1.0  # seconds
    created_at: float = field(default_factory=time.time)
    started_at: Optional[float] = None
    completed_at: Optional[float] = None
    failed_at: Optional[float] = None
    assigned_node: Optional[str] = None
    result: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    
    def get_latency(self) -> Optional[float]:
        """Get task latency"""
        if self.started_at and self.completed_at:
            return self.completed_at - self.started_at
        return None
    
    def get_queue_time(self) -> Optional[float]:
        """Get time spent in queue"""
        if self.started_at:
            return self.started_at - self.created_at
        return None


class ChaosMonkey:
    """Chaos engineering for resilience testing"""
    
    def __init__(self):
        self.is_active = False
        self.failure_probability = 0.1  # 10% chance per check
        self.check_interval = 5.0  # seconds
        self.active_failures: Set[str] = set()
        
        # Failure scenarios
        self.failure_scenarios = [
            self._simulate_node_crash,
            self._simulate_network_delay,
            self._simulate_resource_exhaustion,
            self._simulate_task_failure,
            self._simulate_communication_failure
        ]
    
    async def start_chaos(self, mesh_orchestrator, fault_manager):
        """Start chaos monkey"""
        self.is_active = True
        self.mesh_orchestrator = mesh_orchestrator
        self.fault_manager = fault_manager
        
        logger.info("Chaos Monkey activated!")
        
        while self.is_active:
            try:
                if random.random() < self.failure_probability:
                    await self._inject_random_failure()
                
                await asyncio.sleep(self.check_interval)
                
            except Exception as e:
                logger.error(f"Error in chaos monkey: {e}")
                await asyncio.sleep(self.check_interval)
    
    def stop_chaos(self):
        """Stop chaos monkey"""
        self.is_active = False
        logger.info("Chaos Monkey deactivated")
    
    async def _inject_random_failure(self):
        """Inject a random failure"""
        failure_scenario = random.choice(self.failure_scenarios)
        await failure_scenario()
    
    async def _simulate_node_crash(self):
        """Simulate a node crash"""
        if hasattr(self, 'mesh_orchestrator') and self.mesh_orchestrator.nodes:
            node_id = random.choice(list(self.mesh_orchestrator.nodes.keys()))
            
            logger.warning(f"Chaos Monkey: Simulating crash for node {node_id}")
            
            # Mark node as offline
            if node_id in self.mesh_orchestrator.nodes:
                self.mesh_orchestrator.nodes[node_id].status = "offline"
                self.active_failures.add(f"crash_{node_id}")
                
                # Schedule recovery
                asyncio.create_task(self._recover_node_crash(node_id))
    
    async def _simulate_network_delay(self):
        """Simulate network delay"""
        if hasattr(self, 'fault_manager') and self.fault_manager.health_metrics:
            node_id = random.choice(list(self.fault_manager.health_metrics.keys()))
            
            logger.warning(f"Chaos Monkey: Simulating network delay for node {node_id}")
            
            # Increase network latency
            health = self.fault_manager.health_metrics[node_id]
            original_latency = health.network_latency
            health.network_latency += random.uniform(100, 500)  # Add 100-500ms delay
            
            # Schedule recovery
            asyncio.create_task(self._recover_network_delay(node_id, original_latency))
    
    async def _simulate_resource_exhaustion(self):
        """Simulate resource exhaustion"""
        if hasattr(self, 'fault_manager') and self.fault_manager.health_metrics:
            node_id = random.choice(list(self.fault_manager.health_metrics.keys()))
            
            logger.warning(f"Chaos Monkey: Simulating resource exhaustion for node {node_id}")
            
            # Increase resource usage
            health = self.fault_manager.health_metrics[node_id]
            health.cpu_usage = min(1.0, health.cpu_usage + random.uniform(0.3, 0.6))
            health.memory_usage = min(1.0, health.memory_usage + random.uniform(0.2, 0.5))
            health.update_health_score()
            
            # Schedule recovery
            asyncio.create_task(self._recover_resource_exhaustion(node_id))
    
    async def _simulate_task_failure(self):
        """Simulate task failure"""
        logger.warning("Chaos Monkey: Injecting task failure probability")
        
        # This would be handled at the task level in the orchestrator
        # For simulation, we just log it
        self.active_failures.add(f"task_failure_{time.time()}")
    
    async def _simulate_communication_failure(self):
        """Simulate communication failure"""
        if hasattr(self, 'fault_manager') and self.fault_manager.health_metrics:
            node_id = random.choice(list(self.fault_manager.health_metrics.keys()))
            
            logger.warning(f"Chaos Monkey: Simulating communication failure for node {node_id}")
            
            # Increase error rate
            health = self.fault_manager.health_metrics[node_id]
            health.error_rate = min(1.0, health.error_rate + random.uniform(0.2, 0.5))
            health.update_health_score()
            
            # Schedule recovery
            asyncio.create_task(self._recover_communication_failure(node_id))
    
    async def _recover_node_crash(self, node_id: str):
        """Recover from simulated node crash"""
        await asyncio.sleep(random.uniform(30, 60))  # Recovery takes 30-60 seconds
        
        if hasattr(self, 'mesh_orchestrator') and node_id in self.mesh_orchestrator.nodes:
            self.mesh_orchestrator.nodes[node_id].status = "online"
            self.active_failures.discard(f"crash_{node_id}")
            logger.info(f"Chaos Monkey: Node {node_id} recovered from crash")
    
    async def _recover_network_delay(self, node_id: str, original_latency: float):
        """Recover from simulated network delay"""
        await asyncio.sleep(random.uniform(20, 40))  # Recovery takes 20-40 seconds
        
        if hasattr(self, 'fault_manager') and node_id in self.fault_manager.health_metrics:
            health = self.fault_manager.health_metrics[node_id]
            health.network_latency = original_latency
            logger.info(f"Chaos Monkey: Network delay recovered for node {node_id}")
    
    async def _recover_resource_exhaustion(self, node_id: str):
        """Recover from simulated resource exhaustion"""
        await asyncio.sleep(random.uniform(15, 30))  # Recovery takes 15-30 seconds
        
        if hasattr(self, 'fault_manager') and node_id in self.fault_manager.health_metrics:
            health = self.fault_manager.health_metrics[node_id]
            health.cpu_usage *= 0.5  # Reduce resource usage
            health.memory_usage *= 0.6
            health.update_health_score()
            logger.info(f"Chaos Monkey: Resource exhaustion recovered for node {node_id}")
    
    async def _recover_communication_failure(self, node_id: str):
        """Recover from simulated communication failure"""
        await asyncio.sleep(random.uniform(10, 25))  # Recovery takes 10-25 seconds
        
        if hasattr(self, 'fault_manager') and node_id in self.fault_manager.health_metrics:
            health = self.fault_manager.health_metrics[node_id]
            health.error_rate *= 0.3  # Reduce error rate
            health.update_health_score()
            logger.info(f"Chaos Monkey: Communication failure recovered for node {node_id}")


class LoadTestingFramework:
    """Comprehensive load testing framework for the distributed mesh"""
    
    def __init__(self, mesh_orchestrator=None, fault_manager=None, discovery_service=None):
        self.mesh_orchestrator = mesh_orchestrator
        self.fault_manager = fault_manager
        self.discovery_service = discovery_service
        
        self.active_tests: Dict[str, Dict[str, Any]] = {}
        self.test_results: Dict[str, Dict[str, Any]] = {}
        self.chaos_monkey = ChaosMonkey()
        
        # Metrics collection
        self.metrics_history: List[TestMetrics] = []
        self.task_latencies: List[float] = []
        self.executor = ThreadPoolExecutor(max_workers=50)
    
    async def run_load_test(self, config: LoadTestConfig) -> Dict[str, Any]:
        """Run a comprehensive load test"""
        test_id = str(uuid.uuid4())
        test_start_time = time.time()
        
        logger.info(f"Starting load test: {config.test_name} (ID: {test_id})")
        
        # Initialize test state
        self.active_tests[test_id] = {
            'config': config,
            'start_time': test_start_time,
            'status': 'running',
            'tasks': {},
            'metrics': []
        }
        
        try:
            # Start chaos monkey if enabled
            if config.chaos_monkey_enabled:
                asyncio.create_task(self.chaos_monkey.start_chaos(
                    self.mesh_orchestrator, self.fault_manager
                ))
            
            # Start metrics collection
            metrics_task = asyncio.create_task(self._collect_metrics(test_id, config))
            
            # Run the specific test type
            if config.test_type == LoadTestType.THROUGHPUT:
                test_results = await self._run_throughput_test(test_id, config)
            elif config.test_type == LoadTestType.LATENCY:
                test_results = await self._run_latency_test(test_id, config)
            elif config.test_type == LoadTestType.SCALABILITY:
                test_results = await self._run_scalability_test(test_id, config)
            elif config.test_type == LoadTestType.STRESS:
                test_results = await self._run_stress_test(test_id, config)
            elif config.test_type == LoadTestType.ENDURANCE:
                test_results = await self._run_endurance_test(test_id, config)
            elif config.test_type == LoadTestType.CHAOS:
                test_results = await self._run_chaos_test(test_id, config)
            else:
                test_results = await self._run_throughput_test(test_id, config)
            
            # Stop metrics collection
            metrics_task.cancel()
            
            # Stop chaos monkey
            if config.chaos_monkey_enabled:
                self.chaos_monkey.stop_chaos()
            
            # Calculate final metrics
            final_metrics = self._calculate_final_metrics(test_id)
            
            # Store results
            total_duration = time.time() - test_start_time
            test_result = {
                'test_id': test_id,
                'config': config.to_dict(),
                'start_time': test_start_time,
                'duration': total_duration,
                'status': 'completed',
                'metrics': final_metrics,
                'test_specific_results': test_results,
                'recommendations': self._generate_recommendations(final_metrics, config)
            }
            
            self.test_results[test_id] = test_result
            self.active_tests[test_id]['status'] = 'completed'
            
            logger.info(f"Load test completed: {config.test_name} (Duration: {total_duration:.2f}s)")
            return test_result
            
        except Exception as e:
            logger.error(f"Load test failed: {e}")
            self.active_tests[test_id]['status'] = 'failed'
            
            return {
                'test_id': test_id,
                'config': config.to_dict(),
                'status': 'failed',
                'error': str(e),
                'duration': time.time() - test_start_time
            }
    
    async def _run_throughput_test(self, test_id: str, config: LoadTestConfig) -> Dict[str, Any]:
        """Run throughput-focused load test"""
        logger.info(f"Running throughput test for {config.duration} seconds")
        
        tasks_submitted = 0
        tasks_completed = 0
        tasks_failed = 0
        
        # Task submission loop
        async def submit_tasks():
            nonlocal tasks_submitted
            task_interval = 1.0 / config.tasks_per_second
            end_time = time.time() + config.duration
            
            while time.time() < end_time:
                # Create and submit task
                task = self._create_test_task(config.task_complexity)
                
                if self.mesh_orchestrator:
                    try:
                        # Convert to DistributedTask for submission
                        from cognitive_architecture.distributed_mesh.orchestrator import DistributedTask
                        distributed_task = DistributedTask(
                            task_type=task.task_type,
                            payload={'test_data': f'payload_{task.task_id}', 'size': task.payload_size},
                            priority=random.randint(1, 10)
                        )
                        
                        self.mesh_orchestrator.submit_task(distributed_task)
                        self.active_tests[test_id]['tasks'][task.task_id] = task
                        tasks_submitted += 1
                        
                    except Exception as e:
                        logger.error(f"Failed to submit task: {e}")
                        tasks_failed += 1
                
                await asyncio.sleep(task_interval)
        
        # Run task submission
        await submit_tasks()
        
        # Wait for remaining tasks to complete
        await asyncio.sleep(10)  # Grace period
        
        return {
            'tasks_submitted': tasks_submitted,
            'tasks_completed': tasks_completed,
            'tasks_failed': tasks_failed,
            'effective_throughput': tasks_submitted / config.duration
        }
    
    async def _run_latency_test(self, test_id: str, config: LoadTestConfig) -> Dict[str, Any]:
        """Run latency-focused load test"""
        logger.info("Running latency test with focus on response time")
        
        latencies = []
        
        for i in range(config.concurrent_tasks):
            task = self._create_test_task(config.task_complexity)
            start_time = time.time()
            
            # Simulate task processing
            await asyncio.sleep(random.uniform(0.1, 2.0))
            
            latency = time.time() - start_time
            latencies.append(latency)
            
            if i % 10 == 0:  # Progress update
                await asyncio.sleep(0.01)
        
        # Ensure we have latencies to avoid errors
        if not latencies:
            latencies = [0.0]
        
        return {
            'latencies': latencies,
            'avg_latency': statistics.mean(latencies),
            'min_latency': min(latencies),
            'max_latency': max(latencies),
            'p95_latency': np.percentile(latencies, 95) if len(latencies) > 1 else latencies[0],
            'p99_latency': np.percentile(latencies, 99) if len(latencies) > 1 else latencies[0]
        }
    
    async def _run_scalability_test(self, test_id: str, config: LoadTestConfig) -> Dict[str, Any]:
        """Run scalability test with increasing load"""
        logger.info("Running scalability test with increasing load")
        
        scalability_results = []
        
        # Test with increasing number of concurrent tasks
        for scale_factor in [0.5, 1.0, 2.0, 4.0, 8.0]:
            concurrent_tasks = int(config.concurrent_tasks * scale_factor)
            
            logger.info(f"Testing with {concurrent_tasks} concurrent tasks")
            
            start_time = time.time()
            latencies = []
            
            # Run tasks concurrently
            tasks = []
            for i in range(concurrent_tasks):
                task = asyncio.create_task(self._simulate_task_execution())
                tasks.append(task)
            
            # Wait for all tasks to complete
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Calculate metrics for this scale
            successful_results = [r for r in results if not isinstance(r, Exception)]
            failed_results = [r for r in results if isinstance(r, Exception)]
            
            duration = time.time() - start_time
            throughput = len(successful_results) / duration
            
            scalability_results.append({
                'scale_factor': scale_factor,
                'concurrent_tasks': concurrent_tasks,
                'successful_tasks': len(successful_results),
                'failed_tasks': len(failed_results),
                'throughput': throughput,
                'duration': duration
            })
            
            # Brief pause between scale tests
            await asyncio.sleep(2.0)
        
        return {'scalability_results': scalability_results}
    
    async def _run_stress_test(self, test_id: str, config: LoadTestConfig) -> Dict[str, Any]:
        """Run stress test to find breaking point"""
        logger.info("Running stress test to find system limits")
        
        stress_results = []
        current_load = config.tasks_per_second
        max_successful_load = 0
        breaking_point_found = False
        
        while not breaking_point_found and current_load < config.tasks_per_second * 10:
            logger.info(f"Testing with load: {current_load} tasks/second")
            
            # Run stress test at current load
            start_time = time.time()
            tasks_submitted = 0
            tasks_failed = 0
            
            # Submit tasks for 30 seconds
            test_duration = 30.0
            task_interval = 1.0 / current_load
            end_time = start_time + test_duration
            
            while time.time() < end_time:
                try:
                    await self._simulate_task_execution()
                    tasks_submitted += 1
                except Exception:
                    tasks_failed += 1
                
                await asyncio.sleep(task_interval)
            
            duration = time.time() - start_time
            success_rate = (tasks_submitted - tasks_failed) / tasks_submitted if tasks_submitted > 0 else 0
            
            stress_results.append({
                'load': current_load,
                'tasks_submitted': tasks_submitted,
                'tasks_failed': tasks_failed,
                'success_rate': success_rate,
                'duration': duration
            })
            
            # Check if we found the breaking point
            if success_rate < 0.95:  # Less than 95% success rate
                breaking_point_found = True
                logger.info(f"Breaking point found at {current_load} tasks/second")
            else:
                max_successful_load = current_load
                current_load *= 1.5  # Increase load by 50%
        
        return {
            'stress_results': stress_results,
            'max_successful_load': max_successful_load,
            'breaking_point': current_load if breaking_point_found else None
        }
    
    async def _run_endurance_test(self, test_id: str, config: LoadTestConfig) -> Dict[str, Any]:
        """Run endurance test for sustained load"""
        logger.info(f"Running endurance test for {config.duration} seconds")
        
        # Run sustained load for the full duration
        start_time = time.time()
        interval_results = []
        
        # Break test into 1-minute intervals
        interval_duration = 60.0
        intervals = int(config.duration / interval_duration)
        
        for interval in range(intervals):
            interval_start = time.time()
            
            # Run load for this interval
            tasks_in_interval = int(config.tasks_per_second * interval_duration)
            interval_latencies = []
            
            for i in range(tasks_in_interval):
                task_start = time.time()
                await self._simulate_task_execution()
                latency = time.time() - task_start
                interval_latencies.append(latency)
                
                # Control task rate
                expected_time = interval_start + (i / config.tasks_per_second)
                wait_time = expected_time - time.time()
                if wait_time > 0:
                    await asyncio.sleep(wait_time)
            
            interval_duration_actual = time.time() - interval_start
            
            interval_results.append({
                'interval': interval + 1,
                'tasks_completed': len(interval_latencies),
                'avg_latency': statistics.mean(interval_latencies) if interval_latencies else 0,
                'max_latency': max(interval_latencies) if interval_latencies else 0,
                'throughput': len(interval_latencies) / interval_duration_actual,
                'duration': interval_duration_actual
            })
            
            logger.info(f"Completed interval {interval + 1}/{intervals}")
        
        return {
            'endurance_results': interval_results,
            'total_duration': time.time() - start_time
        }
    
    async def _run_chaos_test(self, test_id: str, config: LoadTestConfig) -> Dict[str, Any]:
        """Run chaos test with failure injection"""
        logger.info("Running chaos test with failure injection")
        
        # Enable chaos monkey
        chaos_task = asyncio.create_task(self.chaos_monkey.start_chaos(
            self.mesh_orchestrator, self.fault_manager
        ))
        
        # Run normal load test while chaos is active
        normal_results = await self._run_throughput_test(test_id, config)
        
        # Stop chaos monkey
        self.chaos_monkey.stop_chaos()
        chaos_task.cancel()
        
        return {
            'chaos_results': normal_results,
            'failures_injected': len(self.chaos_monkey.active_failures),
            'active_failures': list(self.chaos_monkey.active_failures)
        }
    
    def _create_test_task(self, complexity: str) -> TestTask:
        """Create a test task with specified complexity"""
        complexity_configs = {
            'low': {'payload_size': 512, 'expected_duration': 0.5},
            'medium': {'payload_size': 1024, 'expected_duration': 1.0},
            'high': {'payload_size': 2048, 'expected_duration': 2.0}
        }
        
        config = complexity_configs.get(complexity, complexity_configs['medium'])
        
        return TestTask(
            task_type=f"load_test_{complexity}",
            complexity=complexity,
            payload_size=config['payload_size'],
            expected_duration=config['expected_duration']
        )
    
    async def _simulate_task_execution(self) -> float:
        """Simulate task execution and return latency"""
        start_time = time.time()
        
        # Simulate processing time with some randomness
        processing_time = random.uniform(0.1, 1.0)
        await asyncio.sleep(processing_time)
        
        # Simulate occasional failures (5% chance)
        if random.random() < 0.05:
            raise Exception("Simulated task failure")
        
        return time.time() - start_time
    
    async def _collect_metrics(self, test_id: str, config: LoadTestConfig):
        """Collect metrics during test execution"""
        while test_id in self.active_tests and self.active_tests[test_id]['status'] == 'running':
            try:
                metrics = TestMetrics()
                
                # Collect mesh metrics
                if self.mesh_orchestrator:
                    mesh_status = self.mesh_orchestrator.get_mesh_status()
                    metrics.active_nodes = len(mesh_status['nodes'])
                    metrics.tasks_submitted = mesh_status['tasks'].get('pending', 0) + mesh_status['tasks'].get('completed', 0)
                    metrics.tasks_completed = mesh_status['tasks'].get('completed', 0)
                    metrics.tasks_failed = mesh_status['tasks'].get('failed', 0)
                
                # Collect health metrics
                if self.fault_manager:
                    health_summary = self.fault_manager.get_health_summary()
                    if health_summary['nodes']:
                        cpu_usages = [node['cpu_usage'] for node in health_summary['nodes'].values()]
                        memory_usages = [node['memory_usage'] for node in health_summary['nodes'].values()]
                        network_latencies = [node['network_latency'] for node in health_summary['nodes'].values()]
                        
                        metrics.total_cpu_usage = statistics.mean(cpu_usages) if cpu_usages else 0
                        metrics.total_memory_usage = statistics.mean(memory_usages) if memory_usages else 0
                        metrics.network_latency = statistics.mean(network_latencies) if network_latencies else 0
                
                # Calculate derived metrics
                if metrics.tasks_submitted > 0:
                    metrics.error_rate = metrics.tasks_failed / metrics.tasks_submitted
                
                self.active_tests[test_id]['metrics'].append(metrics)
                
                await asyncio.sleep(config.metrics_interval)
                
            except Exception as e:
                logger.error(f"Error collecting metrics: {e}")
                await asyncio.sleep(config.metrics_interval)
    
    def _calculate_final_metrics(self, test_id: str) -> Dict[str, Any]:
        """Calculate final test metrics"""
        if test_id not in self.active_tests:
            return {}
        
        metrics_list = self.active_tests[test_id]['metrics']
        if not metrics_list:
            return {
                'total_tasks_submitted': 0,
                'total_tasks_completed': 0,
                'total_tasks_failed': 0,
                'success_rate': 0.0,
                'error_rate': 0.0,
                'average_cpu_usage': 0.0,
                'average_memory_usage': 0.0,
                'average_network_latency': 0.0,
                'metrics_count': 0
            }
        
        # Aggregate metrics
        total_tasks_submitted = max(m.tasks_submitted for m in metrics_list) if metrics_list else 0
        total_tasks_completed = max(m.tasks_completed for m in metrics_list) if metrics_list else 0
        total_tasks_failed = max(m.tasks_failed for m in metrics_list) if metrics_list else 0
        
        cpu_values = [m.total_cpu_usage for m in metrics_list if m.total_cpu_usage > 0]
        memory_values = [m.total_memory_usage for m in metrics_list if m.total_memory_usage > 0]
        latency_values = [m.network_latency for m in metrics_list if m.network_latency > 0]
        
        avg_cpu_usage = statistics.mean(cpu_values) if cpu_values else 0.0
        avg_memory_usage = statistics.mean(memory_values) if memory_values else 0.0
        avg_network_latency = statistics.mean(latency_values) if latency_values else 0.0
        
        success_rate = (total_tasks_completed / total_tasks_submitted) if total_tasks_submitted > 0 else 0.0
        
        return {
            'total_tasks_submitted': total_tasks_submitted,
            'total_tasks_completed': total_tasks_completed,
            'total_tasks_failed': total_tasks_failed,
            'success_rate': success_rate,
            'error_rate': 1.0 - success_rate,
            'average_cpu_usage': avg_cpu_usage,
            'average_memory_usage': avg_memory_usage,
            'average_network_latency': avg_network_latency,
            'metrics_count': len(metrics_list)
        }
    
    def _generate_recommendations(self, metrics: Dict[str, Any], config: LoadTestConfig) -> List[str]:
        """Generate recommendations based on test results"""
        recommendations = []
        
        success_rate = metrics.get('success_rate', 0)
        avg_cpu = metrics.get('average_cpu_usage', 0)
        avg_memory = metrics.get('average_memory_usage', 0)
        avg_latency = metrics.get('average_network_latency', 0)
        
        if success_rate < 0.95:
            recommendations.append(f"Low success rate ({success_rate:.1%}). Consider reducing load or adding more nodes.")
        
        if avg_cpu > 0.8:
            recommendations.append(f"High CPU usage ({avg_cpu:.1%}). Consider scaling out or optimizing task processing.")
        
        if avg_memory > 0.85:
            recommendations.append(f"High memory usage ({avg_memory:.1%}). Monitor for memory leaks or increase node memory.")
        
        if avg_latency > 500:  # 500ms
            recommendations.append(f"High network latency ({avg_latency:.1f}ms). Check network configuration.")
        
        if config.test_type == LoadTestType.STRESS and success_rate > 0.98:
            recommendations.append("System handled stress test well. Consider increasing load for more challenging test.")
        
        if not recommendations:
            recommendations.append("System performed well within expected parameters.")
        
        return recommendations
    
    def get_test_summary(self) -> Dict[str, Any]:
        """Get summary of all tests"""
        return {
            'active_tests': len(self.active_tests),
            'completed_tests': len(self.test_results),
            'test_results': list(self.test_results.keys()),
            'chaos_monkey_status': {
                'is_active': self.chaos_monkey.is_active,
                'active_failures': len(self.chaos_monkey.active_failures)
            }
        }


# Global load testing framework instance
load_testing_framework = LoadTestingFramework()

# Predefined test configurations
PREDEFINED_CONFIGS = {
    'quick_throughput': LoadTestConfig(
        test_name="Quick Throughput Test",
        test_type=LoadTestType.THROUGHPUT,
        duration=30.0,
        concurrent_tasks=50,
        tasks_per_second=5.0
    ),
    'latency_benchmark': LoadTestConfig(
        test_name="Latency Benchmark",
        test_type=LoadTestType.LATENCY,
        duration=60.0,
        concurrent_tasks=20,
        tasks_per_second=2.0
    ),
    'scalability_test': LoadTestConfig(
        test_name="Scalability Test",
        test_type=LoadTestType.SCALABILITY,
        duration=120.0,
        concurrent_tasks=100,
        tasks_per_second=10.0
    ),
    'stress_test': LoadTestConfig(
        test_name="Stress Test",
        test_type=LoadTestType.STRESS,
        duration=180.0,
        concurrent_tasks=200,
        tasks_per_second=20.0
    ),
    'chaos_test': LoadTestConfig(
        test_name="Chaos Engineering Test",
        test_type=LoadTestType.CHAOS,
        duration=300.0,
        concurrent_tasks=100,
        tasks_per_second=10.0,
        chaos_monkey_enabled=True,
        failure_injection_rate=0.1
    )
}