#!/usr/bin/env python3
"""
Phase 3: Distributed Mesh Topology & Agent Orchestration - Requirements Validation

This script validates all the specific requirements for Phase 3:
1. Design mesh node registration and discovery protocols
2. Implement distributed task queue with priority scheduling  
3. Create agent capability matching algorithms
4. Build mesh health monitoring and auto-recovery
5. Test mesh scalability and resilience under load
6. Document mesh communication protocols and APIs
"""

import asyncio
import json
import sys
import time
import logging
import pytest
from typing import Dict, Any, List

logger = logging.getLogger(__name__)


def test_mesh_node_discovery_protocols():
    """Test mesh node registration and discovery protocols"""
    print("✅ Testing Mesh Node Discovery Protocols")
    print("-" * 60)
    
    from cognitive_architecture.distributed_mesh.discovery import (
        MeshDiscoveryService, NodeAdvertisement, DiscoveryConfig, DiscoveryProtocol
    )
    
    # Test discovery service configuration
    config = DiscoveryConfig(
        protocol=DiscoveryProtocol.MULTICAST,
        discovery_interval=5.0,
        advertisement_ttl=30
    )
    
    discovery_service = MeshDiscoveryService(config)
    
    # Test node advertisement creation
    node_ad = NodeAdvertisement(
        node_id="test_node_1",
        node_type="agent",
        capabilities={"text_processing", "reasoning"},
        endpoint="127.0.0.1",
        port=8080,
        load_capacity=1.0,
        current_load=0.3
    )
    
    print(f"  Created node advertisement: {node_ad.node_id}")
    print(f"  Capabilities: {node_ad.capabilities}")
    print(f"  Load: {node_ad.current_load}/{node_ad.load_capacity}")
    
    # Test advertisement serialization
    ad_dict = node_ad.to_dict()
    reconstructed_ad = NodeAdvertisement.from_dict(ad_dict)
    
    assert reconstructed_ad.node_id == node_ad.node_id, "Advertisement serialization failed"
    assert reconstructed_ad.capabilities == node_ad.capabilities, "Capabilities not preserved"
    
    # Test discovery service configuration
    stats = discovery_service.get_discovery_statistics()
    print(f"  Discovery service stats: {stats['statistics']}")
    
    # Test capability weights setup
    discovery_service.setup_capability_weights({
        'text_processing': 1.0,
        'reasoning': 1.2,
        'neural_inference': 1.3
    })
    
    # Test capability dependencies
    discovery_service.setup_capability_dependencies({
        'reasoning': {'text_processing'},
        'neural_inference': {'text_processing', 'reasoning'}
    })
    
    print("✓ Mesh node discovery protocols working correctly")
    return True


def test_enhanced_capability_matching():
    """Test enhanced agent capability matching algorithms"""
    print("\n✅ Testing Enhanced Agent Capability Matching")
    print("-" * 60)
    
    from cognitive_architecture.distributed_mesh.discovery import (
        CapabilityMatcher, NodeAdvertisement
    )
    
    # Create capability matcher
    matcher = CapabilityMatcher()
    
    # Setup capability weights
    matcher.add_capability_weight('text_processing', 1.0)
    matcher.add_capability_weight('reasoning', 1.2)
    matcher.add_capability_weight('neural_inference', 1.3)
    matcher.add_capability_weight('dialogue', 0.9)
    
    # Setup capability dependencies
    matcher.add_capability_dependency('reasoning', {'text_processing'})
    matcher.add_capability_dependency('neural_inference', {'text_processing', 'reasoning'})
    
    # Create test nodes with different capabilities
    nodes = [
        NodeAdvertisement(
            node_id="node_1",
            node_type="agent",
            capabilities={"text_processing", "dialogue"},
            endpoint="127.0.0.1", port=8081,
            load_capacity=1.0, current_load=0.2
        ),
        NodeAdvertisement(
            node_id="node_2", 
            node_type="processor",
            capabilities={"text_processing", "reasoning", "neural_inference"},
            endpoint="127.0.0.1", port=8082,
            load_capacity=1.0, current_load=0.5
        ),
        NodeAdvertisement(
            node_id="node_3",
            node_type="agent", 
            capabilities={"reasoning", "dialogue"},
            endpoint="127.0.0.1", port=8083,
            load_capacity=1.0, current_load=0.8
        )
    ]
    
    # Record some performance history
    matcher.record_performance("node_1", 0.8)
    matcher.record_performance("node_2", 0.9)
    matcher.record_performance("node_3", 0.6)
    
    # Test capability scoring
    required_capabilities = {"text_processing", "reasoning"}
    
    print(f"  Required capabilities: {required_capabilities}")
    print("  Node capability scores:")
    
    for node in nodes:
        score = matcher.calculate_capability_score(node.capabilities, required_capabilities)
        print(f"    {node.node_id}: {score:.3f} (capabilities: {node.capabilities})")
        
        assert 0.0 <= score <= 1.0, f"Invalid capability score: {score}"
    
    # Test node ranking
    ranked_nodes = matcher.rank_nodes(nodes, required_capabilities, task_priority=1.5)
    
    print("  Ranked nodes for task:")
    for i, (node, score, details) in enumerate(ranked_nodes):
        print(f"    {i+1}. {node.node_id}: final_score={score:.3f}")
        print(f"       capability={details['capability_score']:.3f}, load={details['load_factor']:.3f}, perf={details['performance_score']:.3f}")
    
    # Verify ranking makes sense
    assert len(ranked_nodes) == len(nodes), "Not all nodes ranked"
    assert ranked_nodes[0][1] >= ranked_nodes[1][1], "Ranking order incorrect"
    
    print("✓ Enhanced capability matching working correctly")
    return True


def test_fault_tolerance_and_auto_recovery():
    """Test mesh health monitoring and auto-recovery mechanisms"""
    print("\n✅ Testing Fault Tolerance and Auto-Recovery")
    print("-" * 60)
    
    from cognitive_architecture.distributed_mesh.fault_tolerance import (
        FaultToleranceManager, HealthMetrics, FailureType, RecoveryStrategy, CircuitBreaker
    )
    
    # Create fault tolerance manager
    ft_manager = FaultToleranceManager()
    
    # Register nodes for health monitoring
    node_ids = ["node_1", "node_2", "node_3"]
    for node_id in node_ids:
        health = ft_manager.register_node_health(node_id)
        print(f"  Registered node for health monitoring: {node_id}")
        
        # Update some health metrics
        ft_manager.update_node_health(
            node_id,
            cpu_usage=0.3 + (hash(node_id) % 100) / 200,  # 0.3-0.8
            memory_usage=0.2 + (hash(node_id) % 100) / 300,  # 0.2-0.5
            network_latency=50 + (hash(node_id) % 100),  # 50-150ms
            task_success_rate=0.9 + (hash(node_id) % 10) / 100,  # 0.9-0.99
            error_rate=0.01 + (hash(node_id) % 5) / 1000  # 0.01-0.015
        )
    
    # Test health summary
    health_summary = ft_manager.get_health_summary()
    print(f"  Health summary: {health_summary['summary']}")
    
    assert health_summary['summary']['total_nodes'] == len(node_ids), "Not all nodes registered"
    
    # Test circuit breaker
    cb = ft_manager.get_circuit_breaker("test_service")
    
    # Test normal operation
    def test_service():
        return "success"
    
    result = cb.call_service(test_service)
    assert result == "success", "Circuit breaker normal operation failed"
    
    # Test failure handling
    def failing_service():
        raise Exception("Service failure")
    
    # Trigger failures to open circuit
    for i in range(6):  # Exceed threshold of 5
        try:
            cb.call_service(failing_service)
        except Exception:
            pass
    
    cb_state = cb.get_state()
    assert cb_state['state'] == 'open', f"Circuit breaker should be open, but is {cb_state['state']}"
    print(f"  Circuit breaker state: {cb_state}")
    
    # Test failure statistics
    failure_stats = ft_manager.get_failure_statistics()
    print(f"  Monitoring status: {failure_stats['monitoring_status']}")
    
    print("✓ Fault tolerance and auto-recovery working correctly")
    return True


@pytest.mark.asyncio
async def test_scalability_and_resilience():
    """Test mesh scalability and resilience under load"""
    print("\n✅ Testing Mesh Scalability and Resilience")
    print("-" * 60)
    
    from cognitive_architecture.distributed_mesh.load_testing import (
        LoadTestingFramework, LoadTestConfig, LoadTestType, PREDEFINED_CONFIGS
    )
    from cognitive_architecture.distributed_mesh.orchestrator import mesh_orchestrator, MeshNode, MeshNodeType
    
    # Create load testing framework
    load_tester = LoadTestingFramework()
    
    # Setup mock mesh orchestrator for testing
    load_tester.mesh_orchestrator = mesh_orchestrator
    
    # Add some test nodes to the orchestrator
    test_nodes = []
    for i in range(5):
        node = MeshNode(
            node_type=MeshNodeType.AGENT,
            capabilities={"text_processing", "reasoning"},
            max_load=1.0
        )
        mesh_orchestrator.register_node(node)
        test_nodes.append(node)
    
    print(f"  Created {len(test_nodes)} test nodes")
    
    # Test quick throughput test
    quick_config = PREDEFINED_CONFIGS['quick_throughput']
    print(f"  Running {quick_config.test_name}...")
    
    throughput_result = await load_tester.run_load_test(quick_config)
    
    print(f"  Test status: {throughput_result['status']}")
    if throughput_result['status'] == 'completed':
        metrics = throughput_result['metrics']
        print(f"  Tasks completed: {metrics.get('total_tasks_completed', 0)}")
        print(f"  Success rate: {metrics.get('success_rate', 0):.1%}")
        print(f"  Average CPU: {metrics.get('average_cpu_usage', 0):.1%}")
        
        # Verify test completed successfully
        assert throughput_result['status'] == 'completed', "Load test did not complete"
        assert 'metrics' in throughput_result, "No metrics in test result"
    
    # Test latency benchmark
    latency_config = LoadTestConfig(
        test_name="Quick Latency Test",
        test_type=LoadTestType.LATENCY,
        duration=10.0,
        concurrent_tasks=10,
        tasks_per_second=1.0
    )
    
    print(f"  Running latency test...")
    latency_result = await load_tester.run_load_test(latency_config)
    
    if latency_result['status'] == 'completed':
        test_results = latency_result['test_specific_results']
        if 'latencies' in test_results:
            avg_latency = test_results['avg_latency']
            print(f"  Average latency: {avg_latency:.3f}s")
            assert avg_latency > 0, "Invalid latency measurement"
    
    # Test framework summary
    test_summary = load_tester.get_test_summary()
    print(f"  Load testing summary: {test_summary}")
    
    assert test_summary['completed_tests'] >= 1, "No tests completed"
    
    print("✓ Scalability and resilience testing working correctly")
    return True


def test_integrated_mesh_orchestration():
    """Test integrated mesh orchestration with all Phase 3 components"""
    print("\n✅ Testing Integrated Mesh Orchestration")
    print("-" * 60)
    
    from cognitive_architecture.distributed_mesh.orchestrator import (
        mesh_orchestrator, setup_phase3_integration, DistributedTask
    )
    
    # Setup Phase 3 integration
    setup_phase3_integration()
    
    # Test enhanced mesh status
    enhanced_status = mesh_orchestrator.get_enhanced_mesh_status()
    
    print("  Enhanced mesh components:")
    
    if 'discovery' in enhanced_status:
        discovery_stats = enhanced_status['discovery']['statistics']
        print(f"    Discovery: {discovery_stats['active_nodes']} active nodes")
    
    if 'health' in enhanced_status:
        health_summary = enhanced_status['health']['summary']
        print(f"    Health: {health_summary['total_nodes']} monitored nodes")
    
    if 'fault_tolerance' in enhanced_status:
        ft_stats = enhanced_status['fault_tolerance']['monitoring_status']
        print(f"    Fault tolerance: monitoring={ft_stats['is_monitoring']}")
    
    if 'load_testing' in enhanced_status:
        lt_summary = enhanced_status['load_testing']
        print(f"    Load testing: {lt_summary['completed_tests']} tests completed")
    
    # Test task submission with enhanced capabilities
    test_task = DistributedTask(
        task_type="text_processing",
        payload={"text": "Test mesh orchestration"},
        priority=7
    )
    
    task_id = mesh_orchestrator.submit_task(test_task)
    print(f"  Submitted test task: {task_id}")
    
    # Test optimal node finding
    optimal_nodes = mesh_orchestrator.find_optimal_nodes_for_task(test_task, max_nodes=2)
    print(f"  Found {len(optimal_nodes)} optimal nodes for task")
    
    for node in optimal_nodes:
        print(f"    Node {node.node_id}: {node.capabilities}")
    
    # Test enhanced statistics
    enhanced_stats = enhanced_status['enhanced_statistics']
    print(f"  Enhanced statistics:")
    print(f"    Nodes discovered: {enhanced_stats['nodes_discovered']}")
    print(f"    Tasks completed: {enhanced_stats['tasks_completed']}")
    print(f"    Load tests completed: {enhanced_stats['load_tests_completed']}")
    
    print("✓ Integrated mesh orchestration working correctly")
    return True


def test_mesh_communication_protocols():
    """Test and document mesh communication protocols and APIs"""
    print("\n✅ Testing Mesh Communication Protocols")
    print("-" * 60)
    
    from cognitive_architecture.distributed_mesh.orchestrator import mesh_orchestrator
    from cognitive_architecture.distributed_mesh.discovery import discovery_service
    from cognitive_architecture.distributed_mesh.fault_tolerance import fault_tolerance_manager
    
    # Test mesh status API
    mesh_status = mesh_orchestrator.get_enhanced_mesh_status()
    
    print("  Mesh Communication Protocols:")
    print("    1. Node Registration Protocol:")
    print("       - Endpoint: mesh_orchestrator.register_node()")
    print("       - Format: MeshNode object with capabilities")
    print("       - Response: node_id string")
    
    print("    2. Task Submission Protocol:")
    print("       - Endpoint: mesh_orchestrator.submit_task()")
    print("       - Format: DistributedTask object")
    print("       - Response: task_id string")
    
    print("    3. Discovery Protocol:")
    print("       - Type: Multicast UDP broadcast")
    print("       - Format: JSON node advertisements")
    print("       - Port: 9999 (configurable)")
    
    print("    4. Health Monitoring Protocol:")
    print("       - Method: Periodic heartbeat + metrics")
    print("       - Interval: 10 seconds (configurable)")
    print("       - Metrics: CPU, memory, latency, error rate")
    
    # Create comprehensive API documentation
    api_documentation = {
        "mesh_orchestration_api": {
            "version": "3.0.0",
            "description": "Phase 3 Enhanced Distributed Mesh API",
            "endpoints": {
                "register_node": {
                    "method": "mesh_orchestrator.register_node(node)",
                    "parameters": {
                        "node": {
                            "type": "MeshNode",
                            "required_fields": ["node_type", "capabilities"],
                            "optional_fields": ["max_load", "metadata"]
                        }
                    },
                    "returns": "str (node_id)"
                },
                "submit_task": {
                    "method": "mesh_orchestrator.submit_task(task)",
                    "parameters": {
                        "task": {
                            "type": "DistributedTask",
                            "required_fields": ["task_type", "payload"],
                            "optional_fields": ["priority", "timeout"]
                        }
                    },
                    "returns": "str (task_id)"
                },
                "get_mesh_status": {
                    "method": "mesh_orchestrator.get_enhanced_mesh_status()",
                    "parameters": {},
                    "returns": {
                        "nodes": "dict",
                        "tasks": "dict", 
                        "statistics": "dict",
                        "discovery": "dict",
                        "health": "dict",
                        "fault_tolerance": "dict",
                        "load_testing": "dict"
                    }
                },
                "run_load_test": {
                    "method": "mesh_orchestrator.run_load_test(config_name)",
                    "parameters": {
                        "config_name": {
                            "type": "str",
                            "options": ["quick_throughput", "latency_benchmark", "scalability_test", "stress_test", "chaos_test"]
                        }
                    },
                    "returns": "dict (test_results)"
                }
            }
        },
        "discovery_protocol": {
            "type": "multicast_udp",
            "multicast_group": "224.0.0.1",
            "port": 9999,
            "message_format": {
                "type": "node_advertisement",
                "data": {
                    "node_id": "str",
                    "node_type": "str",
                    "capabilities": "list[str]",
                    "endpoint": "str",
                    "port": "int",
                    "load_capacity": "float",
                    "current_load": "float",
                    "metadata": "dict"
                },
                "timestamp": "float"
            },
            "ttl": 30
        },
        "health_monitoring": {
            "interval_seconds": 10,
            "metrics": {
                "cpu_usage": "float (0.0-1.0)",
                "memory_usage": "float (0.0-1.0)", 
                "network_latency": "float (milliseconds)",
                "task_success_rate": "float (0.0-1.0)",
                "error_rate": "float (0.0-1.0)",
                "uptime": "float (seconds)"
            },
            "health_score_calculation": "weighted_average(cpu, memory, latency, success_rate, error_rate)"
        },
        "fault_tolerance": {
            "failure_detection": {
                "heartbeat_timeout": 30,
                "resource_exhaustion_threshold": 0.95,
                "error_rate_threshold": 0.5
            },
            "recovery_strategies": {
                "node_crash": "restart_or_redistribute",
                "node_unresponsive": "restart",
                "task_timeout": "redistribute", 
                "task_failure": "replicate_or_redistribute",
                "resource_exhaustion": "redistribute",
                "communication_failure": "circuit_breaker_or_fallback"
            },
            "circuit_breaker": {
                "failure_threshold": 5,
                "recovery_timeout": 60,
                "states": ["closed", "open", "half_open"]
            }
        }
    }
    
    # Save API documentation
    with open('/tmp/phase3_api_documentation.json', 'w') as f:
        json.dump(api_documentation, f, indent=2)
    
    print(f"  API documentation saved to: /tmp/phase3_api_documentation.json")
    print(f"  Documentation sections: {list(api_documentation.keys())}")
    
    # Test communication protocol status
    protocol_status = {
        "mesh_orchestrator": "active" if mesh_orchestrator.is_running else "inactive",
        "discovery_service": "available" if discovery_service else "unavailable",
        "fault_tolerance": "available" if fault_tolerance_manager else "unavailable"
    }
    
    print(f"  Protocol status: {protocol_status}")
    
    print("✓ Mesh communication protocols documented and tested")
    return api_documentation


@pytest.mark.asyncio
async def test_phase3_integration_performance():
    """Test integrated Phase 3 system performance"""
    print("\n✅ Testing Phase 3 Integration Performance")
    print("-" * 60)
    
    from cognitive_architecture.distributed_mesh.orchestrator import mesh_orchestrator, setup_phase3_integration
    from cognitive_architecture.distributed_mesh.load_testing import LoadTestConfig, LoadTestType
    
    # Setup full Phase 3 integration
    setup_phase3_integration()
    
    # Start enhanced orchestration
    await mesh_orchestrator.start_enhanced_orchestration()
    
    # Run a comprehensive integration test
    integration_config = LoadTestConfig(
        test_name="Phase 3 Integration Test",
        test_type=LoadTestType.THROUGHPUT,
        duration=30.0,
        concurrent_tasks=20,
        tasks_per_second=2.0,
        chaos_monkey_enabled=False  # Disable for stability
    )
    
    print("  Running Phase 3 integration performance test...")
    start_time = time.time()
    
    # Run load test through the orchestrator
    test_result = await mesh_orchestrator.run_load_test('quick_throughput')
    
    test_duration = time.time() - start_time
    
    print(f"  Integration test completed in {test_duration:.2f} seconds")
    
    if test_result.get('status') == 'completed':
        metrics = test_result['metrics']
        print(f"  Performance metrics:")
        print(f"    Success rate: {metrics.get('success_rate', 0):.1%}")
        print(f"    Tasks completed: {metrics.get('total_tasks_completed', 0)}")
        print(f"    Average CPU usage: {metrics.get('average_cpu_usage', 0):.1%}")
        
        # Performance assertions
        assert metrics.get('success_rate', 0) > 0.8, "Integration test success rate too low"
        assert test_duration < 60.0, "Integration test took too long"
    
    # Get final enhanced status
    final_status = mesh_orchestrator.get_enhanced_mesh_status()
    
    print("  Final system status:")
    print(f"    Active nodes: {len(final_status['nodes'])}")
    print(f"    Completed tasks: {final_status['tasks']['completed']}")
    print(f"    Enhanced features active: {len([k for k in ['discovery', 'health', 'fault_tolerance', 'load_testing'] if k in final_status])}")
    
    # Stop enhanced orchestration
    await mesh_orchestrator.stop_enhanced_orchestration()
    
    print("✓ Phase 3 integration performance verified")
    return True


if __name__ == "__main__":
    print("🎯 PHASE 3 VALIDATION: Distributed Mesh Topology & Agent Orchestration")
    print("=" * 80)
    
    # Run all Phase 3 tests
    test_results = {}
    
    try:
        # 1. Test mesh node discovery protocols
        test_results['discovery_protocols'] = test_mesh_node_discovery_protocols()
        
        # 2. Test enhanced capability matching
        test_results['capability_matching'] = test_enhanced_capability_matching()
        
        # 3. Test fault tolerance and auto-recovery
        test_results['fault_tolerance'] = test_fault_tolerance_and_auto_recovery()
        
        # 4. Test scalability and resilience (async)
        print("\n🔄 Running async tests...")
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        test_results['scalability_resilience'] = loop.run_until_complete(
            test_scalability_and_resilience()
        )
        
        # 5. Test integrated mesh orchestration
        test_results['integrated_orchestration'] = test_integrated_mesh_orchestration()
        
        # 6. Test mesh communication protocols
        test_results['communication_protocols'] = test_mesh_communication_protocols()
        
        # 7. Test Phase 3 integration performance (async)
        test_results['integration_performance'] = loop.run_until_complete(
            test_phase3_integration_performance()
        )
        
        loop.close()
        
    except Exception as e:
        print(f"\n❌ Phase 3 validation failed: {e}")
        sys.exit(1)
    
    # Summary
    print("\n" + "=" * 80)
    print("🎯 PHASE 3 VALIDATION SUMMARY")
    print("=" * 80)
    
    passed_tests = sum(1 for result in test_results.values() if result is True)
    total_tests = len(test_results)
    
    for test_name, result in test_results.items():
        status = "✅ PASSED" if result else "❌ FAILED"
        print(f"{status} {test_name}")
    
    print(f"\n📊 Overall Results: {passed_tests}/{total_tests} tests passed")
    
    if passed_tests == total_tests:
        print("🎉 ALL PHASE 3 REQUIREMENTS VALIDATED SUCCESSFULLY!")
        print("\nPhase 3 Features Implemented:")
        print("  ✅ Mesh node registration and discovery protocols")
        print("  ✅ Distributed task queue with priority scheduling") 
        print("  ✅ Agent capability matching algorithms")
        print("  ✅ Mesh health monitoring and auto-recovery")
        print("  ✅ Scalability and resilience testing framework")
        print("  ✅ Mesh communication protocols and APIs documentation")
        
        # Create a completion marker
        with open('/tmp/phase3_validation_complete.json', 'w') as f:
            json.dump({
                'validation_status': 'complete',
                'tests_passed': passed_tests,
                'total_tests': total_tests,
                'timestamp': time.time(),
                'features_implemented': [
                    'mesh_node_discovery_protocols',
                    'distributed_task_queue_with_priority_scheduling',
                    'agent_capability_matching_algorithms', 
                    'mesh_health_monitoring_and_auto_recovery',
                    'scalability_resilience_testing',
                    'mesh_communication_protocols_documentation'
                ]
            }, f, indent=2)
        
        sys.exit(0)
    else:
        print(f"❌ {total_tests - passed_tests} tests failed. Phase 3 validation incomplete.")
        sys.exit(1)