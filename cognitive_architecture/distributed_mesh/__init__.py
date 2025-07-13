"""
Distributed Mesh Components for Cognitive Architecture

Phase 3 Enhanced Components:
- Advanced node discovery protocols
- Enhanced capability matching algorithms  
- Fault tolerance and auto-recovery
- Load testing and resilience validation
- Comprehensive health monitoring
"""

from .orchestrator import (
    CognitiveMeshOrchestrator,
    MeshNode,
    MeshNodeType,
    DistributedTask,
    TaskStatus,
    mesh_orchestrator,
    setup_ecan_integration,
    setup_phase3_integration
)

from .discovery import (
    MeshDiscoveryService,
    NodeAdvertisement,
    DiscoveryConfig,
    DiscoveryProtocol,
    CapabilityMatcher,
    discovery_service
)

from .fault_tolerance import (
    FaultToleranceManager,
    HealthMetrics,
    FailureEvent,
    FailureType,
    RecoveryStrategy,
    CircuitBreaker,
    fault_tolerance_manager
)

from .load_testing import (
    LoadTestingFramework,
    LoadTestConfig,
    LoadTestType,
    ResilienceTestType,
    ChaosMonkey,
    load_testing_framework,
    PREDEFINED_CONFIGS
)

__all__ = [
    # Core orchestration
    'CognitiveMeshOrchestrator',
    'MeshNode', 
    'MeshNodeType',
    'DistributedTask',
    'TaskStatus',
    'mesh_orchestrator',
    'setup_ecan_integration',
    'setup_phase3_integration',
    
    # Discovery
    'MeshDiscoveryService',
    'NodeAdvertisement',
    'DiscoveryConfig', 
    'DiscoveryProtocol',
    'CapabilityMatcher',
    'discovery_service',
    
    # Fault tolerance
    'FaultToleranceManager',
    'HealthMetrics',
    'FailureEvent',
    'FailureType',
    'RecoveryStrategy', 
    'CircuitBreaker',
    'fault_tolerance_manager',
    
    # Load testing
    'LoadTestingFramework',
    'LoadTestConfig',
    'LoadTestType',
    'ResilienceTestType',
    'ChaosMonkey', 
    'load_testing_framework',
    'PREDEFINED_CONFIGS'
]