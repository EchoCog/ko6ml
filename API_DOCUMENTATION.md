# KO6ML Cognitive Architecture - API Documentation

## Overview

This document provides comprehensive API documentation for the KO6ML Cognitive Architecture. The API is organized by phases, with each phase providing specific cognitive capabilities that build upon previous phases.

## Authentication and Setup

```python
# Basic setup
from cognitive_architecture.integration import kobold_cognitive_integrator

# Initialize the cognitive architecture
success = kobold_cognitive_integrator.initialize()
if not success:
    raise Exception("Failed to initialize cognitive architecture")
```

## Phase 1: Cognitive Primitives & Hypergraph Encoding

### Scheme Grammar Adapter

Convert between text and AtomSpace hypergraph patterns.

#### `SchemeGrammarAdapter`

```python
from cognitive_architecture.scheme_adapters.grammar_adapter import SchemeGrammarAdapter

adapter = SchemeGrammarAdapter()

# Translate text to AtomSpace patterns
patterns = adapter.translate_kobold_to_atomspace(text: str) -> List[str]

# Translate AtomSpace patterns back to text  
text = adapter.translate_atomspace_to_kobold(patterns: List[str]) -> str

# Register a new pattern
success = adapter.register_pattern(
    pattern_id: str,
    pattern_type: str,
    atomspace_pattern: str,
    confidence: float = 0.8
) -> bool

# Get pattern statistics
stats = adapter.get_pattern_statistics() -> Dict[str, Any]
```

### Cognitive Agent System

#### `CognitiveAgent`

```python
from cognitive_architecture.core import CognitiveAgent, CognitiveState, TensorShape

# Create an agent with tensor shape
agent = CognitiveAgent(
    agent_id: str,
    agent_type: str,
    tensor_shape: TensorShape,
    initial_state: CognitiveState = CognitiveState.IDLE
)

# State transitions
agent.transition_to_state(new_state: CognitiveState) -> bool

# Get current state and tensor info
state = agent.get_current_state() -> CognitiveState
tensor_info = agent.get_tensor_shape_info() -> Dict[str, Any]

# Create hypergraph fragment
fragment = agent.create_hypergraph_fragment() -> Dict[str, Any]
```

#### `TensorShape`

```python
from cognitive_architecture.core import TensorShape

# Standard cognitive tensor shape
shape = TensorShape(
    modality: int = 512,      # Input modality dimension
    depth: int = 64,          # Processing depth
    context: int = 2048,      # Context window size
    salience: int = 128,      # Attention salience
    autonomy_index: int = 32  # Agent autonomy level
)

# Get prime factorization signature
signature = shape.get_prime_factorization() -> str

# Calculate total parameters
total = shape.get_total_parameters() -> int
```

## Phase 2: ECAN Attention Allocation & Resource Kernel

### Economic Attention Network

#### `EconomicAttentionNetwork`

```python
from cognitive_architecture.ecan_attention.attention_kernel import EconomicAttentionNetwork

ecan = EconomicAttentionNetwork(
    sti_budget: int = 1000,
    lti_budget: int = 1000,
    decay_rate: float = 0.1
)

# Register cognitive elements
ecan.register_cognitive_element(
    element_id: str,
    initial_sti: float = 0.0,
    initial_lti: float = 0.0,
    initial_urgency: float = 0.0
) -> bool

# Allocate attention budget
ecan.allocate_attention_budget() -> Dict[str, float]

# Spreading activation
ecan.spreading_activation(
    source_element: str,
    spread_amount: float,
    max_hops: int = 3
) -> Dict[str, float]

# Get attention focus
focus = ecan.get_attention_focus(top_n: int = 10) -> List[Dict[str, Any]]

# Performance metrics
metrics = ecan.get_performance_metrics() -> Dict[str, Any]
```

#### `AttentionValue`

```python
from cognitive_architecture.ecan_attention.attention_kernel import AttentionValue

# Create attention value
attention = AttentionValue(
    sti: float = 0.0,    # Short Term Importance
    lti: float = 0.0,    # Long Term Importance
    vlti: float = 0.0,   # Very Long Term Importance
    urgency: float = 0.0, # Urgency level
    novelty: float = 0.0  # Novelty factor
)

# Update values
attention.update_sti(delta: float)
attention.update_lti(delta: float)
attention.update_urgency(delta: float)

# Get total importance
total = attention.get_total_importance() -> float
```

## Phase 3: Distributed Mesh Topology & Agent Orchestration

### Mesh Orchestrator

#### `CognitiveMeshOrchestrator`

```python
from cognitive_architecture.distributed_mesh.orchestrator import mesh_orchestrator

# Register a mesh node
node_id = mesh_orchestrator.register_node(
    node: MeshNode,
    capabilities: Set[str]
) -> str

# Submit distributed task
task_id = mesh_orchestrator.submit_task(
    task: DistributedTask,
    priority: int = 5
) -> str

# Get task status
status = mesh_orchestrator.get_task_status(task_id: str) -> Dict[str, Any]

# Get enhanced mesh status
mesh_status = mesh_orchestrator.get_enhanced_mesh_status() -> Dict[str, Any]

# Start enhanced orchestration
await mesh_orchestrator.start_enhanced_orchestration()
```

#### `MeshNode`

```python
from cognitive_architecture.distributed_mesh import MeshNode, MeshNodeType

# Create mesh node
node = MeshNode(
    node_type: MeshNodeType,
    capabilities: Set[str],
    max_load: float = 1.0,
    performance_score: float = 1.0
)

# Update node load
node.update_load(new_load: float)

# Check if node can handle capability
can_handle = node.can_handle_capability(capability: str) -> bool
```

### Discovery Service

#### `MeshDiscoveryService`

```python
from cognitive_architecture.distributed_mesh.discovery import discovery_service

# Find nodes for capabilities
nodes = discovery_service.find_nodes_for_capabilities(
    required_capabilities: Set[str],
    task_priority: float = 1.0,
    max_nodes: int = 3
) -> List[NodeAdvertisement]

# Get discovery statistics
stats = discovery_service.get_discovery_statistics() -> Dict[str, Any]

# Start discovery service
await discovery_service.start_discovery()
```

## Phase 4: KoboldAI Integration

### Cognitive Integrator

#### `KoboldCognitiveIntegrator`

```python
from cognitive_architecture.integration import kobold_cognitive_integrator

# Initialize integration
success = kobold_cognitive_integrator.initialize() -> bool

# Process input with cognitive enhancement
enhanced_input = kobold_cognitive_integrator.process_input(
    text: str,
    context: Dict[str, Any] = None
) -> str

# Analyze generated output
analysis = kobold_cognitive_integrator.analyze_output(
    generated_text: str,
    original_input: str = None
) -> Dict[str, Any]

# Update story context
kobold_cognitive_integrator.update_story_context(
    context: Dict[str, Any]
) -> bool

# Get creative suggestions
suggestions = kobold_cognitive_integrator.get_creative_suggestions(
    context: Dict[str, Any],
    focus: str = None
) -> List[str]

# Get integration status
status = kobold_cognitive_integrator.get_integration_status() -> Dict[str, Any]
```

### Context Management

```python
# Update character information
kobold_cognitive_integrator.update_character_info(
    character_name: str,
    character_data: Dict[str, Any]
) -> bool

# Check story consistency
consistency = kobold_cognitive_integrator.check_story_consistency(
    story_text: str
) -> Dict[str, Any]

# Get performance metrics
metrics = kobold_cognitive_integrator.get_performance_metrics() -> Dict[str, Any]
```

## Phase 5: Advanced Reasoning & Multi-Modal Cognition

### Advanced Reasoning Engine

#### `AdvancedReasoningEngine`

```python
from cognitive_architecture.reasoning import advanced_reasoning_engine

# Analyze story with comprehensive reasoning
result = advanced_reasoning_engine.reason_about_story(
    story_data: Dict[str, Any],
    reasoning_types: List[str] = None
) -> ReasoningResult

# Get reasoning status
status = advanced_reasoning_engine.get_reasoning_status() -> Dict[str, Any]

# Update cognitive schema
advanced_reasoning_engine.update_cognitive_schema(
    schema_name: str,
    schema_data: Dict[str, Any]
) -> bool
```

### Logical Inference Engine

#### `LogicalInferenceEngine`

```python
from cognitive_architecture.reasoning.inference import LogicalInferenceEngine, InferenceRule

logical_engine = LogicalInferenceEngine()

# Add inference rule
rule = InferenceRule(
    rule_id: str,
    rule_type: InferenceType,
    premises: List[str],
    conclusion: str,
    confidence: float = 0.8
)
logical_engine.add_inference_rule(rule)

# Perform reasoning on narrative
result = logical_engine.reason_about_narrative(
    story_elements: List[Dict[str, Any]]
) -> LogicalResult

# Get inference statistics
stats = logical_engine.get_inference_statistics() -> Dict[str, Any]
```

### Temporal Reasoning Engine

#### `TemporalReasoningEngine`

```python
from cognitive_architecture.reasoning.temporal import TemporalReasoningEngine, TemporalEvent

temporal_engine = TemporalReasoningEngine()

# Add temporal event
event = TemporalEvent(
    event_id: str,
    description: str,
    time_frame: TimeFrame,
    timestamp: float = None
)
temporal_engine.add_event(event)

# Analyze story continuity
result = temporal_engine.analyze_story_continuity(
    story_events: List[Dict[str, Any]]
) -> TemporalResult

# Check for plot holes
plot_holes = temporal_engine.detect_plot_holes(
    events: List[TemporalEvent]
) -> List[Dict[str, Any]]
```

### Causal Reasoning Network

#### `CausalReasoningNetwork`

```python
from cognitive_architecture.reasoning.causal import CausalReasoningNetwork, PlotElement

causal_network = CausalReasoningNetwork()

# Add plot element
element = PlotElement(
    element_id: str,
    element_type: str,
    description: str,
    influence_potential: float = 0.5
)
causal_network.add_plot_element(element)

# Analyze plot causality
result = causal_network.analyze_plot_causality(
    story_data: Dict[str, Any]
) -> CausalResult

# Predict plot development
predictions = causal_network.predict_plot_development(
    current_state: Dict[str, Any],
    num_predictions: int = 5
) -> List[Dict[str, Any]]
```

### Multi-Modal Processor

#### `MultiModalProcessor`

```python
from cognitive_architecture.reasoning.multimodal import MultiModalProcessor, ModalData

processor = MultiModalProcessor()

# Process multi-modal data
modal_data = [
    ModalData(
        data_id: str,
        modality: ModalityType,
        content: Any
    )
]

result = processor.process_multi_modal_data(
    modal_data: List[ModalData]
) -> MultiModalResult

# Analyze cross-modal connections
connections = processor.analyze_cross_modal_connections(
    modal_data: List[ModalData]
) -> List[Dict[str, Any]]
```

## Phase 6: Meta-Cognitive Learning & Adaptive Optimization

### Meta-Cognitive Engine

#### `MetaCognitiveEngine`

```python
from cognitive_architecture.meta_learning import meta_cognitive_engine

# Process cognitive task with optimization
result = meta_cognitive_engine.process_cognitive_task(
    task_data: Dict[str, Any],
    context: str = None
) -> Dict[str, Any]

# Get meta-cognitive status
status = meta_cognitive_engine.get_meta_cognitive_status() -> Dict[str, Any]

# Start meta-cognitive loop
await meta_cognitive_engine.start_meta_cognitive_loop()

# Update self-awareness metrics
meta_cognitive_engine.update_self_awareness_metrics()
```

### Performance Monitor

#### `PerformanceMonitor`

```python
from cognitive_architecture.meta_learning import PerformanceMonitor, MetricType

monitor = PerformanceMonitor(history_size: int = 1000)

# Record metrics
monitor.record_processing_time(
    operation: str,
    time_seconds: float,
    component: str = None
)

monitor.record_accuracy(
    accuracy: float,
    task: str,
    component: str = None
)

monitor.record_efficiency(
    efficiency: float,
    metric_name: str,
    component: str = None
)

# Get performance summary
summary = monitor.get_performance_summary() -> Dict[str, Any]

# Get performance improvement
improvement = monitor.get_performance_improvement(
    metric_type: MetricType,
    time_window: int = 3600
) -> Dict[str, Any]
```

### Adaptive Optimizer

#### `AdaptiveOptimizer`

```python
from cognitive_architecture.meta_learning import AdaptiveOptimizer, ContextualProfile

optimizer = AdaptiveOptimizer()

# Register context profile
profile = ContextualProfile(
    context_type: str,
    task_complexity: float,
    time_pressure: float,
    accuracy_requirement: float
)
optimizer.contextual_adapter.register_context_profile(profile)

# Optimize for context
result = optimizer.optimize_for_context(
    context: str,
    task_data: Dict[str, Any]
) -> Dict[str, Any]

# Get optimization statistics
stats = optimizer.get_optimization_statistics() -> Dict[str, Any]
```

### Learning Engine

#### `LearningEngine`

```python
from cognitive_architecture.meta_learning import LearningEngine, CognitivePattern

engine = LearningEngine()

# Learn cognitive pattern
pattern = CognitivePattern(
    pattern_id: str,
    pattern_type: PatternType,
    pattern_data: Dict[str, Any],
    effectiveness_score: float = 0.5
)
success = engine.pattern_learner.learn_pattern(pattern) -> bool

# Optimize pattern
engine.pattern_learner.optimize_pattern(
    pattern_id: str,
    optimization_target: str = "effectiveness"
) -> bool

# Execute learning cycle
await engine.learning_cycle()

# Get learning statistics
stats = engine.get_learning_statistics() -> Dict[str, Any]
```

## Error Handling

All API methods include comprehensive error handling:

```python
try:
    result = kobold_cognitive_integrator.process_input(text)
except CognitiveArchitectureError as e:
    print(f"Cognitive processing error: {e}")
    # Handle error appropriately
except Exception as e:
    print(f"Unexpected error: {e}")
    # Log error and handle gracefully
```

### Common Error Types

- `CognitiveArchitectureError`: Base exception for cognitive architecture issues
- `AtomSpaceError`: Issues with hypergraph pattern processing
- `ECANError`: Attention allocation failures
- `MeshError`: Distributed processing issues
- `ReasoningError`: Advanced reasoning failures
- `MetaCognitiveError`: Meta-learning system issues

## Configuration

### Global Configuration

```python
from cognitive_architecture.config import CognitiveConfig

config = CognitiveConfig(
    ecan_sti_budget=1000,
    ecan_lti_budget=1000,
    mesh_max_nodes=10,
    reasoning_timeout=30.0,
    meta_learning_enabled=True,
    performance_monitoring=True
)

kobold_cognitive_integrator.configure(config)
```

### Component-Specific Configuration

```python
# ECAN configuration
ecan_config = {
    'sti_budget': 1500,
    'lti_budget': 1500,
    'decay_rate': 0.05,
    'spreading_threshold': 0.1
}

# Mesh configuration
mesh_config = {
    'discovery_interval': 30.0,
    'health_check_interval': 10.0,
    'max_retry_attempts': 3,
    'timeout_seconds': 60.0
}

# Reasoning configuration
reasoning_config = {
    'logical_confidence_threshold': 0.7,
    'temporal_consistency_threshold': 0.8,
    'causal_influence_threshold': 0.6,
    'multimodal_pattern_threshold': 0.5
}

# Apply configurations
kobold_cognitive_integrator.configure_components({
    'ecan': ecan_config,
    'mesh': mesh_config,
    'reasoning': reasoning_config
})
```

## Event System

The cognitive architecture provides an event system for monitoring and integration:

```python
from cognitive_architecture.events import CognitiveEventManager

# Register event handlers
def on_attention_allocated(event):
    print(f"Attention allocated: {event.data}")

def on_reasoning_completed(event):
    print(f"Reasoning completed: {event.data}")

# Subscribe to events
CognitiveEventManager.subscribe('attention_allocated', on_attention_allocated)
CognitiveEventManager.subscribe('reasoning_completed', on_reasoning_completed)

# Publish custom events
CognitiveEventManager.publish('custom_event', {'data': 'value'})
```

## Performance Monitoring

Real-time performance monitoring is available throughout the API:

```python
# Get system-wide performance
performance = kobold_cognitive_integrator.get_system_performance()

print(f"CPU Usage: {performance['cpu_usage']:.1%}")
print(f"Memory Usage: {performance['memory_usage']:.1%}")
print(f"Processing Speed: {performance['processing_speed']:.2f} ops/sec")
print(f"Average Response Time: {performance['avg_response_time']:.3f}s")
```

## Development and Testing

### Testing Framework

```python
import unittest
from cognitive_architecture.testing import CognitiveTestCase

class MyTestCase(CognitiveTestCase):
    def test_cognitive_processing(self):
        # Use test utilities
        test_data = self.create_test_story_data()
        result = advanced_reasoning_engine.reason_about_story(test_data)
        self.assertGreater(result.overall_confidence, 0.5)
```

### Debug Mode

```python
# Enable debug mode for detailed logging
kobold_cognitive_integrator.enable_debug_mode()

# Get debug information
debug_info = kobold_cognitive_integrator.get_debug_info()
```

This API documentation provides comprehensive coverage of all cognitive architecture components. Each API is designed to be intuitive while providing powerful cognitive processing capabilities.