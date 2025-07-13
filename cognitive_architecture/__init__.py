"""
Cognitive Architecture Package

This package implements a distributed agentic cognitive grammar network
that integrates with KoboldAI for enhanced AI-assisted writing capabilities,
including meta-cognitive learning and adaptive optimization.
"""

from .core import cognitive_core, CognitiveAgent, CognitiveState, TensorShape
from .integration import (
    initialize_cognitive_architecture,
    process_kobold_input,
    process_kobold_output,
    update_kobold_memory,
    update_kobold_worldinfo,
    get_cognitive_status,
    shutdown_cognitive_architecture,
    kobold_cognitive_integrator
)
from .scheme_adapters.grammar_adapter import scheme_adapter
from .ecan_attention.attention_kernel import ecan_system
from .distributed_mesh.orchestrator import mesh_orchestrator
from .meta_learning import meta_cognitive_engine
# Flask API is optional - import separately if needed
# from .distributed_mesh.api import start_cognitive_api_server

__version__ = "1.0.0"
__author__ = "KoboldAI Cognitive Architecture Team"

__all__ = [
    # Core components
    'cognitive_core',
    'CognitiveAgent',
    'CognitiveState', 
    'TensorShape',
    
    # Integration functions
    'initialize_cognitive_architecture',
    'process_kobold_input',
    'process_kobold_output',
    'update_kobold_memory',
    'update_kobold_worldinfo',
    'get_cognitive_status',
    'shutdown_cognitive_architecture',
    'kobold_cognitive_integrator',
    
    # Component instances
    'scheme_adapter',
    'ecan_system',
    'mesh_orchestrator',
    'meta_cognitive_engine'
    
    # API server available separately:
    # from cognitive_architecture.distributed_mesh.api import start_cognitive_api_server
]