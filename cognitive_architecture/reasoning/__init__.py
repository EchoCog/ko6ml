"""
Phase 5: Advanced Reasoning & Multi-Modal Cognition

This module provides advanced reasoning capabilities including:
- Logical inference engines using AtomSpace
- Temporal reasoning for story continuity
- Causal reasoning networks for plot development
- Multi-modal processing (text, structured data, metadata)
"""

try:
    from .inference import LogicalInferenceEngine, InferenceRule, InferenceType
    from .temporal import TemporalReasoningEngine, TemporalRelation, TimeFrame
    from .causal import CausalReasoningNetwork, CausalLink, CausalStrength
    from .multimodal import MultiModalProcessor, ModalityType, ProcessingResult

    # Main reasoning engine that integrates all components
    from .reasoning_engine import AdvancedReasoningEngine

    # Global instance for integration with existing cognitive architecture
    advanced_reasoning_engine = AdvancedReasoningEngine()
    
except ImportError as e:
    import logging
    logger = logging.getLogger(__name__)
    logger.warning(f"Could not import reasoning components: {e}")
    
    # Create dummy classes for graceful degradation
    class DummyReasoningEngine:
        def __init__(self):
            pass
        def reason_about_story(self, *args, **kwargs):
            return {'error': 'Reasoning engine not available'}
    
    advanced_reasoning_engine = DummyReasoningEngine()

__all__ = [
    'LogicalInferenceEngine', 'InferenceRule', 'InferenceType',
    'TemporalReasoningEngine', 'TemporalRelation', 'TimeFrame', 
    'CausalReasoningNetwork', 'CausalLink', 'CausalStrength',
    'MultiModalProcessor', 'ModalityType', 'ProcessingResult',
    'AdvancedReasoningEngine', 'advanced_reasoning_engine'
]