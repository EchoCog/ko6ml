"""
Meta-Cognitive Learning & Adaptive Optimization Module

This module implements meta-cognitive capabilities for self-improvement and
adaptive optimization, enabling the cognitive architecture to learn about its
own performance and optimize its processing strategies.
"""

from .performance_monitor import PerformanceMonitor, MetricType, PerformanceMetric
from .adaptive_optimizer import AdaptiveOptimizer, OptimizationStrategy, ContextualAdapter, ContextualProfile
from .learning_engine import LearningEngine, PatternLearner, FeedbackProcessor, PatternType, CognitivePattern, LearningMode, FeedbackData
from .meta_cognitive_engine import MetaCognitiveEngine

__all__ = [
    'PerformanceMonitor',
    'MetricType', 
    'PerformanceMetric',
    'AdaptiveOptimizer',
    'OptimizationStrategy',
    'ContextualAdapter',
    'ContextualProfile',
    'LearningEngine',
    'PatternLearner',
    'FeedbackProcessor',
    'PatternType',
    'CognitivePattern',
    'LearningMode', 
    'FeedbackData',
    'MetaCognitiveEngine'
]

# Global meta-cognitive engine instance
meta_cognitive_engine = MetaCognitiveEngine()