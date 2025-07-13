"""
Adaptive Optimization System

Implements adaptive algorithm selection based on context and performance
feedback, enabling the system to dynamically choose optimal processing
strategies for different cognitive tasks.
"""

import time
import logging
import asyncio
from typing import Dict, List, Any, Optional, Callable, Tuple
from dataclasses import dataclass, field
from enum import Enum
import numpy as np
from collections import defaultdict, deque
import json

logger = logging.getLogger(__name__)


class OptimizationStrategy(Enum):
    """Types of optimization strategies"""
    SPEED_OPTIMIZED = "speed_optimized"
    ACCURACY_OPTIMIZED = "accuracy_optimized"
    MEMORY_OPTIMIZED = "memory_optimized"
    BALANCED = "balanced"
    QUALITY_OPTIMIZED = "quality_optimized"
    ADAPTIVE = "adaptive"


@dataclass
class ContextualProfile:
    """Contextual processing profile"""
    context_type: str
    task_complexity: float = 0.5  # 0-1 scale
    time_pressure: float = 0.5  # 0-1 scale
    accuracy_requirement: float = 0.7  # 0-1 scale
    resource_availability: float = 1.0  # 0-1 scale
    user_preference: str = "balanced"
    historical_performance: Dict[str, float] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary representation"""
        return {
            'context_type': self.context_type,
            'task_complexity': self.task_complexity,
            'time_pressure': self.time_pressure,
            'accuracy_requirement': self.accuracy_requirement,
            'resource_availability': self.resource_availability,
            'user_preference': self.user_preference,
            'historical_performance': self.historical_performance
        }


@dataclass
class AlgorithmConfiguration:
    """Algorithm configuration parameters"""
    algorithm_id: str
    parameters: Dict[str, Any] = field(default_factory=dict)
    strategy: OptimizationStrategy = OptimizationStrategy.BALANCED
    performance_metrics: Dict[str, float] = field(default_factory=dict)
    usage_count: int = 0
    success_rate: float = 1.0
    avg_processing_time: float = 0.0
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary representation"""
        return {
            'algorithm_id': self.algorithm_id,
            'parameters': self.parameters,
            'strategy': self.strategy.value,
            'performance_metrics': self.performance_metrics,
            'usage_count': self.usage_count,
            'success_rate': self.success_rate,
            'avg_processing_time': self.avg_processing_time
        }


class ContextualAdapter:
    """Contextual adaptation for algorithm selection"""
    
    def __init__(self):
        self.context_profiles: Dict[str, ContextualProfile] = {}
        self.algorithm_configs: Dict[str, AlgorithmConfiguration] = {}
        self.context_algorithm_matrix: Dict[str, Dict[str, float]] = defaultdict(dict)
        self.adaptation_history: deque = deque(maxlen=1000)
        
        # Default algorithm configurations
        self._initialize_default_algorithms()
        
        logger.info("Contextual adapter initialized")
    
    def _initialize_default_algorithms(self):
        """Initialize default algorithm configurations"""
        default_algorithms = [
            AlgorithmConfiguration(
                algorithm_id="logical_inference_standard",
                parameters={"confidence_threshold": 0.7, "max_iterations": 10},
                strategy=OptimizationStrategy.BALANCED
            ),
            AlgorithmConfiguration(
                algorithm_id="logical_inference_fast",
                parameters={"confidence_threshold": 0.5, "max_iterations": 5},
                strategy=OptimizationStrategy.SPEED_OPTIMIZED
            ),
            AlgorithmConfiguration(
                algorithm_id="logical_inference_thorough",
                parameters={"confidence_threshold": 0.9, "max_iterations": 20},
                strategy=OptimizationStrategy.ACCURACY_OPTIMIZED
            ),
            AlgorithmConfiguration(
                algorithm_id="temporal_reasoning_standard",
                parameters={"time_window": 100, "constraint_weight": 0.7},
                strategy=OptimizationStrategy.BALANCED
            ),
            AlgorithmConfiguration(
                algorithm_id="temporal_reasoning_fast",
                parameters={"time_window": 50, "constraint_weight": 0.5},
                strategy=OptimizationStrategy.SPEED_OPTIMIZED
            ),
            AlgorithmConfiguration(
                algorithm_id="causal_network_standard",
                parameters={"influence_threshold": 0.6, "depth_limit": 5},
                strategy=OptimizationStrategy.BALANCED
            ),
            AlgorithmConfiguration(
                algorithm_id="multimodal_processing_standard",
                parameters={"modality_weights": {}, "cross_modal_threshold": 0.5},
                strategy=OptimizationStrategy.BALANCED
            )
        ]
        
        for algo in default_algorithms:
            self.algorithm_configs[algo.algorithm_id] = algo
    
    def register_context_profile(self, profile: ContextualProfile):
        """Register a contextual profile"""
        self.context_profiles[profile.context_type] = profile
        logger.info(f"Registered context profile: {profile.context_type}")
    
    def update_algorithm_performance(self, algorithm_id: str, performance_data: Dict[str, float]):
        """Update algorithm performance metrics"""
        if algorithm_id in self.algorithm_configs:
            config = self.algorithm_configs[algorithm_id]
            config.usage_count += 1
            config.performance_metrics.update(performance_data)
            
            # Update success rate
            if 'success' in performance_data:
                old_rate = config.success_rate
                config.success_rate = (old_rate * (config.usage_count - 1) + performance_data['success']) / config.usage_count
            
            # Update average processing time
            if 'processing_time' in performance_data:
                old_time = config.avg_processing_time
                config.avg_processing_time = (old_time * (config.usage_count - 1) + performance_data['processing_time']) / config.usage_count
            
            logger.debug(f"Updated performance for {algorithm_id}: {performance_data}")
    
    def select_optimal_algorithm(self, context_type: str, task_data: Dict[str, Any]) -> str:
        """Select optimal algorithm based on context"""
        try:
            # Get context profile
            profile = self.context_profiles.get(context_type)
            if not profile:
                # Create default profile for unknown context
                profile = ContextualProfile(context_type=context_type)
                self.register_context_profile(profile)
            
            # Calculate context-specific requirements
            requirements = self._analyze_task_requirements(task_data, profile)
            
            # Score all algorithms for this context
            algorithm_scores = {}
            for algo_id, config in self.algorithm_configs.items():
                score = self._score_algorithm_for_context(config, requirements, profile)
                algorithm_scores[algo_id] = score
            
            # Select best algorithm
            best_algorithm = max(algorithm_scores.items(), key=lambda x: x[1])
            selected_algorithm = best_algorithm[0]
            
            # Record adaptation decision
            self.adaptation_history.append({
                'timestamp': time.time(),
                'context_type': context_type,
                'selected_algorithm': selected_algorithm,
                'score': best_algorithm[1],
                'all_scores': algorithm_scores,
                'requirements': requirements
            })
            
            logger.info(f"Selected algorithm {selected_algorithm} for context {context_type} "
                       f"(score: {best_algorithm[1]:.3f})")
            
            return selected_algorithm
            
        except Exception as e:
            logger.error(f"Error selecting optimal algorithm: {e}")
            return "logical_inference_standard"  # Fallback
    
    def _analyze_task_requirements(self, task_data: Dict[str, Any], 
                                 profile: ContextualProfile) -> Dict[str, float]:
        """Analyze task requirements based on data and context"""
        requirements = {
            'speed_importance': 0.5,
            'accuracy_importance': 0.7,
            'memory_importance': 0.3,
            'quality_importance': 0.6
        }
        
        # Adjust based on context profile
        requirements['speed_importance'] = profile.time_pressure
        requirements['accuracy_importance'] = profile.accuracy_requirement
        requirements['quality_importance'] = (profile.accuracy_requirement + 0.3) / 1.3
        
        # Adjust based on task complexity
        if profile.task_complexity > 0.7:
            requirements['accuracy_importance'] *= 1.2
            requirements['quality_importance'] *= 1.2
            requirements['speed_importance'] *= 0.8
        elif profile.task_complexity < 0.3:
            requirements['speed_importance'] *= 1.3
            requirements['accuracy_importance'] *= 0.9
        
        # Adjust based on resource availability
        requirements['memory_importance'] = 1.0 - profile.resource_availability
        
        # Analyze task data characteristics
        if 'text' in task_data:
            text_length = len(task_data['text'])
            if text_length > 1000:  # Long text
                requirements['speed_importance'] *= 0.9
                requirements['quality_importance'] *= 1.1
        
        if 'events' in task_data and len(task_data['events']) > 10:
            requirements['accuracy_importance'] *= 1.1  # Complex temporal reasoning
        
        # Normalize to 0-1 range
        for key in requirements:
            requirements[key] = max(0.0, min(1.0, requirements[key]))
        
        return requirements
    
    def _score_algorithm_for_context(self, config: AlgorithmConfiguration, 
                                   requirements: Dict[str, float],
                                   profile: ContextualProfile) -> float:
        """Score algorithm suitability for context"""
        score = 0.0
        
        # Strategy alignment score
        strategy_scores = {
            OptimizationStrategy.SPEED_OPTIMIZED: requirements['speed_importance'] * 1.2,
            OptimizationStrategy.ACCURACY_OPTIMIZED: requirements['accuracy_importance'] * 1.2,
            OptimizationStrategy.MEMORY_OPTIMIZED: requirements['memory_importance'] * 1.2,
            OptimizationStrategy.QUALITY_OPTIMIZED: requirements['quality_importance'] * 1.2,
            OptimizationStrategy.BALANCED: sum(requirements.values()) / len(requirements),
            OptimizationStrategy.ADAPTIVE: sum(requirements.values()) / len(requirements) * 1.1
        }
        
        score += strategy_scores.get(config.strategy, 0.5) * 0.4
        
        # Historical performance score
        if config.usage_count > 0:
            score += config.success_rate * 0.3
            
            # Processing time score (lower is better for speed)
            if config.avg_processing_time > 0:
                time_score = max(0.1, 1.0 - (config.avg_processing_time / 10.0))  # Normalize to 10s max
                score += time_score * requirements['speed_importance'] * 0.2
        else:
            score += 0.5 * 0.5  # Default score for untested algorithms
        
        # Context-specific historical performance
        context_performance = profile.historical_performance.get(config.algorithm_id, 0.5)
        score += context_performance * 0.1
        
        return score
    
    def get_adaptation_statistics(self) -> Dict[str, Any]:
        """Get adaptation statistics"""
        if not self.adaptation_history:
            return {'error': 'No adaptation history available'}
        
        recent_adaptations = list(self.adaptation_history)[-100:]  # Last 100 adaptations
        
        # Algorithm usage frequency
        algorithm_usage = defaultdict(int)
        context_usage = defaultdict(int)
        
        for adaptation in recent_adaptations:
            algorithm_usage[adaptation['selected_algorithm']] += 1
            context_usage[adaptation['context_type']] += 1
        
        # Average scores
        avg_scores = {}
        for algo_id in algorithm_usage:
            scores = [a['score'] for a in recent_adaptations if a['selected_algorithm'] == algo_id]
            avg_scores[algo_id] = np.mean(scores) if scores else 0
        
        return {
            'total_adaptations': len(self.adaptation_history),
            'recent_adaptations': len(recent_adaptations),
            'algorithm_usage': dict(algorithm_usage),
            'context_usage': dict(context_usage),
            'average_scores': avg_scores,
            'most_used_algorithm': max(algorithm_usage.items(), key=lambda x: x[1])[0] if algorithm_usage else None,
            'highest_scoring_algorithm': max(avg_scores.items(), key=lambda x: x[1])[0] if avg_scores else None
        }


class AdaptiveOptimizer:
    """Main adaptive optimization system"""
    
    def __init__(self):
        self.contextual_adapter = ContextualAdapter()
        self.optimization_strategies: Dict[OptimizationStrategy, Dict[str, Any]] = {}
        self.active_optimizations: Dict[str, Dict[str, Any]] = {}
        self.optimization_history: deque = deque(maxlen=1000)
        self.performance_baselines: Dict[str, float] = {}
        
        # Initialize optimization strategies
        self._initialize_optimization_strategies()
        
        logger.info("Adaptive optimizer initialized")
    
    def _initialize_optimization_strategies(self):
        """Initialize optimization strategies"""
        self.optimization_strategies = {
            OptimizationStrategy.SPEED_OPTIMIZED: {
                'timeout_multiplier': 0.5,
                'accuracy_threshold': 0.6,
                'parallel_processing': True,
                'cache_aggressive': True,
                'early_termination': True
            },
            OptimizationStrategy.ACCURACY_OPTIMIZED: {
                'timeout_multiplier': 2.0,
                'accuracy_threshold': 0.9,
                'parallel_processing': False,
                'cache_aggressive': False,
                'early_termination': False,
                'validation_rounds': 3
            },
            OptimizationStrategy.MEMORY_OPTIMIZED: {
                'batch_size_reducer': 0.5,
                'cache_size_limit': 0.3,
                'streaming_mode': True,
                'garbage_collection': True
            },
            OptimizationStrategy.BALANCED: {
                'timeout_multiplier': 1.0,
                'accuracy_threshold': 0.7,
                'parallel_processing': True,
                'cache_aggressive': False
            },
            OptimizationStrategy.QUALITY_OPTIMIZED: {
                'timeout_multiplier': 1.5,
                'accuracy_threshold': 0.85,
                'validation_rounds': 2,
                'quality_checks': True,
                'refinement_passes': 2
            },
            OptimizationStrategy.ADAPTIVE: {
                'dynamic_adjustment': True,
                'performance_monitoring': True,
                'auto_tuning': True
            }
        }
    
    def optimize_for_context(self, context_type: str, task_data: Dict[str, Any], 
                           performance_requirements: Optional[Dict[str, float]] = None) -> Dict[str, Any]:
        """Optimize processing for specific context"""
        try:
            optimization_id = f"opt_{int(time.time()*1000)}_{hash(str(task_data))}"
            
            # Select optimal algorithm
            selected_algorithm = self.contextual_adapter.select_optimal_algorithm(context_type, task_data)
            
            # Get algorithm configuration
            algorithm_config = self.contextual_adapter.algorithm_configs.get(selected_algorithm)
            if not algorithm_config:
                logger.error(f"Algorithm configuration not found: {selected_algorithm}")
                return {'error': f'Algorithm not found: {selected_algorithm}'}
            
            # Apply optimization strategy
            strategy_config = self.optimization_strategies.get(algorithm_config.strategy, {})
            
            # Merge configuration with strategy-specific optimizations
            optimized_config = {
                'algorithm_id': selected_algorithm,
                'base_parameters': algorithm_config.parameters.copy(),
                'strategy': algorithm_config.strategy.value,
                'optimizations': strategy_config.copy()
            }
            
            # Apply performance requirements if specified
            if performance_requirements:
                optimized_config = self._apply_performance_requirements(
                    optimized_config, performance_requirements
                )
            
            # Apply adaptive optimizations
            if algorithm_config.strategy == OptimizationStrategy.ADAPTIVE:
                optimized_config = self._apply_adaptive_optimizations(
                    optimized_config, context_type, task_data
                )
            
            # Record optimization
            self.active_optimizations[optimization_id] = {
                'context_type': context_type,
                'algorithm_id': selected_algorithm,
                'config': optimized_config,
                'start_time': time.time(),
                'task_data_hash': hash(str(task_data))
            }
            
            logger.info(f"Optimized configuration for {context_type}: {selected_algorithm} "
                       f"with strategy {algorithm_config.strategy.value}")
            
            return {
                'optimization_id': optimization_id,
                'optimized_config': optimized_config,
                'estimated_performance': self._estimate_performance(optimized_config, task_data)
            }
            
        except Exception as e:
            logger.error(f"Error optimizing for context: {e}")
            return {'error': str(e)}
    
    def _apply_performance_requirements(self, config: Dict[str, Any], 
                                      requirements: Dict[str, float]) -> Dict[str, Any]:
        """Apply specific performance requirements to configuration"""
        optimizations = config['optimizations']
        
        # Speed requirements
        if requirements.get('max_processing_time'):
            max_time = requirements['max_processing_time']
            if max_time < 1.0:  # Very fast required
                optimizations['timeout_multiplier'] = 0.3
                optimizations['early_termination'] = True
                optimizations['parallel_processing'] = True
            elif max_time < 5.0:  # Fast required
                optimizations['timeout_multiplier'] = 0.7
        
        # Accuracy requirements
        if requirements.get('min_accuracy'):
            min_accuracy = requirements['min_accuracy']
            if min_accuracy > 0.9:  # High accuracy required
                optimizations['accuracy_threshold'] = min_accuracy
                optimizations['validation_rounds'] = 3
                optimizations['early_termination'] = False
        
        # Memory requirements
        if requirements.get('max_memory_usage'):
            max_memory = requirements['max_memory_usage']
            if max_memory < 100:  # Low memory available
                optimizations['streaming_mode'] = True
                optimizations['cache_size_limit'] = 0.2
                optimizations['batch_size_reducer'] = 0.3
        
        return config
    
    def _apply_adaptive_optimizations(self, config: Dict[str, Any], 
                                    context_type: str, task_data: Dict[str, Any]) -> Dict[str, Any]:
        """Apply adaptive optimizations based on learning"""
        # Get historical performance for this context
        context_profile = self.contextual_adapter.context_profiles.get(context_type)
        
        if context_profile and context_profile.historical_performance:
            # Analyze what worked well historically
            best_algorithm = max(context_profile.historical_performance.items(), 
                               key=lambda x: x[1])[0]
            
            # Adapt parameters based on successful patterns
            if best_algorithm in self.contextual_adapter.algorithm_configs:
                best_config = self.contextual_adapter.algorithm_configs[best_algorithm]
                
                # Blend parameters from successful configuration
                base_params = config['base_parameters']
                best_params = best_config.parameters
                
                for param, value in best_params.items():
                    if param in base_params and isinstance(value, (int, float)):
                        # Weighted average towards successful parameters
                        base_params[param] = 0.7 * base_params[param] + 0.3 * value
        
        # Analyze task characteristics for dynamic adjustment
        if 'text' in task_data:
            text_length = len(task_data['text'])
            complexity_factor = min(1.0, text_length / 1000)
            
            # Adjust timeouts based on text complexity
            current_timeout = config['optimizations'].get('timeout_multiplier', 1.0)
            config['optimizations']['timeout_multiplier'] = current_timeout * (1 + complexity_factor * 0.5)
        
        return config
    
    def _estimate_performance(self, config: Dict[str, Any], task_data: Dict[str, Any]) -> Dict[str, float]:
        """Estimate performance based on configuration and task data"""
        algorithm_id = config['algorithm_id']
        algorithm_config = self.contextual_adapter.algorithm_configs.get(algorithm_id)
        
        if not algorithm_config:
            return {'estimated_time': 1.0, 'estimated_accuracy': 0.7}
        
        # Base estimates from historical data
        base_time = algorithm_config.avg_processing_time or 1.0
        base_accuracy = algorithm_config.success_rate or 0.7
        
        # Apply optimization effects
        optimizations = config['optimizations']
        
        # Timeout multiplier affects processing time
        time_multiplier = optimizations.get('timeout_multiplier', 1.0)
        estimated_time = base_time * time_multiplier
        
        # Accuracy threshold affects accuracy
        accuracy_threshold = optimizations.get('accuracy_threshold', 0.7)
        estimated_accuracy = min(1.0, base_accuracy * (1 + (accuracy_threshold - 0.7) * 0.3))
        
        # Task complexity affects estimates
        if 'text' in task_data:
            complexity_factor = len(task_data['text']) / 1000
            estimated_time *= (1 + complexity_factor * 0.5)
            estimated_accuracy *= (1 - complexity_factor * 0.1)
        
        return {
            'estimated_time': max(0.01, estimated_time),
            'estimated_accuracy': max(0.1, min(1.0, estimated_accuracy)),
            'confidence': 0.7  # Confidence in the estimate
        }
    
    def record_optimization_result(self, optimization_id: str, actual_performance: Dict[str, float]):
        """Record actual performance results for optimization"""
        if optimization_id not in self.active_optimizations:
            logger.warning(f"Optimization ID not found: {optimization_id}")
            return
        
        optimization = self.active_optimizations[optimization_id]
        
        # Calculate optimization effectiveness
        estimated = self._estimate_performance(
            optimization['config'], 
            {'text': ''}  # Placeholder since we don't store original task data
        )
        
        effectiveness = {
            'time_accuracy': 1.0 - abs(estimated['estimated_time'] - actual_performance.get('processing_time', 1.0)) / estimated['estimated_time'],
            'accuracy_prediction': 1.0 - abs(estimated['estimated_accuracy'] - actual_performance.get('accuracy', 0.7)) / estimated['estimated_accuracy']
        }
        
        # Update algorithm performance
        algorithm_id = optimization['algorithm_id']
        self.contextual_adapter.update_algorithm_performance(algorithm_id, actual_performance)
        
        # Update context profile
        context_type = optimization['context_type']
        if context_type in self.contextual_adapter.context_profiles:
            profile = self.contextual_adapter.context_profiles[context_type]
            profile.historical_performance[algorithm_id] = actual_performance.get('success', 0.5)
        
        # Record in history
        result_record = {
            'optimization_id': optimization_id,
            'timestamp': time.time(),
            'context_type': context_type,
            'algorithm_id': algorithm_id,
            'estimated_performance': estimated,
            'actual_performance': actual_performance,
            'effectiveness': effectiveness,
            'duration': time.time() - optimization['start_time']
        }
        
        self.optimization_history.append(result_record)
        
        # Clean up active optimization
        del self.active_optimizations[optimization_id]
        
        logger.info(f"Recorded optimization result for {optimization_id}: "
                   f"Time accuracy: {effectiveness['time_accuracy']:.2f}, "
                   f"Accuracy prediction: {effectiveness['accuracy_prediction']:.2f}")
    
    def get_optimization_statistics(self) -> Dict[str, Any]:
        """Get comprehensive optimization statistics"""
        if not self.optimization_history:
            return {'error': 'No optimization history available'}
        
        recent_optimizations = list(self.optimization_history)[-100:]
        
        # Calculate effectiveness metrics
        time_accuracies = [opt['effectiveness']['time_accuracy'] for opt in recent_optimizations]
        accuracy_predictions = [opt['effectiveness']['accuracy_prediction'] for opt in recent_optimizations]
        
        # Algorithm performance by context
        context_performance = defaultdict(lambda: defaultdict(list))
        for opt in recent_optimizations:
            context = opt['context_type']
            algorithm = opt['algorithm_id']
            performance = opt['actual_performance'].get('accuracy', 0.5)
            context_performance[context][algorithm].append(performance)
        
        # Strategy effectiveness
        strategy_effectiveness = defaultdict(list)
        for opt in recent_optimizations:
            algorithm_id = opt['algorithm_id']
            if algorithm_id in self.contextual_adapter.algorithm_configs:
                strategy = self.contextual_adapter.algorithm_configs[algorithm_id].strategy
                overall_effectiveness = (opt['effectiveness']['time_accuracy'] + 
                                       opt['effectiveness']['accuracy_prediction']) / 2
                strategy_effectiveness[strategy.value].append(overall_effectiveness)
        
        return {
            'total_optimizations': len(self.optimization_history),
            'recent_optimizations': len(recent_optimizations),
            'average_time_accuracy': float(np.mean(time_accuracies)) if time_accuracies else 0,
            'average_accuracy_prediction': float(np.mean(accuracy_predictions)) if accuracy_predictions else 0,
            'overall_effectiveness': float(np.mean(time_accuracies + accuracy_predictions)) if time_accuracies or accuracy_predictions else 0,
            'strategy_effectiveness': {
                strategy: float(np.mean(values)) if values else 0
                for strategy, values in strategy_effectiveness.items()
            },
            'context_adapter_stats': self.contextual_adapter.get_adaptation_statistics(),
            'active_optimizations': len(self.active_optimizations)
        }
    
    def update_optimization_strategy(self, strategy: OptimizationStrategy, 
                                   config_updates: Dict[str, Any]):
        """Update optimization strategy configuration"""
        if strategy in self.optimization_strategies:
            self.optimization_strategies[strategy].update(config_updates)
            logger.info(f"Updated optimization strategy {strategy.value}: {config_updates}")
        else:
            logger.error(f"Unknown optimization strategy: {strategy}")
    
    def get_current_optimizations(self) -> Dict[str, Any]:
        """Get currently active optimizations"""
        return {
            opt_id: {
                'context_type': opt['context_type'],
                'algorithm_id': opt['algorithm_id'],
                'duration': time.time() - opt['start_time'],
                'strategy': opt['config']['strategy']
            }
            for opt_id, opt in self.active_optimizations.items()
        }