"""
Meta-Cognitive Engine

Main orchestrator for meta-cognitive learning and adaptive optimization,
integrating performance monitoring, adaptive optimization, and learning
mechanisms into a unified meta-cognitive system.
"""

import time
import logging
import asyncio
from typing import Dict, List, Any, Optional, Callable
from dataclasses import dataclass, field
from enum import Enum
import numpy as np
from collections import defaultdict, deque

from .performance_monitor import PerformanceMonitor, MetricType, PerformanceMetric
from .adaptive_optimizer import AdaptiveOptimizer, OptimizationStrategy, ContextualProfile
from .learning_engine import LearningEngine, PatternType, CognitivePattern, LearningMode, FeedbackData

logger = logging.getLogger(__name__)


class MetaCognitiveState(Enum):
    """States of meta-cognitive processing"""
    MONITORING = "monitoring"
    ANALYZING = "analyzing"
    OPTIMIZING = "optimizing"
    LEARNING = "learning"
    ADAPTING = "adapting"
    IDLE = "idle"


@dataclass
class MetaCognitiveEvent:
    """Meta-cognitive event for system self-awareness"""
    event_id: str
    event_type: str
    timestamp: float = field(default_factory=time.time)
    source_component: str = "meta_cognitive_engine"
    data: Dict[str, Any] = field(default_factory=dict)
    confidence: float = 1.0
    impact_assessment: float = 0.5
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary representation"""
        return {
            'event_id': self.event_id,
            'event_type': self.event_type,
            'timestamp': self.timestamp,
            'source_component': self.source_component,
            'data': self.data,
            'confidence': self.confidence,
            'impact_assessment': self.impact_assessment
        }


class MetaCognitiveEngine:
    """Main meta-cognitive learning and adaptive optimization engine"""
    
    def __init__(self):
        # Core components
        self.performance_monitor = PerformanceMonitor()
        self.adaptive_optimizer = AdaptiveOptimizer()
        self.learning_engine = LearningEngine()
        
        # Meta-cognitive state management
        self.current_state = MetaCognitiveState.IDLE
        self.state_history: deque = deque(maxlen=1000)
        self.meta_events: deque = deque(maxlen=1000)
        
        # Feedback loops and continuous improvement
        self.feedback_loops: Dict[str, Callable] = {}
        self.improvement_targets: Dict[str, float] = {}
        self.adaptation_strategies: Dict[str, Dict[str, Any]] = {}
        
        # System self-awareness metrics
        self.self_awareness_metrics = {
            'performance_awareness': 0.5,
            'learning_effectiveness': 0.5,
            'adaptation_capability': 0.5,
            'optimization_success_rate': 0.5,
            'meta_learning_confidence': 0.5
        }
        
        # Operating parameters
        self.meta_cognitive_active = False
        self.cycle_interval = 5.0  # seconds
        self.adaptation_threshold = 0.7
        self.learning_rate = 0.1
        
        # Statistics and monitoring
        self.operation_stats = {
            'start_time': time.time(),
            'total_cycles': 0,
            'adaptations_performed': 0,
            'optimizations_applied': 0,
            'patterns_learned': 0,
            'performance_improvements': 0
        }
        
        self._initialize_feedback_loops()
        self._initialize_adaptation_strategies()
        
        logger.info("Meta-cognitive engine initialized")
    
    def _initialize_feedback_loops(self):
        """Initialize feedback loops for continuous improvement"""
        self.feedback_loops = {
            'performance_to_optimization': self._performance_optimization_feedback,
            'optimization_to_learning': self._optimization_learning_feedback,
            'learning_to_adaptation': self._learning_adaptation_feedback,
            'adaptation_to_performance': self._adaptation_performance_feedback
        }
        
        # Set performance listeners
        self.performance_monitor.add_performance_listener(self._on_performance_metric)
    
    def _initialize_adaptation_strategies(self):
        """Initialize adaptation strategies"""
        self.adaptation_strategies = {
            'performance_based': {
                'trigger_threshold': 0.3,  # Performance below 30% triggers adaptation
                'adaptation_type': 'optimization_strategy_change',
                'cooldown_period': 300  # 5 minutes between adaptations
            },
            'learning_based': {
                'trigger_threshold': 0.8,  # High learning confidence triggers new optimizations
                'adaptation_type': 'pattern_application',
                'cooldown_period': 600  # 10 minutes between adaptations
            },
            'context_based': {
                'trigger_threshold': 0.6,  # Context change confidence
                'adaptation_type': 'algorithm_selection',
                'cooldown_period': 60   # 1 minute between adaptations
            }
        }
    
    async def start_meta_cognitive_loop(self):
        """Start the main meta-cognitive processing loop"""
        self.meta_cognitive_active = True
        self.learning_engine.start_learning()
        self.performance_monitor.start_monitoring()
        
        logger.info("Starting meta-cognitive processing loop")
        
        while self.meta_cognitive_active:
            try:
                await self._execute_meta_cognitive_cycle()
                await asyncio.sleep(self.cycle_interval)
            except Exception as e:
                logger.error(f"Error in meta-cognitive loop: {e}")
                await asyncio.sleep(self.cycle_interval)
    
    async def _execute_meta_cognitive_cycle(self):
        """Execute one meta-cognitive processing cycle"""
        cycle_start = time.time()
        
        # Update state to analyzing
        self._update_meta_cognitive_state(MetaCognitiveState.ANALYZING)
        
        # Gather current system state
        system_state = await self._gather_system_state()
        
        # Perform meta-cognitive analysis
        analysis_results = await self._perform_meta_cognitive_analysis(system_state)
        
        # Decide on adaptations
        adaptations = await self._decide_adaptations(analysis_results)
        
        # Apply adaptations
        if adaptations:
            self._update_meta_cognitive_state(MetaCognitiveState.ADAPTING)
            adaptation_results = await self._apply_adaptations(adaptations)
            self.operation_stats['adaptations_performed'] += len(adaptation_results)
        
        # Execute learning cycle
        self._update_meta_cognitive_state(MetaCognitiveState.LEARNING)
        learning_results = await self.learning_engine.learning_cycle()
        
        # Update self-awareness metrics
        self._update_self_awareness_metrics(system_state, analysis_results, learning_results)
        
        # Record meta-cognitive event
        self._record_meta_cognitive_event(
            'cycle_completed',
            {
                'cycle_duration': time.time() - cycle_start,
                'system_state': system_state,
                'adaptations_applied': len(adaptations) if adaptations else 0,
                'learning_results': learning_results
            }
        )
        
        self.operation_stats['total_cycles'] += 1
        self._update_meta_cognitive_state(MetaCognitiveState.MONITORING)
        
        logger.debug(f"Meta-cognitive cycle completed in {time.time() - cycle_start:.3f}s")
    
    async def _gather_system_state(self) -> Dict[str, Any]:
        """Gather comprehensive system state information"""
        try:
            # Performance metrics
            performance_summary = self.performance_monitor.get_performance_summary()
            
            # Optimization statistics
            optimization_stats = self.adaptive_optimizer.get_optimization_statistics()
            
            # Learning status
            learning_status = self.learning_engine.get_learning_status()
            
            # Current meta-cognitive state
            meta_state = {
                'current_state': self.current_state.value,
                'self_awareness_metrics': self.self_awareness_metrics.copy(),
                'operation_stats': self.operation_stats.copy()
            }
            
            return {
                'timestamp': time.time(),
                'performance': performance_summary,
                'optimization': optimization_stats,
                'learning': learning_status,
                'meta_cognitive': meta_state
            }
            
        except Exception as e:
            logger.error(f"Error gathering system state: {e}")
            return {'error': str(e), 'timestamp': time.time()}
    
    async def _perform_meta_cognitive_analysis(self, system_state: Dict[str, Any]) -> Dict[str, Any]:
        """Perform meta-cognitive analysis of system state"""
        analysis = {
            'performance_trends': {},
            'optimization_effectiveness': 0.5,
            'learning_progress': 0.5,
            'adaptation_opportunities': [],
            'system_health': 'unknown'
        }
        
        try:
            # Analyze performance trends
            performance_data = system_state.get('performance', {})
            if 'metrics' in performance_data:
                analysis['performance_trends'] = self._analyze_performance_trends(performance_data['metrics'])
                analysis['system_health'] = performance_data.get('overall_health', 'unknown')
            
            # Analyze optimization effectiveness
            optimization_data = system_state.get('optimization', {})
            if 'overall_effectiveness' in optimization_data:
                analysis['optimization_effectiveness'] = optimization_data['overall_effectiveness']
            
            # Analyze learning progress
            learning_data = system_state.get('learning', {})
            if 'pattern_learner' in learning_data:
                pattern_stats = learning_data['pattern_learner']
                if 'average_effectiveness' in pattern_stats:
                    analysis['learning_progress'] = pattern_stats['average_effectiveness']
            
            # Identify adaptation opportunities
            analysis['adaptation_opportunities'] = self._identify_adaptation_opportunities(system_state)
            
            return analysis
            
        except Exception as e:
            logger.error(f"Error in meta-cognitive analysis: {e}")
            analysis['error'] = str(e)
            return analysis
    
    def _analyze_performance_trends(self, metrics: Dict[str, Any]) -> Dict[str, str]:
        """Analyze performance trends from metrics"""
        trends = {}
        
        for metric_name, metric_data in metrics.items():
            if isinstance(metric_data, dict) and 'trend' in metric_data:
                trends[metric_name] = metric_data['trend']
            else:
                trends[metric_name] = 'unknown'
        
        return trends
    
    def _identify_adaptation_opportunities(self, system_state: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Identify opportunities for system adaptation"""
        opportunities = []
        
        # Performance-based opportunities
        performance_data = system_state.get('performance', {})
        if performance_data.get('overall_health') == 'poor':
            opportunities.append({
                'type': 'performance_improvement',
                'priority': 'high',
                'action': 'optimize_algorithms',
                'confidence': 0.8
            })
        
        # Learning-based opportunities
        learning_data = system_state.get('learning', {})
        pattern_stats = learning_data.get('pattern_learner', {})
        if pattern_stats.get('average_effectiveness', 0) > 0.8:
            opportunities.append({
                'type': 'pattern_application',
                'priority': 'medium',
                'action': 'apply_successful_patterns',
                'confidence': 0.7
            })
        
        # Optimization-based opportunities
        optimization_data = system_state.get('optimization', {})
        if optimization_data.get('overall_effectiveness', 0) < 0.5:
            opportunities.append({
                'type': 'optimization_strategy_change',
                'priority': 'high',
                'action': 'revise_optimization_strategies',
                'confidence': 0.6
            })
        
        return opportunities
    
    async def _decide_adaptations(self, analysis_results: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Decide on adaptations based on analysis results"""
        adaptations = []
        
        opportunities = analysis_results.get('adaptation_opportunities', [])
        
        for opportunity in opportunities:
            # Check if adaptation should be triggered
            confidence = opportunity.get('confidence', 0.5)
            priority = opportunity.get('priority', 'low')
            
            should_adapt = False
            
            if priority == 'high' and confidence > 0.6:
                should_adapt = True
            elif priority == 'medium' and confidence > 0.7:
                should_adapt = True
            elif priority == 'low' and confidence > 0.8:
                should_adapt = True
            
            if should_adapt:
                adaptation = {
                    'adaptation_id': f"adapt_{int(time.time()*1000)}",
                    'type': opportunity['type'],
                    'action': opportunity['action'],
                    'priority': priority,
                    'confidence': confidence,
                    'timestamp': time.time()
                }
                adaptations.append(adaptation)
        
        # Limit concurrent adaptations
        adaptations = adaptations[:3]  # Max 3 adaptations per cycle
        
        return adaptations
    
    async def _apply_adaptations(self, adaptations: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Apply decided adaptations"""
        results = []
        
        for adaptation in adaptations:
            try:
                result = await self._apply_single_adaptation(adaptation)
                results.append(result)
                
                # Record adaptation event
                self._record_meta_cognitive_event(
                    'adaptation_applied',
                    {
                        'adaptation': adaptation,
                        'result': result
                    }
                )
                
            except Exception as e:
                logger.error(f"Error applying adaptation {adaptation['adaptation_id']}: {e}")
                results.append({
                    'adaptation_id': adaptation['adaptation_id'],
                    'status': 'error',
                    'error': str(e)
                })
        
        return results
    
    async def _apply_single_adaptation(self, adaptation: Dict[str, Any]) -> Dict[str, Any]:
        """Apply a single adaptation"""
        adaptation_type = adaptation['type']
        action = adaptation['action']
        
        if adaptation_type == 'performance_improvement':
            return await self._apply_performance_improvement(adaptation)
        elif adaptation_type == 'pattern_application':
            return await self._apply_pattern_application(adaptation)
        elif adaptation_type == 'optimization_strategy_change':
            return await self._apply_optimization_strategy_change(adaptation)
        else:
            return {
                'adaptation_id': adaptation['adaptation_id'],
                'status': 'unknown_type',
                'type': adaptation_type
            }
    
    async def _apply_performance_improvement(self, adaptation: Dict[str, Any]) -> Dict[str, Any]:
        """Apply performance improvement adaptation"""
        # Get current poor-performing components
        performance_summary = self.performance_monitor.get_performance_summary()
        
        improvements_applied = 0
        
        # Focus on speed optimization for slow components
        if performance_summary.get('metrics', {}).get('processing_time', {}).get('trend') == 'degrading':
            # Switch to speed-optimized strategies
            for strategy in [OptimizationStrategy.SPEED_OPTIMIZED]:
                self.adaptive_optimizer.update_optimization_strategy(strategy, {
                    'timeout_multiplier': 0.5,
                    'early_termination': True,
                    'parallel_processing': True
                })
                improvements_applied += 1
        
        return {
            'adaptation_id': adaptation['adaptation_id'],
            'status': 'applied',
            'improvements_applied': improvements_applied
        }
    
    async def _apply_pattern_application(self, adaptation: Dict[str, Any]) -> Dict[str, Any]:
        """Apply successful pattern application adaptation"""
        # Get best patterns from learning engine
        best_patterns = self.learning_engine.pattern_learner.get_best_patterns(
            pattern_type=PatternType.OPTIMIZATION_PATTERN,
            limit=3
        )
        
        patterns_applied = 0
        
        for pattern in best_patterns:
            if pattern.effectiveness_score > 0.8:
                # Create optimization configuration from pattern
                if 'optimization_params' in pattern.pattern_data:
                    params = pattern.pattern_data['optimization_params']
                    
                    # Apply pattern to optimization strategies
                    for strategy in OptimizationStrategy:
                        self.adaptive_optimizer.update_optimization_strategy(strategy, params)
                    
                    patterns_applied += 1
        
        return {
            'adaptation_id': adaptation['adaptation_id'],
            'status': 'applied',
            'patterns_applied': patterns_applied
        }
    
    async def _apply_optimization_strategy_change(self, adaptation: Dict[str, Any]) -> Dict[str, Any]:
        """Apply optimization strategy change adaptation"""
        # Switch to more conservative strategies if current ones are not effective
        strategy_changes = 0
        
        # Get optimization statistics
        opt_stats = self.adaptive_optimizer.get_optimization_statistics()
        
        if opt_stats.get('overall_effectiveness', 0) < 0.5:
            # Switch to balanced strategy for better stability
            self.adaptive_optimizer.update_optimization_strategy(
                OptimizationStrategy.BALANCED,
                {
                    'timeout_multiplier': 1.0,
                    'accuracy_threshold': 0.7,
                    'validation_rounds': 1
                }
            )
            strategy_changes += 1
        
        return {
            'adaptation_id': adaptation['adaptation_id'],
            'status': 'applied',
            'strategy_changes': strategy_changes
        }
    
    def _update_self_awareness_metrics(self, system_state: Dict[str, Any], 
                                     analysis_results: Dict[str, Any],
                                     learning_results: Dict[str, Any]):
        """Update self-awareness metrics based on system analysis"""
        # Performance awareness - how well we understand our performance
        performance_data = system_state.get('performance', {})
        if 'overall_health' in performance_data and performance_data['overall_health'] != 'unknown':
            self.self_awareness_metrics['performance_awareness'] = min(1.0, 
                self.self_awareness_metrics['performance_awareness'] + 0.05)
        
        # Learning effectiveness - how well our learning is working
        if learning_results.get('pattern_optimization', {}).get('optimized_count', 0) > 0:
            self.self_awareness_metrics['learning_effectiveness'] = min(1.0,
                self.self_awareness_metrics['learning_effectiveness'] + 0.1)
        
        # Adaptation capability - how well we adapt to changes
        opportunities = analysis_results.get('adaptation_opportunities', [])
        if opportunities:
            self.self_awareness_metrics['adaptation_capability'] = min(1.0,
                self.self_awareness_metrics['adaptation_capability'] + 0.05)
        
        # Optimization success rate - how well our optimizations work
        opt_effectiveness = analysis_results.get('optimization_effectiveness', 0.5)
        self.self_awareness_metrics['optimization_success_rate'] = (
            0.9 * self.self_awareness_metrics['optimization_success_rate'] + 
            0.1 * opt_effectiveness
        )
        
        # Meta-learning confidence - overall confidence in our meta-cognitive abilities
        avg_awareness = np.mean(list(self.self_awareness_metrics.values()))
        self.self_awareness_metrics['meta_learning_confidence'] = avg_awareness
    
    def _update_meta_cognitive_state(self, new_state: MetaCognitiveState):
        """Update meta-cognitive state"""
        old_state = self.current_state
        self.current_state = new_state
        
        self.state_history.append({
            'timestamp': time.time(),
            'old_state': old_state.value,
            'new_state': new_state.value
        })
        
        logger.debug(f"Meta-cognitive state changed: {old_state.value} -> {new_state.value}")
    
    def _record_meta_cognitive_event(self, event_type: str, data: Dict[str, Any]):
        """Record a meta-cognitive event"""
        event = MetaCognitiveEvent(
            event_id=f"event_{int(time.time()*1000)}",
            event_type=event_type,
            data=data
        )
        
        self.meta_events.append(event)
        
        logger.debug(f"Recorded meta-cognitive event: {event_type}")
    
    def _on_performance_metric(self, metric: PerformanceMetric):
        """Handle performance metric updates"""
        # Trigger immediate analysis for critical performance issues
        if metric.metric_type == MetricType.PROCESSING_TIME and metric.value > 5.0:
            # Very slow processing detected
            self._record_meta_cognitive_event(
                'performance_alert',
                {
                    'metric_type': metric.metric_type.value,
                    'value': metric.value,
                    'component': metric.component,
                    'alert_level': 'high'
                }
            )
        elif metric.metric_type == MetricType.ACCURACY and metric.value < 0.3:
            # Very low accuracy detected
            self._record_meta_cognitive_event(
                'accuracy_alert',
                {
                    'metric_type': metric.metric_type.value,
                    'value': metric.value,
                    'component': metric.component,
                    'alert_level': 'critical'
                }
            )
    
    # Feedback loop implementations
    async def _performance_optimization_feedback(self, performance_data: Dict[str, Any]):
        """Feedback loop from performance monitoring to optimization"""
        if performance_data.get('overall_health') == 'poor':
            # Submit feedback to trigger optimization changes
            feedback = FeedbackData(
                feedback_id=f"perf_opt_{int(time.time()*1000)}",
                source_component='performance_monitor',
                target_pattern='optimization_strategy',
                feedback_type='negative',
                feedback_value=0.3,
                context={'trigger': 'poor_performance', 'data': performance_data}
            )
            
            self.learning_engine.feedback_processor.submit_feedback(feedback)
    
    async def _optimization_learning_feedback(self, optimization_data: Dict[str, Any]):
        """Feedback loop from optimization to learning"""
        if optimization_data.get('overall_effectiveness', 0) > 0.8:
            # Create pattern from successful optimization
            pattern_data = {
                'optimization_effectiveness': optimization_data['overall_effectiveness'],
                'successful_strategies': optimization_data.get('strategy_effectiveness', {}),
                'context_performance': optimization_data.get('context_adapter_stats', {})
            }
            
            self.learning_engine.create_pattern_from_data(
                PatternType.OPTIMIZATION_PATTERN,
                pattern_data,
                context='optimization_success'
            )
    
    async def _learning_adaptation_feedback(self, learning_data: Dict[str, Any]):
        """Feedback loop from learning to adaptation"""
        pattern_stats = learning_data.get('pattern_learner', {})
        if pattern_stats.get('average_effectiveness', 0) > 0.8:
            # High learning effectiveness suggests good adaptation capability
            self.adaptation_strategies['learning_based']['trigger_threshold'] = 0.7
        else:
            # Lower threshold to trigger more adaptations
            self.adaptation_strategies['learning_based']['trigger_threshold'] = 0.9
    
    async def _adaptation_performance_feedback(self, adaptation_results: List[Dict[str, Any]]):
        """Feedback loop from adaptation back to performance monitoring"""
        successful_adaptations = [r for r in adaptation_results if r.get('status') == 'applied']
        
        if successful_adaptations:
            # Set baseline expectations for improved performance
            for metric_type in [MetricType.PROCESSING_TIME, MetricType.ACCURACY, MetricType.EFFICIENCY]:
                current_summary = self.performance_monitor.get_performance_summary()
                if metric_type.value in current_summary.get('metrics', {}):
                    current_value = current_summary['metrics'][metric_type.value].get('mean', 0.5)
                    # Expect 10% improvement after adaptation
                    improved_baseline = current_value * 1.1 if metric_type == MetricType.ACCURACY else current_value * 0.9
                    self.performance_monitor.set_baseline(metric_type, improved_baseline)
    
    # Public interface methods
    def process_cognitive_task(self, task_data: Dict[str, Any], context: str = 'general') -> Dict[str, Any]:
        """Process a cognitive task with meta-cognitive optimization"""
        task_start = time.time()
        
        try:
            # Record task initiation
            self._record_meta_cognitive_event(
                'task_initiated',
                {'context': context, 'task_complexity': len(str(task_data))}
            )
            
            # Get optimized configuration for this context
            optimization_result = self.adaptive_optimizer.optimize_for_context(context, task_data)
            
            if 'error' in optimization_result:
                return optimization_result
            
            optimized_config = optimization_result['optimized_config']
            optimization_id = optimization_result['optimization_id']
            
            # Record performance metrics
            processing_time = time.time() - task_start
            self.performance_monitor.record_processing_time(
                operation='cognitive_task',
                duration=processing_time,
                component='meta_cognitive_engine'
            )
            
            # Submit performance feedback to learning system
            performance_data = {
                'processing_time': processing_time,
                'success': 1.0,  # Assume success for now
                'effectiveness': 0.8  # Default effectiveness score
            }
            
            self.adaptive_optimizer.record_optimization_result(optimization_id, performance_data)
            self.learning_engine.submit_performance_feedback(
                optimized_config['algorithm_id'],
                performance_data,
                context
            )
            
            return {
                'status': 'processed',
                'optimization_id': optimization_id,
                'optimized_config': optimized_config,
                'processing_time': processing_time,
                'meta_cognitive_state': self.current_state.value
            }
            
        except Exception as e:
            logger.error(f"Error processing cognitive task: {e}")
            return {'error': str(e)}
    
    def get_meta_cognitive_status(self) -> Dict[str, Any]:
        """Get comprehensive meta-cognitive system status"""
        try:
            return {
                'meta_cognitive_active': self.meta_cognitive_active,
                'current_state': self.current_state.value,
                'self_awareness_metrics': self.self_awareness_metrics.copy(),
                'operation_stats': self.operation_stats.copy(),
                'performance_monitor': self.performance_monitor.get_monitoring_status(),
                'adaptive_optimizer': self.adaptive_optimizer.get_optimization_statistics(),
                'learning_engine': self.learning_engine.get_learning_status(),
                'recent_events': [event.to_dict() for event in list(self.meta_events)[-10:]],
                'state_history': list(self.state_history)[-10:],
                'feedback_loops_active': len(self.feedback_loops),
                'adaptation_strategies': len(self.adaptation_strategies),
                'uptime_seconds': time.time() - self.operation_stats['start_time']
            }
        except Exception as e:
            logger.error(f"Error getting meta-cognitive status: {e}")
            return {'error': str(e)}
    
    def update_meta_cognitive_parameters(self, parameters: Dict[str, Any]) -> bool:
        """Update meta-cognitive engine parameters"""
        try:
            valid_parameters = {
                'cycle_interval', 'adaptation_threshold', 'learning_rate'
            }
            
            for param, value in parameters.items():
                if param in valid_parameters:
                    setattr(self, param, value)
                    logger.info(f"Updated meta-cognitive parameter {param} to {value}")
            
            return True
            
        except Exception as e:
            logger.error(f"Error updating meta-cognitive parameters: {e}")
            return False
    
    def stop_meta_cognitive_loop(self):
        """Stop the meta-cognitive processing loop"""
        self.meta_cognitive_active = False
        self.learning_engine.stop_learning()
        self.performance_monitor.stop_monitoring()
        
        self._record_meta_cognitive_event(
            'system_stopped',
            {'uptime_seconds': time.time() - self.operation_stats['start_time']}
        )
        
        logger.info("Meta-cognitive processing loop stopped")