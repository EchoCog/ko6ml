"""
Learning Engine for Cognitive Pattern Optimization

Implements learning mechanisms for cognitive pattern optimization and
feedback processing to continuously improve system performance.
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


class LearningMode(Enum):
    """Types of learning modes"""
    SUPERVISED = "supervised"
    UNSUPERVISED = "unsupervised"
    REINFORCEMENT = "reinforcement"
    TRANSFER = "transfer"
    META = "meta"


class PatternType(Enum):
    """Types of cognitive patterns"""
    REASONING_PATTERN = "reasoning_pattern"
    ATTENTION_PATTERN = "attention_pattern"
    PROCESSING_PATTERN = "processing_pattern"
    OPTIMIZATION_PATTERN = "optimization_pattern"
    BEHAVIORAL_PATTERN = "behavioral_pattern"


@dataclass
class CognitivePattern:
    """Cognitive pattern representation"""
    pattern_id: str
    pattern_type: PatternType
    pattern_data: Dict[str, Any]
    effectiveness_score: float = 0.5
    usage_count: int = 0
    success_rate: float = 1.0
    learning_confidence: float = 0.5
    context_applicability: List[str] = field(default_factory=list)
    creation_time: float = field(default_factory=time.time)
    last_updated: float = field(default_factory=time.time)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary representation"""
        return {
            'pattern_id': self.pattern_id,
            'pattern_type': self.pattern_type.value,
            'pattern_data': self.pattern_data,
            'effectiveness_score': self.effectiveness_score,
            'usage_count': self.usage_count,
            'success_rate': self.success_rate,
            'learning_confidence': self.learning_confidence,
            'context_applicability': self.context_applicability,
            'creation_time': self.creation_time,
            'last_updated': self.last_updated
        }


@dataclass
class FeedbackData:
    """Feedback data for learning"""
    feedback_id: str
    source_component: str
    target_pattern: str
    feedback_type: str  # "positive", "negative", "neutral"
    feedback_value: float
    context: Dict[str, Any] = field(default_factory=dict)
    timestamp: float = field(default_factory=time.time)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary representation"""
        return {
            'feedback_id': self.feedback_id,
            'source_component': self.source_component,
            'target_pattern': self.target_pattern,
            'feedback_type': self.feedback_type,
            'feedback_value': self.feedback_value,
            'context': self.context,
            'timestamp': self.timestamp
        }


class PatternLearner:
    """Pattern learning and optimization system"""
    
    def __init__(self):
        self.learned_patterns: Dict[str, CognitivePattern] = {}
        self.pattern_relationships: Dict[str, Dict[str, float]] = defaultdict(dict)
        self.learning_history: deque = deque(maxlen=1000)
        self.pattern_usage_stats: Dict[str, Dict[str, Any]] = defaultdict(dict)
        
        # Learning parameters
        self.learning_rate = 0.1
        self.forgetting_factor = 0.95
        self.confidence_threshold = 0.7
        self.effectiveness_decay = 0.99
        
        logger.info("Pattern learner initialized")
    
    def learn_pattern(self, pattern: CognitivePattern, 
                     learning_mode: LearningMode = LearningMode.UNSUPERVISED) -> bool:
        """Learn a new cognitive pattern"""
        try:
            # Check if pattern already exists
            if pattern.pattern_id in self.learned_patterns:
                return self._update_existing_pattern(pattern, learning_mode)
            
            # Add new pattern
            self.learned_patterns[pattern.pattern_id] = pattern
            
            # Initialize usage statistics
            self.pattern_usage_stats[pattern.pattern_id] = {
                'learning_mode': learning_mode.value,
                'first_learned': time.time(),
                'contexts_used': set(),
                'performance_history': []
            }
            
            # Record learning event
            self.learning_history.append({
                'timestamp': time.time(),
                'action': 'pattern_learned',
                'pattern_id': pattern.pattern_id,
                'pattern_type': pattern.pattern_type.value,
                'learning_mode': learning_mode.value,
                'initial_effectiveness': pattern.effectiveness_score
            })
            
            logger.info(f"Learned new pattern: {pattern.pattern_id} "
                       f"({pattern.pattern_type.value}, {learning_mode.value})")
            
            return True
            
        except Exception as e:
            logger.error(f"Error learning pattern: {e}")
            return False
    
    def _update_existing_pattern(self, pattern: CognitivePattern, 
                               learning_mode: LearningMode) -> bool:
        """Update existing pattern with new information"""
        existing = self.learned_patterns[pattern.pattern_id]
        
        # Update effectiveness using exponential moving average
        existing.effectiveness_score = (
            self.learning_rate * pattern.effectiveness_score + 
            (1 - self.learning_rate) * existing.effectiveness_score
        )
        
        # Update pattern data selectively
        for key, value in pattern.pattern_data.items():
            if key in existing.pattern_data:
                # Blend numerical values
                if isinstance(value, (int, float)) and isinstance(existing.pattern_data[key], (int, float)):
                    existing.pattern_data[key] = (
                        self.learning_rate * value + 
                        (1 - self.learning_rate) * existing.pattern_data[key]
                    )
                else:
                    existing.pattern_data[key] = value  # Replace non-numerical values
            else:
                existing.pattern_data[key] = value  # Add new keys
        
        # Update metadata
        existing.last_updated = time.time()
        existing.learning_confidence = min(1.0, existing.learning_confidence + 0.1)
        
        # Merge context applicability
        for context in pattern.context_applicability:
            if context not in existing.context_applicability:
                existing.context_applicability.append(context)
        
        logger.debug(f"Updated existing pattern: {pattern.pattern_id}")
        return True
    
    def optimize_pattern(self, pattern_id: str, optimization_target: str = "effectiveness") -> bool:
        """Optimize a learned pattern"""
        if pattern_id not in self.learned_patterns:
            logger.warning(f"Pattern not found for optimization: {pattern_id}")
            return False
        
        try:
            pattern = self.learned_patterns[pattern_id]
            
            if optimization_target == "effectiveness":
                # Analyze usage statistics to optimize effectiveness
                stats = self.pattern_usage_stats[pattern_id]
                performance_history = stats.get('performance_history', [])
                
                if len(performance_history) >= 5:
                    # Find patterns in performance data
                    recent_performance = performance_history[-10:]
                    avg_performance = np.mean([p['effectiveness'] for p in recent_performance])
                    
                    # Adjust pattern parameters based on performance trends
                    if avg_performance > pattern.effectiveness_score:
                        # Performance is improving
                        pattern.learning_confidence = min(1.0, pattern.learning_confidence + 0.05)
                    else:
                        # Performance is declining, explore variations
                        self._explore_pattern_variations(pattern)
            
            elif optimization_target == "applicability":
                # Optimize context applicability
                self._optimize_context_applicability(pattern)
            
            elif optimization_target == "efficiency":
                # Optimize computational efficiency
                self._optimize_pattern_efficiency(pattern)
            
            pattern.last_updated = time.time()
            
            # Record optimization event
            self.learning_history.append({
                'timestamp': time.time(),
                'action': 'pattern_optimized',
                'pattern_id': pattern_id,
                'optimization_target': optimization_target,
                'new_effectiveness': pattern.effectiveness_score
            })
            
            logger.info(f"Optimized pattern {pattern_id} for {optimization_target}")
            return True
            
        except Exception as e:
            logger.error(f"Error optimizing pattern: {e}")
            return False
    
    def _explore_pattern_variations(self, pattern: CognitivePattern):
        """Explore variations of a pattern to improve performance"""
        # Simple parameter exploration
        for key, value in pattern.pattern_data.items():
            if isinstance(value, (int, float)):
                # Try small variations
                variations = [value * 0.9, value * 1.1, value * 0.95, value * 1.05]
                
                # Choose variation that might improve effectiveness
                # (In a full implementation, this would test variations)
                best_variation = np.random.choice(variations)
                pattern.pattern_data[key] = best_variation
    
    def _optimize_context_applicability(self, pattern: CognitivePattern):
        """Optimize pattern's context applicability"""
        stats = self.pattern_usage_stats[pattern.pattern_id]
        contexts_used = stats.get('contexts_used', set())
        
        # Add successful contexts to applicability list
        for context in contexts_used:
            if context not in pattern.context_applicability:
                pattern.context_applicability.append(context)
        
        # Remove contexts where pattern performed poorly
        # (Implementation would track per-context performance)
    
    def _optimize_pattern_efficiency(self, pattern: CognitivePattern):
        """Optimize pattern computational efficiency"""
        # Simplify pattern data to reduce computational overhead
        if 'complexity_factor' in pattern.pattern_data:
            current_complexity = pattern.pattern_data['complexity_factor']
            pattern.pattern_data['complexity_factor'] = max(0.1, current_complexity * 0.95)
    
    def record_pattern_usage(self, pattern_id: str, context: str, 
                           performance_data: Dict[str, float]):
        """Record pattern usage and performance"""
        if pattern_id not in self.learned_patterns:
            return
        
        pattern = self.learned_patterns[pattern_id]
        stats = self.pattern_usage_stats[pattern_id]
        
        # Update usage count
        pattern.usage_count += 1
        
        # Update success rate
        success = performance_data.get('success', 0.5)
        pattern.success_rate = (pattern.success_rate * (pattern.usage_count - 1) + success) / pattern.usage_count
        
        # Update effectiveness based on performance
        effectiveness = performance_data.get('effectiveness', 0.5)
        pattern.effectiveness_score = (
            self.learning_rate * effectiveness + 
            (1 - self.learning_rate) * pattern.effectiveness_score
        )
        
        # Record context usage
        stats['contexts_used'].add(context)
        
        # Record performance history
        stats['performance_history'].append({
            'timestamp': time.time(),
            'context': context,
            'effectiveness': effectiveness,
            'success': success,
            'performance_data': performance_data
        })
        
        # Keep only recent history
        if len(stats['performance_history']) > 100:
            stats['performance_history'] = stats['performance_history'][-100:]
        
        # Update pattern relationships
        self._update_pattern_relationships(pattern_id, context, effectiveness)
        
        pattern.last_updated = time.time()
    
    def _update_pattern_relationships(self, pattern_id: str, context: str, effectiveness: float):
        """Update relationships between patterns"""
        # Find other patterns used in similar contexts
        for other_pattern_id, other_pattern in self.learned_patterns.items():
            if other_pattern_id != pattern_id and context in other_pattern.context_applicability:
                # Update relationship strength based on co-occurrence and effectiveness
                current_strength = self.pattern_relationships[pattern_id].get(other_pattern_id, 0.0)
                new_strength = 0.9 * current_strength + 0.1 * effectiveness
                self.pattern_relationships[pattern_id][other_pattern_id] = new_strength
    
    def get_best_patterns(self, context: str = None, pattern_type: PatternType = None, 
                         limit: int = 10) -> List[CognitivePattern]:
        """Get best patterns for given criteria"""
        candidates = list(self.learned_patterns.values())
        
        # Filter by context if specified
        if context:
            candidates = [p for p in candidates if context in p.context_applicability or not p.context_applicability]
        
        # Filter by pattern type if specified
        if pattern_type:
            candidates = [p for p in candidates if p.pattern_type == pattern_type]
        
        # Sort by effectiveness score
        candidates.sort(key=lambda p: p.effectiveness_score, reverse=True)
        
        return candidates[:limit]
    
    def get_pattern_recommendations(self, context: str, current_patterns: List[str]) -> List[str]:
        """Get pattern recommendations based on relationships"""
        recommendations = defaultdict(float)
        
        for pattern_id in current_patterns:
            if pattern_id in self.pattern_relationships:
                for related_pattern, strength in self.pattern_relationships[pattern_id].items():
                    if related_pattern not in current_patterns:
                        recommendations[related_pattern] += strength
        
        # Sort by recommendation strength
        sorted_recommendations = sorted(recommendations.items(), key=lambda x: x[1], reverse=True)
        
        return [pattern_id for pattern_id, _ in sorted_recommendations[:5]]
    
    def get_learning_statistics(self) -> Dict[str, Any]:
        """Get comprehensive learning statistics"""
        total_patterns = len(self.learned_patterns)
        
        if total_patterns == 0:
            return {'error': 'No patterns learned yet'}
        
        # Calculate statistics
        effectiveness_scores = [p.effectiveness_score for p in self.learned_patterns.values()]
        success_rates = [p.success_rate for p in self.learned_patterns.values()]
        usage_counts = [p.usage_count for p in self.learned_patterns.values()]
        
        # Pattern type distribution
        type_distribution = defaultdict(int)
        for pattern in self.learned_patterns.values():
            type_distribution[pattern.pattern_type.value] += 1
        
        # Learning mode distribution
        mode_distribution = defaultdict(int)
        for stats in self.pattern_usage_stats.values():
            mode_distribution[stats.get('learning_mode', 'unknown')] += 1
        
        # Recent learning activity
        recent_activity = [event for event in self.learning_history 
                          if time.time() - event['timestamp'] < 3600]  # Last hour
        
        return {
            'total_patterns': total_patterns,
            'average_effectiveness': float(np.mean(effectiveness_scores)),
            'average_success_rate': float(np.mean(success_rates)),
            'average_usage_count': float(np.mean(usage_counts)),
            'pattern_type_distribution': dict(type_distribution),
            'learning_mode_distribution': dict(mode_distribution),
            'recent_learning_events': len(recent_activity),
            'total_learning_events': len(self.learning_history),
            'pattern_relationships': len(self.pattern_relationships),
            'learning_parameters': {
                'learning_rate': self.learning_rate,
                'forgetting_factor': self.forgetting_factor,
                'confidence_threshold': self.confidence_threshold
            }
        }


class FeedbackProcessor:
    """Feedback processing system for learning improvement"""
    
    def __init__(self, pattern_learner: PatternLearner):
        self.pattern_learner = pattern_learner
        self.feedback_queue: deque = deque(maxlen=1000)
        self.feedback_processors: Dict[str, Callable] = {}
        self.processing_active = True
        self.feedback_stats: Dict[str, Any] = defaultdict(int)
        
        # Initialize default feedback processors
        self._initialize_feedback_processors()
        
        logger.info("Feedback processor initialized")
    
    def _initialize_feedback_processors(self):
        """Initialize default feedback processors"""
        self.feedback_processors = {
            'performance_feedback': self._process_performance_feedback,
            'user_feedback': self._process_user_feedback,
            'system_feedback': self._process_system_feedback,
            'adaptive_feedback': self._process_adaptive_feedback
        }
    
    def submit_feedback(self, feedback: FeedbackData) -> bool:
        """Submit feedback for processing"""
        try:
            if not self.processing_active:
                return False
            
            self.feedback_queue.append(feedback)
            self.feedback_stats['total_submitted'] += 1
            self.feedback_stats[f'type_{feedback.feedback_type}'] += 1
            
            logger.debug(f"Submitted feedback: {feedback.feedback_id}")
            return True
            
        except Exception as e:
            logger.error(f"Error submitting feedback: {e}")
            return False
    
    async def process_feedback_batch(self, batch_size: int = 10) -> Dict[str, Any]:
        """Process a batch of feedback"""
        if not self.feedback_queue:
            return {'processed': 0, 'message': 'No feedback to process'}
        
        processed_count = 0
        processing_results = []
        
        # Process up to batch_size feedback items
        for _ in range(min(batch_size, len(self.feedback_queue))):
            if not self.feedback_queue:
                break
            
            feedback = self.feedback_queue.popleft()
            
            try:
                result = await self._process_single_feedback(feedback)
                processing_results.append(result)
                processed_count += 1
                
            except Exception as e:
                logger.error(f"Error processing feedback {feedback.feedback_id}: {e}")
                processing_results.append({'feedback_id': feedback.feedback_id, 'error': str(e)})
        
        self.feedback_stats['total_processed'] += processed_count
        
        return {
            'processed': processed_count,
            'results': processing_results,
            'queue_size': len(self.feedback_queue)
        }
    
    async def _process_single_feedback(self, feedback: FeedbackData) -> Dict[str, Any]:
        """Process a single feedback item"""
        # Determine processor based on source component
        processor_key = f"{feedback.source_component}_feedback"
        if processor_key not in self.feedback_processors:
            processor_key = 'system_feedback'  # Default processor
        
        processor = self.feedback_processors[processor_key]
        result = await processor(feedback)
        
        logger.debug(f"Processed feedback {feedback.feedback_id} with {processor_key}")
        
        return {
            'feedback_id': feedback.feedback_id,
            'processor_used': processor_key,
            'result': result
        }
    
    async def _process_performance_feedback(self, feedback: FeedbackData) -> Dict[str, Any]:
        """Process performance-related feedback"""
        try:
            # Update pattern performance based on feedback
            if feedback.target_pattern in self.pattern_learner.learned_patterns:
                pattern = self.pattern_learner.learned_patterns[feedback.target_pattern]
                
                # Adjust effectiveness based on feedback
                feedback_impact = feedback.feedback_value * 0.1  # 10% impact per feedback
                
                if feedback.feedback_type == 'positive':
                    pattern.effectiveness_score = min(1.0, pattern.effectiveness_score + feedback_impact)
                elif feedback.feedback_type == 'negative':
                    pattern.effectiveness_score = max(0.0, pattern.effectiveness_score - feedback_impact)
                
                # Update learning confidence
                pattern.learning_confidence = min(1.0, pattern.learning_confidence + 0.05)
                pattern.last_updated = time.time()
                
                return {'status': 'updated', 'new_effectiveness': pattern.effectiveness_score}
            else:
                return {'status': 'pattern_not_found', 'pattern_id': feedback.target_pattern}
                
        except Exception as e:
            return {'status': 'error', 'error': str(e)}
    
    async def _process_user_feedback(self, feedback: FeedbackData) -> Dict[str, Any]:
        """Process user-provided feedback"""
        try:
            # User feedback is typically more authoritative
            if feedback.target_pattern in self.pattern_learner.learned_patterns:
                pattern = self.pattern_learner.learned_patterns[feedback.target_pattern]
                
                # Higher impact for user feedback
                feedback_impact = feedback.feedback_value * 0.2
                
                if feedback.feedback_type == 'positive':
                    pattern.effectiveness_score = min(1.0, pattern.effectiveness_score + feedback_impact)
                    pattern.learning_confidence = min(1.0, pattern.learning_confidence + 0.1)
                elif feedback.feedback_type == 'negative':
                    pattern.effectiveness_score = max(0.0, pattern.effectiveness_score - feedback_impact)
                    # Don't reduce confidence for negative user feedback, just effectiveness
                
                pattern.last_updated = time.time()
                
                # Record user preference
                context = feedback.context.get('context_type', 'general')
                if context not in pattern.context_applicability:
                    pattern.context_applicability.append(context)
                
                return {'status': 'updated', 'new_effectiveness': pattern.effectiveness_score}
            else:
                return {'status': 'pattern_not_found', 'pattern_id': feedback.target_pattern}
                
        except Exception as e:
            return {'status': 'error', 'error': str(e)}
    
    async def _process_system_feedback(self, feedback: FeedbackData) -> Dict[str, Any]:
        """Process system-generated feedback"""
        try:
            # System feedback for general pattern maintenance
            if feedback.target_pattern in self.pattern_learner.learned_patterns:
                pattern = self.pattern_learner.learned_patterns[feedback.target_pattern]
                
                # Smaller impact for system feedback
                feedback_impact = feedback.feedback_value * 0.05
                
                if feedback.feedback_type == 'positive':
                    pattern.effectiveness_score = min(1.0, pattern.effectiveness_score + feedback_impact)
                elif feedback.feedback_type == 'negative':
                    pattern.effectiveness_score = max(0.0, pattern.effectiveness_score - feedback_impact)
                
                pattern.last_updated = time.time()
                
                return {'status': 'updated', 'new_effectiveness': pattern.effectiveness_score}
            else:
                return {'status': 'pattern_not_found', 'pattern_id': feedback.target_pattern}
                
        except Exception as e:
            return {'status': 'error', 'error': str(e)}
    
    async def _process_adaptive_feedback(self, feedback: FeedbackData) -> Dict[str, Any]:
        """Process adaptive feedback from optimization systems"""
        try:
            # Adaptive feedback helps with pattern optimization
            if feedback.target_pattern in self.pattern_learner.learned_patterns:
                # Trigger pattern optimization based on feedback
                optimization_target = feedback.context.get('optimization_target', 'effectiveness')
                success = self.pattern_learner.optimize_pattern(
                    feedback.target_pattern, 
                    optimization_target
                )
                
                if success:
                    return {'status': 'optimized', 'optimization_target': optimization_target}
                else:
                    return {'status': 'optimization_failed'}
            else:
                return {'status': 'pattern_not_found', 'pattern_id': feedback.target_pattern}
                
        except Exception as e:
            return {'status': 'error', 'error': str(e)}
    
    def register_feedback_processor(self, feedback_type: str, processor: Callable):
        """Register custom feedback processor"""
        self.feedback_processors[feedback_type] = processor
        logger.info(f"Registered feedback processor: {feedback_type}")
    
    def get_feedback_statistics(self) -> Dict[str, Any]:
        """Get feedback processing statistics"""
        return {
            'queue_size': len(self.feedback_queue),
            'processing_active': self.processing_active,
            'registered_processors': list(self.feedback_processors.keys()),
            'stats': dict(self.feedback_stats)
        }
    
    def start_processing(self):
        """Start feedback processing"""
        self.processing_active = True
        logger.info("Feedback processing started")
    
    def stop_processing(self):
        """Stop feedback processing"""
        self.processing_active = False
        logger.info("Feedback processing stopped")


class LearningEngine:
    """Main learning engine coordinating all learning components"""
    
    def __init__(self):
        self.pattern_learner = PatternLearner()
        self.feedback_processor = FeedbackProcessor(self.pattern_learner)
        self.learning_active = True
        self.learning_stats = {
            'start_time': time.time(),
            'total_learning_cycles': 0,
            'successful_optimizations': 0,
            'patterns_created': 0
        }
        
        logger.info("Learning engine initialized")
    
    async def learning_cycle(self) -> Dict[str, Any]:
        """Execute one learning cycle"""
        if not self.learning_active:
            return {'status': 'learning_inactive'}
        
        cycle_start = time.time()
        results = {}
        
        try:
            # Process feedback batch
            feedback_result = await self.feedback_processor.process_feedback_batch(batch_size=5)
            results['feedback_processing'] = feedback_result
            
            # Optimize patterns based on recent performance
            optimization_results = await self._optimize_patterns()
            results['pattern_optimization'] = optimization_results
            
            # Learn new patterns from recent data
            learning_results = await self._discover_new_patterns()
            results['pattern_discovery'] = learning_results
            
            # Update learning statistics
            self.learning_stats['total_learning_cycles'] += 1
            self.learning_stats['successful_optimizations'] += optimization_results.get('optimized_count', 0)
            
            cycle_duration = time.time() - cycle_start
            results['cycle_duration'] = cycle_duration
            results['status'] = 'completed'
            
            logger.debug(f"Learning cycle completed in {cycle_duration:.3f}s")
            
            return results
            
        except Exception as e:
            logger.error(f"Error in learning cycle: {e}")
            return {'status': 'error', 'error': str(e)}
    
    async def _optimize_patterns(self) -> Dict[str, Any]:
        """Optimize existing patterns"""
        optimization_results = {
            'optimized_count': 0,
            'optimization_attempts': 0,
            'optimization_targets': []
        }
        
        # Get patterns that need optimization (low effectiveness or high usage)
        candidates = []
        for pattern in self.pattern_learner.learned_patterns.values():
            if (pattern.effectiveness_score < 0.6 and pattern.usage_count > 5) or \
               (pattern.usage_count > 50 and pattern.learning_confidence < 0.8):
                candidates.append(pattern)
        
        # Optimize top candidates
        for pattern in candidates[:5]:  # Limit to 5 optimizations per cycle
            optimization_results['optimization_attempts'] += 1
            
            # Choose optimization target based on pattern characteristics
            if pattern.effectiveness_score < 0.5:
                target = 'effectiveness'
            elif len(pattern.context_applicability) < 3:
                target = 'applicability'
            else:
                target = 'efficiency'
            
            success = self.pattern_learner.optimize_pattern(pattern.pattern_id, target)
            if success:
                optimization_results['optimized_count'] += 1
                optimization_results['optimization_targets'].append(target)
        
        return optimization_results
    
    async def _discover_new_patterns(self) -> Dict[str, Any]:
        """Discover new patterns from recent activity"""
        discovery_results = {
            'patterns_discovered': 0,
            'patterns_analyzed': 0
        }
        
        # Analyze recent learning history for pattern discovery opportunities
        recent_events = [event for event in self.pattern_learner.learning_history 
                        if time.time() - event['timestamp'] < 3600]  # Last hour
        
        discovery_results['patterns_analyzed'] = len(recent_events)
        
        # Simple pattern discovery based on common successful configurations
        if len(recent_events) >= 5:
            # Look for common success patterns
            successful_events = [event for event in recent_events 
                               if event.get('initial_effectiveness', 0) > 0.7]
            
            if len(successful_events) >= 3:
                # Create meta-pattern from successful patterns
                meta_pattern = CognitivePattern(
                    pattern_id=f"meta_pattern_{int(time.time())}",
                    pattern_type=PatternType.META,
                    pattern_data={
                        'success_factors': 'high_effectiveness_threshold',
                        'common_characteristics': 'analysis_pending',
                        'derived_from': [event['pattern_id'] for event in successful_events 
                                       if 'pattern_id' in event]
                    },
                    effectiveness_score=0.8,
                    learning_confidence=0.6
                )
                
                success = self.pattern_learner.learn_pattern(meta_pattern, LearningMode.META)
                if success:
                    discovery_results['patterns_discovered'] += 1
                    self.learning_stats['patterns_created'] += 1
        
        return discovery_results
    
    def create_pattern_from_data(self, pattern_type: PatternType, pattern_data: Dict[str, Any],
                               context: str = None) -> Optional[str]:
        """Create a new pattern from provided data"""
        try:
            pattern_id = f"{pattern_type.value}_{int(time.time()*1000)}"
            
            pattern = CognitivePattern(
                pattern_id=pattern_id,
                pattern_type=pattern_type,
                pattern_data=pattern_data,
                effectiveness_score=0.5,  # Start with neutral effectiveness
                context_applicability=[context] if context else []
            )
            
            success = self.pattern_learner.learn_pattern(pattern, LearningMode.SUPERVISED)
            if success:
                self.learning_stats['patterns_created'] += 1
                logger.info(f"Created pattern from data: {pattern_id}")
                return pattern_id
            
            return None
            
        except Exception as e:
            logger.error(f"Error creating pattern from data: {e}")
            return None
    
    def submit_performance_feedback(self, pattern_id: str, performance_data: Dict[str, float],
                                  context: str = 'general') -> bool:
        """Submit performance feedback for a pattern"""
        feedback = FeedbackData(
            feedback_id=f"perf_{int(time.time()*1000)}",
            source_component='performance_monitor',
            target_pattern=pattern_id,
            feedback_type='positive' if performance_data.get('success', 0.5) > 0.7 else 'negative',
            feedback_value=performance_data.get('success', 0.5),
            context={'context_type': context, 'performance_data': performance_data}
        )
        
        return self.feedback_processor.submit_feedback(feedback)
    
    def get_learning_status(self) -> Dict[str, Any]:
        """Get comprehensive learning system status"""
        pattern_stats = self.pattern_learner.get_learning_statistics()
        feedback_stats = self.feedback_processor.get_feedback_statistics()
        
        return {
            'learning_active': self.learning_active,
            'learning_stats': self.learning_stats,
            'pattern_learner': pattern_stats,
            'feedback_processor': feedback_stats,
            'uptime_seconds': time.time() - self.learning_stats['start_time']
        }
    
    def start_learning(self):
        """Start the learning engine"""
        self.learning_active = True
        self.feedback_processor.start_processing()
        logger.info("Learning engine started")
    
    def stop_learning(self):
        """Stop the learning engine"""
        self.learning_active = False
        self.feedback_processor.stop_processing()
        logger.info("Learning engine stopped")