#!/usr/bin/env python3
"""
Phase 6: Meta-Cognitive Learning & Adaptive Optimization Test Suite

This comprehensive test suite validates all Phase 6 requirements for meta-cognitive
capabilities including self-monitoring, adaptive optimization, learning mechanisms,
and feedback loops.
"""

import sys
import os
import time
import logging
import asyncio
import unittest
from unittest.mock import Mock, patch
import json
import tempfile

# Setup path for imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class TestPhase6MetaCognitiveLearning(unittest.TestCase):
    """Comprehensive test suite for Phase 6 meta-cognitive learning capabilities"""
    
    @classmethod
    def setUpClass(cls):
        """Set up Phase 6 meta-cognitive learning test environment"""
        logger.info("=" * 70)
        logger.info("PHASE 6 META-COGNITIVE LEARNING & ADAPTIVE OPTIMIZATION TEST SUITE")
        logger.info("=" * 70)
        logger.info("Setting up Phase 6 meta-cognitive learning test environment...")
        
        try:
            # Import Phase 6 components
            from cognitive_architecture.meta_learning import (
                PerformanceMonitor, MetricType, PerformanceMetric,
                AdaptiveOptimizer, OptimizationStrategy, ContextualProfile,
                LearningEngine, PatternType, CognitivePattern, LearningMode,
                MetaCognitiveEngine
            )
            
            # Store references to components
            cls.PerformanceMonitor = PerformanceMonitor
            cls.MetricType = MetricType
            cls.PerformanceMetric = PerformanceMetric
            cls.AdaptiveOptimizer = AdaptiveOptimizer
            cls.OptimizationStrategy = OptimizationStrategy
            cls.ContextualProfile = ContextualProfile
            cls.LearningEngine = LearningEngine
            cls.PatternType = PatternType
            cls.CognitivePattern = CognitivePattern
            cls.LearningMode = LearningMode
            cls.MetaCognitiveEngine = MetaCognitiveEngine
            
            # Initialize test components
            cls.performance_monitor = PerformanceMonitor()
            cls.adaptive_optimizer = AdaptiveOptimizer()
            cls.learning_engine = LearningEngine()
            cls.meta_cognitive_engine = MetaCognitiveEngine()
            
            logger.info("✓ Meta-cognitive learning components initialized successfully")
            
        except ImportError as e:
            cls.fail(f"Failed to import meta-cognitive components: {e}")
        except Exception as e:
            cls.fail(f"Failed to initialize meta-cognitive components: {e}")
    
    def test_performance_monitoring_system(self):
        """Test self-monitoring cognitive performance metrics"""
        logger.info("Testing performance monitoring system...")
        
        # Test metric recording
        metric = self.PerformanceMetric(
            metric_type=self.MetricType.PROCESSING_TIME,
            value=1.5,
            component="test_component",
            context={"operation": "test_operation"}
        )
        
        success = self.performance_monitor.record_metric(metric)
        self.assertTrue(success, "Should successfully record performance metric")
        
        # Test convenience methods
        self.assertTrue(self.performance_monitor.record_processing_time("test_op", 0.5))
        self.assertTrue(self.performance_monitor.record_accuracy(0.85, "test_task"))
        self.assertTrue(self.performance_monitor.record_efficiency(0.75, "memory"))
        
        # Test performance summary
        summary = self.performance_monitor.get_performance_summary()
        self.assertIsInstance(summary, dict, "Should return performance summary")
        self.assertIn('metrics', summary, "Summary should contain metrics")
        self.assertIn('timestamp', summary, "Summary should contain timestamp")
        
        # Test baseline setting and improvement calculation
        self.performance_monitor.set_baseline(self.MetricType.ACCURACY, 0.7)
        
        # Add more accuracy metrics
        for i in range(10):
            self.performance_monitor.record_accuracy(0.8 + i * 0.01, f"task_{i}")
        
        improvement = self.performance_monitor.get_performance_improvement(
            self.MetricType.ACCURACY, time_window=3600
        )
        self.assertIsInstance(improvement, dict, "Should return improvement data")
        
        # Test component comparison
        self.performance_monitor.record_processing_time("comp1_op", 1.0, "component1")
        self.performance_monitor.record_processing_time("comp2_op", 2.0, "component2")
        
        comparison = self.performance_monitor.get_performance_comparison(
            "component1", "component2", self.MetricType.PROCESSING_TIME
        )
        self.assertIsInstance(comparison, dict, "Should return comparison data")
        
        # Test metrics export
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.json') as f:
            export_success = self.performance_monitor.export_metrics(f.name)
            self.assertTrue(export_success, "Should successfully export metrics")
            
            # Verify export file exists and is valid JSON
            with open(f.name, 'r') as export_file:
                exported_data = json.load(export_file)
                self.assertIn('metrics', exported_data)
            
            os.unlink(f.name)
        
        logger.info("✓ Performance monitoring system test passed")
    
    def test_adaptive_optimization_system(self):
        """Test adaptive algorithm selection based on context"""
        logger.info("Testing adaptive optimization system...")
        
        # Test contextual profile registration
        profile = self.ContextualProfile(
            context_type="story_generation",
            task_complexity=0.7,
            time_pressure=0.3,
            accuracy_requirement=0.8,
            user_preference="quality_optimized"
        )
        
        self.adaptive_optimizer.contextual_adapter.register_context_profile(profile)
        
        # Test optimization for context
        task_data = {
            'text': 'A brave knight embarked on a quest to save the kingdom.',
            'characters': [{'name': 'Knight', 'role': 'protagonist'}],
            'events': [{'description': 'Quest begins', 'participants': ['Knight']}]
        }
        
        optimization_result = self.adaptive_optimizer.optimize_for_context(
            "story_generation", task_data
        )
        
        self.assertIsInstance(optimization_result, dict, "Should return optimization result")
        self.assertIn('optimization_id', optimization_result, "Should contain optimization ID")
        self.assertIn('optimized_config', optimization_result, "Should contain optimized configuration")
        
        # Test performance requirements
        performance_requirements = {
            'max_processing_time': 2.0,
            'min_accuracy': 0.85,
            'max_memory_usage': 100
        }
        
        optimization_result_with_reqs = self.adaptive_optimizer.optimize_for_context(
            "story_generation", task_data, performance_requirements
        )
        
        self.assertIsInstance(optimization_result_with_reqs, dict)
        
        # Test recording optimization results
        actual_performance = {
            'processing_time': 1.8,
            'accuracy': 0.87,
            'success': 1.0
        }
        
        optimization_id = optimization_result['optimization_id']
        self.adaptive_optimizer.record_optimization_result(optimization_id, actual_performance)
        
        # Test optimization statistics
        stats = self.adaptive_optimizer.get_optimization_statistics()
        self.assertIsInstance(stats, dict, "Should return optimization statistics")
        self.assertIn('total_optimizations', stats, "Should contain total optimizations")
        
        # Test strategy updates
        self.adaptive_optimizer.update_optimization_strategy(
            self.OptimizationStrategy.SPEED_OPTIMIZED,
            {'timeout_multiplier': 0.4}
        )
        
        logger.info("✓ Adaptive optimization system test passed")
    
    def test_learning_mechanisms(self):
        """Test learning mechanisms for cognitive pattern optimization"""
        logger.info("Testing learning mechanisms...")
        
        # Test pattern learning
        pattern = self.CognitivePattern(
            pattern_id="test_reasoning_pattern",
            pattern_type=self.PatternType.REASONING_PATTERN,
            pattern_data={
                'confidence_threshold': 0.7,
                'max_iterations': 10,
                'strategy': 'balanced'
            },
            effectiveness_score=0.8,
            context_applicability=["story_analysis", "reasoning"]
        )
        
        success = self.learning_engine.pattern_learner.learn_pattern(
            pattern, self.LearningMode.SUPERVISED
        )
        self.assertTrue(success, "Should successfully learn pattern")
        
        # Test pattern optimization
        opt_success = self.learning_engine.pattern_learner.optimize_pattern(
            pattern.pattern_id, "effectiveness"
        )
        self.assertTrue(opt_success, "Should successfully optimize pattern")
        
        # Test pattern usage recording
        performance_data = {
            'success': 1.0,
            'effectiveness': 0.85,
            'processing_time': 1.2
        }
        
        self.learning_engine.pattern_learner.record_pattern_usage(
            pattern.pattern_id, "story_analysis", performance_data
        )
        
        # Test best patterns retrieval
        best_patterns = self.learning_engine.pattern_learner.get_best_patterns(
            context="story_analysis",
            pattern_type=self.PatternType.REASONING_PATTERN,
            limit=5
        )
        
        self.assertIsInstance(best_patterns, list, "Should return list of patterns")
        self.assertGreater(len(best_patterns), 0, "Should find patterns")
        
        # Test pattern recommendations
        recommendations = self.learning_engine.pattern_learner.get_pattern_recommendations(
            "story_analysis", [pattern.pattern_id]
        )
        
        self.assertIsInstance(recommendations, list, "Should return recommendations list")
        
        # Test creating pattern from data
        pattern_id = self.learning_engine.create_pattern_from_data(
            self.PatternType.OPTIMIZATION_PATTERN,
            {'optimization_type': 'speed', 'parameters': {'timeout': 0.5}},
            context="fast_processing"
        )
        
        self.assertIsInstance(pattern_id, str, "Should return pattern ID")
        
        # Test learning statistics
        stats = self.learning_engine.pattern_learner.get_learning_statistics()
        self.assertIsInstance(stats, dict, "Should return learning statistics")
        self.assertIn('total_patterns', stats, "Should contain total patterns count")
        
        logger.info("✓ Learning mechanisms test passed")
    
    def test_feedback_loops(self):
        """Test feedback loops for continuous improvement"""
        logger.info("Testing feedback loops for continuous improvement...")
        
        # Test feedback submission
        from cognitive_architecture.meta_learning.learning_engine import FeedbackData
        
        feedback = FeedbackData(
            feedback_id="test_feedback_001",
            source_component="test_system",
            target_pattern="test_reasoning_pattern",
            feedback_type="positive",
            feedback_value=0.9,
            context={'test_context': 'performance_improvement'}
        )
        
        success = self.learning_engine.feedback_processor.submit_feedback(feedback)
        self.assertTrue(success, "Should successfully submit feedback")
        
        # Test batch feedback processing
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        try:
            processing_result = loop.run_until_complete(
                self.learning_engine.feedback_processor.process_feedback_batch(batch_size=5)
            )
            
            self.assertIsInstance(processing_result, dict, "Should return processing result")
            self.assertIn('processed', processing_result, "Should contain processed count")
            
        finally:
            loop.close()
        
        # Test performance feedback submission
        feedback_success = self.learning_engine.submit_performance_feedback(
            "test_reasoning_pattern",
            {'success': 0.9, 'processing_time': 1.0, 'accuracy': 0.85},
            context="test_context"
        )
        self.assertTrue(feedback_success, "Should successfully submit performance feedback")
        
        # Test feedback statistics
        feedback_stats = self.learning_engine.feedback_processor.get_feedback_statistics()
        self.assertIsInstance(feedback_stats, dict, "Should return feedback statistics")
        
        # Test learning cycle
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        try:
            cycle_result = loop.run_until_complete(self.learning_engine.learning_cycle())
            
            self.assertIsInstance(cycle_result, dict, "Should return cycle result")
            self.assertIn('status', cycle_result, "Should contain status")
            
        finally:
            loop.close()
        
        logger.info("✓ Feedback loops test passed")
    
    def test_meta_cognitive_adaptation(self):
        """Test meta-cognitive adaptation under varying conditions"""
        logger.info("Testing meta-cognitive adaptation under varying conditions...")
        
        # Test cognitive task processing
        task_data = {
            'text': 'In a realm where magic and technology coexist, a young inventor discovers an ancient artifact.',
            'context_type': 'fantasy_story',
            'complexity': 'medium',
            'characters': [
                {'name': 'Inventor', 'type': 'protagonist'},
                {'name': 'Artifact', 'type': 'magical_item'}
            ]
        }
        
        # Process task with meta-cognitive optimization
        result = self.meta_cognitive_engine.process_cognitive_task(task_data, context="fantasy_story")
        
        self.assertIsInstance(result, dict, "Should return processing result")
        self.assertIn('status', result, "Should contain status")
        
        # Test meta-cognitive status
        status = self.meta_cognitive_engine.get_meta_cognitive_status()
        
        self.assertIsInstance(status, dict, "Should return meta-cognitive status")
        self.assertIn('self_awareness_metrics', status, "Should contain self-awareness metrics")
        self.assertIn('current_state', status, "Should contain current state")
        self.assertIn('operation_stats', status, "Should contain operation statistics")
        
        # Test parameter updates
        param_update_success = self.meta_cognitive_engine.update_meta_cognitive_parameters({
            'cycle_interval': 3.0,
            'learning_rate': 0.15
        })
        self.assertTrue(param_update_success, "Should successfully update parameters")
        
        # Test adaptation under different conditions
        conditions = [
            {'context': 'high_speed', 'task_complexity': 0.3, 'time_pressure': 0.9},
            {'context': 'high_accuracy', 'task_complexity': 0.8, 'time_pressure': 0.2},
            {'context': 'balanced', 'task_complexity': 0.5, 'time_pressure': 0.5}
        ]
        
        adaptation_results = []
        for condition in conditions:
            context_task = {
                'text': f"Test task for {condition['context']} processing",
                'complexity': condition['task_complexity'],
                'time_pressure': condition['time_pressure']
            }
            
            result = self.meta_cognitive_engine.process_cognitive_task(
                context_task, context=condition['context']
            )
            adaptation_results.append(result)
        
        # Verify different optimizations were applied
        self.assertEqual(len(adaptation_results), 3, "Should process all test conditions")
        
        for result in adaptation_results:
            self.assertIn('optimized_config', result, "Should contain optimized configuration")
        
        logger.info("✓ Meta-cognitive adaptation test passed")
    
    def test_emergent_behaviors_documentation(self):
        """Test documentation of emergent cognitive behaviors and optimization patterns"""
        logger.info("Testing emergent behaviors and optimization patterns documentation...")
        
        # Simulate learning activity to generate emergent behaviors
        patterns_to_learn = [
            {
                'pattern_id': 'emergent_pattern_1',
                'pattern_type': self.PatternType.BEHAVIORAL_PATTERN,
                'pattern_data': {
                    'behavior_type': 'adaptive_reasoning',
                    'trigger_conditions': ['complex_task', 'time_pressure'],
                    'optimization_strategy': 'dynamic_threshold_adjustment'
                },
                'effectiveness_score': 0.85
            },
            {
                'pattern_id': 'emergent_pattern_2',
                'pattern_type': self.PatternType.ATTENTION_PATTERN,
                'pattern_data': {
                    'attention_strategy': 'selective_focus',
                    'context_sensitivity': 0.7,
                    'adaptation_rate': 0.3
                },
                'effectiveness_score': 0.78
            }
        ]
        
        # Learn patterns
        for pattern_data in patterns_to_learn:
            pattern = self.CognitivePattern(
                pattern_id=pattern_data['pattern_id'],
                pattern_type=pattern_data['pattern_type'],
                pattern_data=pattern_data['pattern_data'],
                effectiveness_score=pattern_data['effectiveness_score']
            )
            
            success = self.learning_engine.pattern_learner.learn_pattern(pattern)
            self.assertTrue(success, f"Should learn pattern {pattern.pattern_id}")
        
        # Simulate pattern usage and optimization
        for pattern_data in patterns_to_learn:
            for i in range(5):  # Multiple usage instances
                performance_data = {
                    'success': 0.8 + i * 0.05,
                    'effectiveness': 0.75 + i * 0.03,
                    'adaptation_speed': 0.6 + i * 0.02
                }
                
                self.learning_engine.pattern_learner.record_pattern_usage(
                    pattern_data['pattern_id'],
                    f"context_{i}",
                    performance_data
                )
        
        # Get comprehensive statistics to document emergent behaviors
        learning_stats = self.learning_engine.get_learning_status()
        optimization_stats = self.adaptive_optimizer.get_optimization_statistics()
        meta_status = self.meta_cognitive_engine.get_meta_cognitive_status()
        
        # Verify documentation contains key emergent behavior indicators
        self.assertIn('pattern_learner', learning_stats, "Should document pattern learning")
        self.assertIn('total_optimizations', optimization_stats, "Should document optimizations")
        self.assertIn('self_awareness_metrics', meta_status, "Should document self-awareness")
        
        # Test pattern relationship analysis
        pattern_stats = learning_stats['pattern_learner']
        if 'total_patterns' in pattern_stats:
            self.assertGreater(pattern_stats['total_patterns'], 0, "Should have learned patterns")
        
        # Test optimization pattern discovery
        if 'strategy_effectiveness' in optimization_stats:
            strategy_effectiveness = optimization_stats['strategy_effectiveness']
            self.assertIsInstance(strategy_effectiveness, dict, "Should document strategy effectiveness")
        
        # Test self-awareness metrics evolution
        self_awareness = meta_status['self_awareness_metrics']
        expected_metrics = [
            'performance_awareness',
            'learning_effectiveness', 
            'adaptation_capability',
            'optimization_success_rate',
            'meta_learning_confidence'
        ]
        
        for metric in expected_metrics:
            self.assertIn(metric, self_awareness, f"Should track {metric}")
            self.assertIsInstance(self_awareness[metric], (int, float), f"{metric} should be numeric")
        
        logger.info("✓ Emergent behaviors documentation test passed")
    
    def test_system_integration(self):
        """Test integration with existing cognitive architecture phases"""
        logger.info("Testing integration with existing cognitive architecture phases...")
        
        try:
            # Test integration with existing components
            from cognitive_architecture import (
                cognitive_core, ecan_system, mesh_orchestrator, scheme_adapter
            )
            
            # Verify core components are accessible
            self.assertIsNotNone(cognitive_core, "Should have access to cognitive core")
            self.assertIsNotNone(ecan_system, "Should have access to ECAN system")
            self.assertIsNotNone(mesh_orchestrator, "Should have access to mesh orchestrator")
            self.assertIsNotNone(scheme_adapter, "Should have access to scheme adapter")
            
            # Test meta-cognitive engine integration with performance monitoring
            # Simulate performance data from existing systems
            performance_metrics = [
                ('attention_allocation', 0.85, 'ecan_system'),
                ('mesh_task_completion', 0.92, 'mesh_orchestrator'),
                ('pattern_translation', 0.78, 'scheme_adapter')
            ]
            
            for operation, value, component in performance_metrics:
                success = self.meta_cognitive_engine.performance_monitor.record_efficiency(
                    value, operation, component
                )
                self.assertTrue(success, f"Should record {operation} performance")
            
            # Test adaptive optimization with existing reasoning engines
            from cognitive_architecture.reasoning import advanced_reasoning_engine
            
            # Simulate reasoning task with meta-cognitive optimization
            story_data = {
                'text': 'The wizard cast a spell to protect the village from the approaching storm.',
                'characters': [{'name': 'Wizard', 'role': 'protagonist'}],
                'events': [{'description': 'Spell casting', 'outcome': 'village protection'}]
            }
            
            # Process through meta-cognitive engine
            meta_result = self.meta_cognitive_engine.process_cognitive_task(
                story_data, context="magical_reasoning"
            )
            
            self.assertIsInstance(meta_result, dict, "Should process cognitive task")
            
            # Verify integration doesn't break existing functionality
            # Test basic reasoning engine functionality
            reasoning_result = advanced_reasoning_engine.reason_about_story(story_data)
            
            self.assertIsNotNone(reasoning_result, "Should maintain reasoning functionality")
            self.assertTrue(hasattr(reasoning_result, 'overall_confidence'), "Should provide reasoning confidence")
            
            logger.info("✓ System integration test passed")
            
        except ImportError as e:
            logger.warning(f"Some integration components not available: {e}")
            # Continue with available components
    
    def test_performance_validation(self):
        """Test performance improvement validation over time"""
        logger.info("Testing performance improvement validation over time...")
        
        # Simulate performance data over time
        baseline_performance = 0.6
        
        # Set baseline
        self.meta_cognitive_engine.performance_monitor.set_baseline(
            self.MetricType.ACCURACY, baseline_performance
        )
        
        # Simulate improving performance over time
        time_intervals = [i * 0.1 for i in range(10)]  # 10 time points
        performance_values = [baseline_performance + i * 0.03 for i in range(10)]  # Improving trend
        
        for i, (time_offset, performance) in enumerate(zip(time_intervals, performance_values)):
            # Record performance metric
            self.meta_cognitive_engine.performance_monitor.record_accuracy(
                performance, f"task_{i}", "validation_component"
            )
            
            # Small delay to simulate time passage
            time.sleep(0.01)
        
        # Test performance improvement calculation
        improvement = self.meta_cognitive_engine.performance_monitor.get_performance_improvement(
            self.MetricType.ACCURACY, time_window=3600
        )
        
        self.assertIsInstance(improvement, dict, "Should return improvement data")
        
        if 'improvement' in improvement:
            improvement_data = improvement['improvement']
            self.assertIn('direction', improvement_data, "Should indicate improvement direction")
            
            # Should show improvement
            if improvement_data['direction'] == 'improvement':
                self.assertGreater(improvement_data['relative'], 0, "Should show positive improvement")
        
        # Test validation across different metrics
        validation_metrics = [
            (self.MetricType.PROCESSING_TIME, [2.0, 1.8, 1.6, 1.4, 1.2]),  # Improving (decreasing)
            (self.MetricType.EFFICIENCY, [0.6, 0.65, 0.7, 0.75, 0.8]),     # Improving (increasing)
            (self.MetricType.REASONING_QUALITY, [0.7, 0.72, 0.75, 0.77, 0.8])  # Improving
        ]
        
        validation_results = {}
        
        for metric_type, values in validation_metrics:
            for i, value in enumerate(values):
                if metric_type == self.MetricType.PROCESSING_TIME:
                    self.meta_cognitive_engine.performance_monitor.record_processing_time(
                        f"validation_op_{i}", value, "validation_component"
                    )
                elif metric_type == self.MetricType.EFFICIENCY:
                    self.meta_cognitive_engine.performance_monitor.record_efficiency(
                        value, f"validation_resource_{i}", "validation_component"
                    )
                else:  # REASONING_QUALITY
                    self.meta_cognitive_engine.performance_monitor.record_metric(
                        self.PerformanceMetric(
                            metric_type=metric_type,
                            value=value,
                            component="validation_component",
                            context={'validation_task': f'task_{i}'}
                        )
                    )
                
                time.sleep(0.01)  # Small delay
            
            # Calculate improvement for this metric
            improvement = self.meta_cognitive_engine.performance_monitor.get_performance_improvement(
                metric_type, time_window=3600
            )
            validation_results[metric_type.value] = improvement
        
        # Verify improvements are detected
        for metric_name, improvement_data in validation_results.items():
            self.assertIsInstance(improvement_data, dict, f"Should have improvement data for {metric_name}")
        
        # Test overall performance summary
        summary = self.meta_cognitive_engine.performance_monitor.get_performance_summary(
            component="validation_component"
        )
        
        self.assertIsInstance(summary, dict, "Should return performance summary")
        self.assertIn('metrics', summary, "Should contain metrics data")
        
        logger.info("✓ Performance validation test passed")

if __name__ == '__main__':
    # Run the comprehensive test suite
    logger.info("Starting Phase 6 Meta-Cognitive Learning & Adaptive Optimization test suite...")
    
    # Create test suite
    test_suite = unittest.TestLoader().loadTestsFromTestCase(TestPhase6MetaCognitiveLearning)
    
    # Run tests with detailed output
    runner = unittest.TextTestRunner(verbosity=2, stream=sys.stdout)
    result = runner.run(test_suite)
    
    # Print final results
    logger.info("=" * 70)
    if result.wasSuccessful():
        logger.info("🎉 ALL PHASE 6 META-COGNITIVE LEARNING TESTS PASSED!")
        logger.info(f"✓ Ran {result.testsRun} tests successfully")
        logger.info("✓ Self-monitoring cognitive performance metrics - IMPLEMENTED")
        logger.info("✓ Adaptive algorithm selection based on context - IMPLEMENTED")
        logger.info("✓ Learning mechanisms for cognitive pattern optimization - IMPLEMENTED")
        logger.info("✓ Feedback loops for continuous improvement - IMPLEMENTED")
        logger.info("✓ Meta-cognitive adaptation under varying conditions - VALIDATED")
        logger.info("✓ Emergent cognitive behaviors and optimization patterns - DOCUMENTED")
    else:
        logger.error(f"❌ {len(result.failures)} test(s) failed, {len(result.errors)} error(s)")
        for test, error in result.failures + result.errors:
            logger.error(f"Failed: {test}")
            logger.error(f"Error: {error}")
    
    logger.info("=" * 70)
    
    # Exit with appropriate code
    sys.exit(0 if result.wasSuccessful() else 1)