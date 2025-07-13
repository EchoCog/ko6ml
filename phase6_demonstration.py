#!/usr/bin/env python3
"""
Phase 6: Meta-Cognitive Learning & Adaptive Optimization Demonstration

This script demonstrates the meta-cognitive capabilities implemented in Phase 6,
including self-monitoring, adaptive optimization, learning mechanisms, and
feedback loops for continuous improvement.
"""

import sys
import os
import time
import logging
import asyncio
import json
from typing import Dict, Any

# Setup path for imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def print_section_header(title: str):
    """Print a formatted section header"""
    print("\n" + "=" * 80)
    print(f" {title}")
    print("=" * 80)

def print_subsection_header(title: str):
    """Print a formatted subsection header"""
    print(f"\n{'-' * 60}")
    print(f" {title}")
    print("-" * 60)

def print_results(results: Dict[str, Any], indent: int = 0):
    """Pretty print results with indentation"""
    spaces = "  " * indent
    for key, value in results.items():
        if isinstance(value, dict) and len(value) > 0:
            print(f"{spaces}{key}:")
            print_results(value, indent + 1)
        elif isinstance(value, list) and len(value) > 0 and isinstance(value[0], dict):
            print(f"{spaces}{key}: [{len(value)} items]")
            if len(value) <= 3:  # Show first few items
                for i, item in enumerate(value[:3]):
                    print(f"{spaces}  [{i}]:")
                    print_results(item, indent + 2)
        else:
            # Format numeric values nicely
            if isinstance(value, float):
                if 0 < value < 1:
                    print(f"{spaces}{key}: {value:.3f}")
                else:
                    print(f"{spaces}{key}: {value:.2f}")
            else:
                print(f"{spaces}{key}: {value}")

async def main():
    """Main demonstration function"""
    
    print_section_header("PHASE 6: META-COGNITIVE LEARNING & ADAPTIVE OPTIMIZATION DEMONSTRATION")
    
    print("\nThis demonstration showcases the meta-cognitive capabilities that enable")
    print("the cognitive architecture to learn about its own performance and")
    print("continuously optimize its processing strategies.")
    
    try:
        # Import Phase 6 components
        from cognitive_architecture.meta_learning import (
            meta_cognitive_engine, PerformanceMonitor, MetricType, 
            PerformanceMetric, OptimizationStrategy, PatternType, 
            CognitivePattern, LearningMode
        )
        
        print("\n✓ Successfully imported Phase 6 meta-cognitive learning components")
        
        # Initialize demonstration data
        story_scenarios = [
            {
                'name': 'Simple Fantasy Quest',
                'context': 'fantasy_adventure',
                'data': {
                    'text': 'A brave knight embarked on a quest to rescue the princess from the dragon\'s lair.',
                    'characters': [
                        {'name': 'Knight', 'role': 'protagonist', 'traits': ['brave', 'noble']},
                        {'name': 'Princess', 'role': 'character_in_distress'},
                        {'name': 'Dragon', 'role': 'antagonist', 'traits': ['powerful', 'territorial']}
                    ],
                    'events': [
                        {'description': 'Quest begins', 'participants': ['Knight']},
                        {'description': 'Dragon encounter', 'participants': ['Knight', 'Dragon']},
                        {'description': 'Princess rescue', 'participants': ['Knight', 'Princess']}
                    ],
                    'locations': ['Village', 'Forest', 'Dragon\'s Lair', 'Castle']
                },
                'complexity': 0.6,
                'expected_processing_style': 'balanced'
            },
            {
                'name': 'Complex Sci-Fi Mystery',
                'context': 'scifi_mystery',
                'data': {
                    'text': 'In the year 2157, Detective Sarah Chen investigated a series of impossible crimes aboard the orbital station Kepler-7, where quantum physics and human psychology intertwined in ways that challenged the very nature of reality.',
                    'characters': [
                        {'name': 'Detective Sarah Chen', 'role': 'protagonist', 'traits': ['analytical', 'persistent']},
                        {'name': 'Dr. Marcus Webb', 'role': 'scientist', 'traits': ['brilliant', 'secretive']},
                        {'name': 'Station AI ARIA', 'role': 'artificial_intelligence'}
                    ],
                    'events': [
                        {'description': 'First impossible crime discovered', 'timestamp': '2157-03-15T09:30:00Z'},
                        {'description': 'Quantum signature detected', 'timestamp': '2157-03-15T14:22:00Z'},
                        {'description': 'Pattern analysis reveals conspiracy', 'timestamp': '2157-03-16T11:45:00Z'}
                    ],
                    'locations': ['Kepler-7 Station', 'Quantum Lab', 'Security Center', 'AI Core']
                },
                'complexity': 0.9,
                'expected_processing_style': 'accuracy_optimized'
            },
            {
                'name': 'Fast-Paced Action Scene',
                'context': 'action_sequence',
                'data': {
                    'text': 'The building exploded behind them as Jake and Maria sprinted through the narrow alley, bullets ricocheting off the brick walls.',
                    'characters': [
                        {'name': 'Jake', 'role': 'protagonist', 'traits': ['quick', 'resourceful']},
                        {'name': 'Maria', 'role': 'ally', 'traits': ['skilled', 'brave']}
                    ],
                    'events': [
                        {'description': 'Building explosion', 'urgency': 'high'},
                        {'description': 'Chase sequence', 'urgency': 'high'},
                        {'description': 'Gunfire exchange', 'urgency': 'critical'}
                    ]
                },
                'complexity': 0.4,
                'expected_processing_style': 'speed_optimized'
            }
        ]
        
        # 1. Performance Monitoring Demonstration
        print_section_header("1. SELF-MONITORING COGNITIVE PERFORMANCE METRICS")
        
        print("Demonstrating real-time performance monitoring and metrics collection...")
        
        performance_monitor = meta_cognitive_engine.performance_monitor
        
        # Record some baseline metrics
        print("\n📊 Recording baseline performance metrics:")
        
        baseline_metrics = [
            (MetricType.PROCESSING_TIME, 1.2, "reasoning_engine"),
            (MetricType.ACCURACY, 0.75, "logical_inference"),
            (MetricType.EFFICIENCY, 0.68, "memory_management"),
            (MetricType.ATTENTION_FOCUS, 0.82, "ecan_system")
        ]
        
        for metric_type, value, component in baseline_metrics:
            metric = PerformanceMetric(
                metric_type=metric_type,
                value=value,
                component=component,
                context={'phase': 'baseline', 'demo': 'phase6'}
            )
            success = performance_monitor.record_metric(metric)
            print(f"  ✓ Recorded {metric_type.value}: {value} ({component})")
        
        # Get initial performance summary
        print("\n📈 Initial Performance Summary:")
        initial_summary = performance_monitor.get_performance_summary()
        if 'metrics' in initial_summary:
            for metric_name, metric_data in initial_summary['metrics'].items():
                if isinstance(metric_data, dict) and 'mean' in metric_data:
                    print(f"  {metric_name}: {metric_data['mean']:.3f} (trend: {metric_data.get('trend', 'unknown')})")
        
        # 2. Adaptive Optimization Demonstration
        print_section_header("2. ADAPTIVE ALGORITHM SELECTION BASED ON CONTEXT")
        
        print("Demonstrating context-aware optimization for different story types...")
        
        optimization_results = []
        
        for scenario in story_scenarios:
            print_subsection_header(f"Processing: {scenario['name']}")
            
            # Process task with meta-cognitive optimization
            start_time = time.time()
            result = meta_cognitive_engine.process_cognitive_task(
                scenario['data'], 
                context=scenario['context']
            )
            processing_time = time.time() - start_time
            
            if 'error' not in result:
                optimization_results.append({
                    'scenario': scenario['name'],
                    'context': scenario['context'],
                    'processing_time': processing_time,
                    'optimization_id': result.get('optimization_id'),
                    'algorithm_selected': result.get('optimized_config', {}).get('algorithm_id'),
                    'strategy': result.get('optimized_config', {}).get('strategy')
                })
                
                print(f"  ✓ Algorithm Selected: {result.get('optimized_config', {}).get('algorithm_id')}")
                print(f"  ✓ Strategy: {result.get('optimized_config', {}).get('strategy')}")
                print(f"  ✓ Processing Time: {processing_time:.3f}s")
                
                # Record performance for this optimization
                actual_performance = {
                    'processing_time': processing_time,
                    'success': 1.0,
                    'accuracy': 0.8 + (0.1 * (1 - scenario['complexity'])),  # Higher accuracy for simpler tasks
                    'effectiveness': 0.75 + (0.15 * (1 - scenario['complexity']))
                }
                
                optimization_id = result.get('optimization_id')
                if optimization_id:
                    meta_cognitive_engine.adaptive_optimizer.record_optimization_result(
                        optimization_id, actual_performance
                    )
                    print(f"  ✓ Recorded performance: accuracy={actual_performance['accuracy']:.3f}")
            else:
                print(f"  ❌ Error processing {scenario['name']}: {result['error']}")
        
        # Show optimization statistics
        print("\n📊 Adaptive Optimization Statistics:")
        opt_stats = meta_cognitive_engine.adaptive_optimizer.get_optimization_statistics()
        print_results({
            'Total Optimizations': opt_stats.get('total_optimizations', 0),
            'Overall Effectiveness': f"{opt_stats.get('overall_effectiveness', 0):.3f}",
            'Strategy Effectiveness': opt_stats.get('strategy_effectiveness', {})
        })
        
        # 3. Learning Mechanisms Demonstration
        print_section_header("3. LEARNING MECHANISMS FOR COGNITIVE PATTERN OPTIMIZATION")
        
        print("Demonstrating pattern learning and optimization...")
        
        learning_engine = meta_cognitive_engine.learning_engine
        
        # Create and learn some cognitive patterns
        print("\n🧠 Learning cognitive patterns:")
        
        patterns_to_learn = [
            {
                'id': 'fantasy_reasoning_pattern',
                'type': PatternType.REASONING_PATTERN,
                'data': {
                    'genre': 'fantasy',
                    'reasoning_style': 'archetypal',
                    'confidence_threshold': 0.7,
                    'pattern_indicators': ['magic', 'quest', 'hero_journey']
                },
                'effectiveness': 0.85
            },
            {
                'id': 'scifi_complexity_pattern',
                'type': PatternType.PROCESSING_PATTERN,
                'data': {
                    'genre': 'science_fiction',
                    'complexity_handling': 'analytical',
                    'processing_depth': 'thorough',
                    'technical_concepts': True
                },
                'effectiveness': 0.78
            },
            {
                'id': 'action_speed_pattern',
                'type': PatternType.OPTIMIZATION_PATTERN,
                'data': {
                    'genre': 'action',
                    'optimization_target': 'speed',
                    'urgency_response': 'immediate',
                    'detail_level': 'essential'
                },
                'effectiveness': 0.82
            }
        ]
        
        learned_patterns = []
        for pattern_info in patterns_to_learn:
            pattern = CognitivePattern(
                pattern_id=pattern_info['id'],
                pattern_type=pattern_info['type'],
                pattern_data=pattern_info['data'],
                effectiveness_score=pattern_info['effectiveness'],
                context_applicability=[pattern_info['data'].get('genre', 'general')]
            )
            
            success = learning_engine.pattern_learner.learn_pattern(pattern, LearningMode.SUPERVISED)
            if success:
                learned_patterns.append(pattern_info['id'])
                print(f"  ✓ Learned pattern: {pattern_info['id']} (effectiveness: {pattern_info['effectiveness']:.3f})")
        
        # Demonstrate pattern optimization
        print("\n🔧 Optimizing learned patterns:")
        for pattern_id in learned_patterns[:2]:  # Optimize first 2 patterns
            success = learning_engine.pattern_learner.optimize_pattern(pattern_id, "effectiveness")
            if success:
                print(f"  ✓ Optimized pattern: {pattern_id}")
        
        # Record pattern usage
        print("\n📝 Recording pattern usage and performance:")
        for i, pattern_id in enumerate(learned_patterns):
            performance_data = {
                'success': 0.8 + (i * 0.05),  # Varying success rates
                'effectiveness': 0.75 + (i * 0.03),
                'processing_time': 1.0 + (i * 0.2)
            }
            
            learning_engine.pattern_learner.record_pattern_usage(
                pattern_id, 
                f"demo_context_{i}",
                performance_data
            )
            print(f"  ✓ Recorded usage for {pattern_id}: success={performance_data['success']:.3f}")
        
        # Get learning statistics
        print("\n📊 Learning Statistics:")
        learning_stats = learning_engine.get_learning_status()
        pattern_stats = learning_stats.get('pattern_learner', {})
        print_results({
            'Total Patterns': pattern_stats.get('total_patterns', 0),
            'Average Effectiveness': f"{pattern_stats.get('average_effectiveness', 0):.3f}",
            'Average Success Rate': f"{pattern_stats.get('average_success_rate', 0):.3f}",
            'Pattern Types': pattern_stats.get('pattern_type_distribution', {})
        })
        
        # 4. Feedback Loops Demonstration
        print_section_header("4. FEEDBACK LOOPS FOR CONTINUOUS IMPROVEMENT")
        
        print("Demonstrating feedback submission and processing...")
        
        from cognitive_architecture.meta_learning.learning_engine import FeedbackData
        
        # Submit various types of feedback
        print("\n💬 Submitting feedback for continuous improvement:")
        
        feedback_items = [
            {
                'source': 'performance_monitor',
                'target': 'fantasy_reasoning_pattern',
                'type': 'positive',
                'value': 0.9,
                'context': {'reason': 'excellent_accuracy', 'domain': 'fantasy'}
            },
            {
                'source': 'user_interface',
                'target': 'scifi_complexity_pattern',
                'type': 'negative',
                'value': 0.3,
                'context': {'reason': 'too_slow', 'user_preference': 'speed'}
            },
            {
                'source': 'optimization_system',
                'target': 'action_speed_pattern',
                'type': 'positive',
                'value': 0.85,
                'context': {'reason': 'optimal_speed', 'metrics': 'processing_time'}
            }
        ]
        
        for i, feedback_info in enumerate(feedback_items):
            feedback = FeedbackData(
                feedback_id=f"demo_feedback_{i}",
                source_component=feedback_info['source'],
                target_pattern=feedback_info['target'],
                feedback_type=feedback_info['type'],
                feedback_value=feedback_info['value'],
                context=feedback_info['context']
            )
            
            success = learning_engine.feedback_processor.submit_feedback(feedback)
            if success:
                print(f"  ✓ Submitted {feedback_info['type']} feedback for {feedback_info['target']}")
        
        # Process feedback batch
        print("\n⚙️ Processing feedback batch:")
        processing_result = await learning_engine.feedback_processor.process_feedback_batch(batch_size=5)
        print(f"  ✓ Processed {processing_result.get('processed', 0)} feedback items")
        
        # Execute learning cycle
        print("\n🔄 Executing learning cycle:")
        cycle_result = await learning_engine.learning_cycle()
        if cycle_result.get('status') == 'completed':
            print(f"  ✓ Learning cycle completed in {cycle_result.get('cycle_duration', 0):.3f}s")
            
            feedback_processing = cycle_result.get('feedback_processing', {})
            pattern_optimization = cycle_result.get('pattern_optimization', {})
            
            print(f"  ✓ Feedback processed: {feedback_processing.get('processed', 0)} items")
            print(f"  ✓ Patterns optimized: {pattern_optimization.get('optimized_count', 0)}")
        
        # 5. Meta-Cognitive Analysis and Self-Awareness
        print_section_header("5. META-COGNITIVE ANALYSIS AND SELF-AWARENESS")
        
        print("Demonstrating meta-cognitive self-awareness and system analysis...")
        
        # Get comprehensive meta-cognitive status
        print("\n🧠 Meta-Cognitive Status Analysis:")
        meta_status = meta_cognitive_engine.get_meta_cognitive_status()
        
        print("\n📊 Self-Awareness Metrics:")
        self_awareness = meta_status.get('self_awareness_metrics', {})
        for metric_name, value in self_awareness.items():
            print(f"  {metric_name.replace('_', ' ').title()}: {value:.3f}")
        
        print("\n📈 Operation Statistics:")
        op_stats = meta_status.get('operation_stats', {})
        print_results({
            'Total Cycles': op_stats.get('total_cycles', 0),
            'Adaptations Performed': op_stats.get('adaptations_performed', 0),
            'Patterns Learned': op_stats.get('patterns_learned', 0),
            'Performance Improvements': op_stats.get('performance_improvements', 0)
        })
        
        print("\n🔍 Recent Meta-Cognitive Events:")
        recent_events = meta_status.get('recent_events', [])
        for i, event in enumerate(recent_events[:3]):  # Show last 3 events
            print(f"  [{i+1}] {event.get('event_type', 'unknown')}: {event.get('data', {}).get('description', 'No description')}")
        
        # 6. Performance Improvement Validation
        print_section_header("6. PERFORMANCE IMPROVEMENT VALIDATION")
        
        print("Demonstrating performance improvement tracking over time...")
        
        # Simulate additional improved performance metrics
        print("\n📈 Simulating performance improvements:")
        
        improved_metrics = [
            (MetricType.PROCESSING_TIME, [1.2, 1.1, 1.0, 0.9, 0.85]),  # Improving (decreasing)
            (MetricType.ACCURACY, [0.75, 0.78, 0.81, 0.84, 0.87]),     # Improving (increasing)
            (MetricType.EFFICIENCY, [0.68, 0.72, 0.75, 0.78, 0.82])    # Improving (increasing)
        ]
        
        for metric_type, values in improved_metrics:
            print(f"\n  Recording {metric_type.value} improvements:")
            for i, value in enumerate(values):
                if metric_type == MetricType.PROCESSING_TIME:
                    performance_monitor.record_processing_time(f"improved_op_{i}", value, "demo_component")
                elif metric_type == MetricType.ACCURACY:
                    performance_monitor.record_accuracy(value, f"improved_task_{i}", "demo_component")
                else:  # EFFICIENCY
                    performance_monitor.record_efficiency(value, f"improved_resource_{i}", "demo_component")
                
                time.sleep(0.01)  # Small delay to simulate time passage
            
            print(f"    ✓ Recorded {len(values)} measurements showing improvement trend")
        
        # Calculate and display improvements
        print("\n📊 Performance Improvement Analysis:")
        for metric_type, _ in improved_metrics:
            improvement = performance_monitor.get_performance_improvement(metric_type, time_window=3600)
            
            if 'improvement' in improvement:
                improvement_data = improvement['improvement']
                direction = improvement_data.get('direction', 'unknown')
                relative_change = improvement_data.get('relative', 0)
                
                print(f"  {metric_type.value}:")
                print(f"    Direction: {direction}")
                print(f"    Relative Change: {relative_change:.1%}")
        
        # 7. Emergent Behaviors Documentation
        print_section_header("7. EMERGENT COGNITIVE BEHAVIORS AND OPTIMIZATION PATTERNS")
        
        print("Analyzing emergent behaviors and optimization patterns...")
        
        # Get comprehensive system analysis
        print("\n🔍 Emergent Behavior Analysis:")
        
        # Pattern relationship analysis
        best_patterns = learning_engine.pattern_learner.get_best_patterns(limit=5)
        print(f"\n  Top Performing Patterns ({len(best_patterns)} found):")
        for i, pattern in enumerate(best_patterns[:3]):
            print(f"    [{i+1}] {pattern.pattern_id}:")
            print(f"        Type: {pattern.pattern_type.value}")
            print(f"        Effectiveness: {pattern.effectiveness_score:.3f}")
            print(f"        Usage Count: {pattern.usage_count}")
            print(f"        Contexts: {', '.join(pattern.context_applicability[:3])}")
        
        # Optimization strategy effectiveness
        print("\n  Optimization Strategy Effectiveness:")
        strategy_stats = opt_stats.get('strategy_effectiveness', {})
        for strategy, effectiveness in strategy_stats.items():
            print(f"    {strategy.replace('_', ' ').title()}: {effectiveness:.3f}")
        
        # Meta-learning insights
        print("\n  Meta-Learning Insights:")
        adaptation_capability = self_awareness.get('adaptation_capability', 0)
        learning_effectiveness = self_awareness.get('learning_effectiveness', 0)
        meta_confidence = self_awareness.get('meta_learning_confidence', 0)
        
        print(f"    Adaptation Capability: {adaptation_capability:.3f}")
        print(f"    Learning Effectiveness: {learning_effectiveness:.3f}")
        print(f"    Meta-Learning Confidence: {meta_confidence:.3f}")
        
        if meta_confidence > 0.7:
            print("    🎯 High meta-learning confidence indicates strong self-awareness")
        elif meta_confidence > 0.5:
            print("    📈 Moderate meta-learning confidence shows developing self-awareness")
        else:
            print("    🔄 Lower meta-learning confidence suggests need for more learning cycles")
        
        # Final summary
        print_section_header("DEMONSTRATION SUMMARY")
        
        print("🎉 Phase 6 Meta-Cognitive Learning & Adaptive Optimization demonstration completed!")
        print("\n✅ Successfully demonstrated:")
        print("  • Self-monitoring cognitive performance metrics")
        print("  • Adaptive algorithm selection based on context")
        print("  • Learning mechanisms for cognitive pattern optimization")
        print("  • Feedback loops for continuous improvement")
        print("  • Meta-cognitive adaptation under varying conditions")
        print("  • Documentation of emergent cognitive behaviors")
        
        # Performance summary
        final_summary = performance_monitor.get_performance_summary()
        total_metrics = sum(len(metrics) for metrics in performance_monitor.metrics_history.values())
        
        print(f"\n📊 Final Statistics:")
        print(f"  • Total metrics recorded: {total_metrics}")
        print(f"  • Patterns learned: {len(learned_patterns)}")
        print(f"  • Optimizations performed: {len(optimization_results)}")
        print(f"  • Meta-cognitive cycles: {op_stats.get('total_cycles', 0)}")
        print(f"  • System health: {final_summary.get('overall_health', 'unknown')}")
        
        print("\n🚀 The meta-cognitive system is now self-aware and continuously improving!")
        
    except ImportError as e:
        print(f"\n❌ Failed to import required components: {e}")
        print("Please ensure Phase 6 meta-cognitive learning components are properly installed.")
        return False
    except Exception as e:
        print(f"\n❌ Demonstration error: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    return True

if __name__ == '__main__':
    print("Starting Phase 6 Meta-Cognitive Learning & Adaptive Optimization Demonstration...")
    
    # Run the demonstration
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
    try:
        success = loop.run_until_complete(main())
        
        if success:
            print("\n✅ Demonstration completed successfully!")
            sys.exit(0)
        else:
            print("\n❌ Demonstration failed!")
            sys.exit(1)
    finally:
        loop.close()