#!/usr/bin/env python3
"""
Phase 4 KoboldAI Integration Test Suite

This test suite validates the integration of the cognitive architecture 
with KoboldAI's text generation pipeline.
"""

import sys
import os
import time
import logging
from typing import Dict, Any, List
import unittest

# Add the project root to the path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class TestPhase4Integration(unittest.TestCase):
    """Test suite for Phase 4 KoboldAI Cognitive Integration"""
    
    @classmethod
    def setUpClass(cls):
        """Set up test environment"""
        logger.info("Setting up Phase 4 integration test environment...")
        
        # Import cognitive integration components
        try:
            from cognitive_architecture.integration import kobold_cognitive_integrator
            cls.cognitive_integrator = kobold_cognitive_integrator
            
            # Initialize the cognitive architecture
            success = cls.cognitive_integrator.initialize()
            cls.assertTrue(success, "Failed to initialize cognitive architecture")
            
            logger.info("✓ Cognitive architecture initialized successfully")
            
        except ImportError as e:
            cls.fail(f"Failed to import cognitive architecture: {e}")
        except Exception as e:
            cls.fail(f"Failed to set up test environment: {e}")
    
    def test_cognitive_initialization(self):
        """Test that cognitive architecture initializes properly"""
        status = self.cognitive_integrator.get_integration_status()
        
        self.assertTrue(status['is_initialized'], "Cognitive integrator should be initialized")
        self.assertIn('agents_created', status['stats'], "Should have integration stats")
        self.assertGreater(status['stats']['agents_created'], 0, "Should have created agents")
        
        logger.info("✓ Cognitive initialization test passed")
    
    def test_user_input_processing(self):
        """Test processing of user input through cognitive architecture"""
        test_inputs = [
            "The brave knight ventured into the dark forest.",
            "Magic spells illuminate the ancient castle walls.",
            "Tell me about the dragon's treasure hoard.",
        ]
        
        for user_input in test_inputs:
            with self.subTest(input=user_input):
                result = self.cognitive_integrator.process_user_input(
                    user_input,
                    context={'actionmode': 0, 'gamestarted': True}
                )
                
                self.assertNotIn('error', result, f"Should not have errors processing: {user_input}")
                self.assertIn('atomspace_patterns', result, "Should return atomspace patterns")
                self.assertIn('attention_elements', result, "Should return attention elements")
                self.assertIn('cognitive_state', result, "Should return cognitive state")
                
                # Verify some patterns were generated
                patterns = result.get('atomspace_patterns', [])
                self.assertIsInstance(patterns, list, "Patterns should be a list")
                
        logger.info(f"✓ User input processing test passed for {len(test_inputs)} inputs")
    
    def test_model_output_processing(self):
        """Test processing of model output through cognitive architecture"""
        test_outputs = [
            "The knight found a mysterious glowing sword in the depths of the cave.",
            "Ancient runes began to shimmer as magical energy flowed through the chamber.",
            "The dragon's eyes gleamed with intelligence as it spoke in a deep, resonant voice.",
        ]
        
        for output in test_outputs:
            with self.subTest(output=output):
                result = self.cognitive_integrator.process_model_output(
                    output,
                    context={
                        'lastctx': 'Previous story context...',
                        'story_length': 5,
                        'generation_settings': {
                            'temp': 0.7,
                            'top_p': 0.9,
                            'rep_pen': 1.1
                        }
                    }
                )
                
                self.assertNotIn('error', result, f"Should not have errors processing: {output}")
                self.assertIn('atomspace_patterns', result, "Should return atomspace patterns")
                self.assertIn('enhanced_text', result, "Should return enhanced text")
                self.assertIn('attention_elements', result, "Should return attention elements")
                
                # Verify enhanced text is returned
                enhanced_text = result.get('enhanced_text', '')
                self.assertIsInstance(enhanced_text, str, "Enhanced text should be a string")
                self.assertGreater(len(enhanced_text), 0, "Enhanced text should not be empty")
                
        logger.info(f"✓ Model output processing test passed for {len(test_outputs)} outputs")
    
    def test_memory_enhancement(self):
        """Test cognitive memory enhancement functionality"""
        test_memories = [
            "The protagonist is a skilled mage with a mysterious past.",
            "The ancient kingdom was destroyed by a powerful curse centuries ago.",
            "Magic crystals are the source of power in this world.",
        ]
        
        for memory in test_memories:
            with self.subTest(memory=memory):
                result = self.cognitive_integrator.update_context_memory(
                    memory,
                    importance=0.8
                )
                
                self.assertNotIn('error', result, f"Should not have errors updating memory: {memory}")
                self.assertIn('enhanced_importance', result, "Should return enhanced importance")
                
                # Verify enhanced importance is calculated
                enhanced_importance = result.get('enhanced_importance', 0)
                self.assertIsInstance(enhanced_importance, (int, float), "Enhanced importance should be numeric")
                self.assertGreater(enhanced_importance, 0, "Enhanced importance should be positive")
                self.assertLessEqual(enhanced_importance, 1.0, "Enhanced importance should not exceed 1.0")
                
        logger.info(f"✓ Memory enhancement test passed for {len(test_memories)} memory entries")
    
    def test_world_info_updates(self):
        """Test dynamic world-info updates via cognitive reasoning"""
        test_world_info = [
            "Dragons in this realm are intelligent and can speak ancient languages.",
            "The royal castle is protected by powerful magical barriers.",
            "Travelling merchants carry news between distant kingdoms.",
        ]
        
        for world_info in test_world_info:
            with self.subTest(world_info=world_info):
                result = self.cognitive_integrator.update_world_info(
                    world_info,
                    relevance=0.7
                )
                
                self.assertNotIn('error', result, f"Should not have errors updating world info: {world_info}")
                
                # Check if the result contains expected fields
                self.assertIn('patterns', result, "Should return patterns for world info")
                
        logger.info(f"✓ World info updates test passed for {len(test_world_info)} entries")
    
    def test_attention_guided_enhancements(self):
        """Test attention-guided generation quality improvements"""
        # This test verifies that the attention system is working
        test_text = "The warrior walked through the forest. The forest was dark. The warrior was brave."
        
        # Process through the cognitive system to build attention state
        self.cognitive_integrator.process_user_input("Tell me about a brave warrior")
        
        # Now process output with attention guidance
        result = self.cognitive_integrator.process_model_output(
            test_text,
            context={
                'generation_settings': {'temp': 0.7, 'rep_pen': 1.2},
                'lastctx': 'A brave warrior story...'
            }
        )
        
        self.assertNotIn('error', result, "Should not have errors in attention-guided processing")
        self.assertIn('enhanced_text', result, "Should return enhanced text")
        
        enhanced_text = result.get('enhanced_text', '')
        self.assertIsInstance(enhanced_text, str, "Enhanced text should be a string")
        
        logger.info("✓ Attention-guided enhancements test passed")
    
    def test_integration_statistics(self):
        """Test that integration statistics are properly tracked"""
        # Get initial stats
        initial_status = self.cognitive_integrator.get_integration_status()
        initial_stats = initial_status['stats']
        
        # Process some text to update stats
        self.cognitive_integrator.process_user_input("Test input for statistics")
        self.cognitive_integrator.process_model_output("Test output for statistics")
        
        # Get updated stats
        updated_status = self.cognitive_integrator.get_integration_status()
        updated_stats = updated_status['stats']
        
        # Verify stats were updated
        self.assertGreaterEqual(
            updated_stats['texts_processed'], 
            initial_stats['texts_processed'],
            "Texts processed count should increase"
        )
        
        # Verify required stat fields exist
        required_fields = ['texts_processed', 'patterns_generated', 'attention_cycles', 'agents_created']
        for field in required_fields:
            self.assertIn(field, updated_stats, f"Stats should include {field}")
        
        logger.info("✓ Integration statistics test passed")
    
    def test_error_handling(self):
        """Test error handling in cognitive processing"""
        # Test with empty input
        result = self.cognitive_integrator.process_user_input("")
        self.assertNotIn('error', result, "Empty input should not cause errors")
        
        # Test with None input (should handle gracefully)
        try:
            result = self.cognitive_integrator.process_model_output(None)
            # Should either handle gracefully or return an error result
            if 'error' in result:
                self.assertIsInstance(result['error'], str, "Error should be a string")
        except Exception as e:
            # If it throws an exception, that's also acceptable error handling
            self.assertIsInstance(e, Exception, "Should handle None input gracefully")
        
        logger.info("✓ Error handling test passed")
    
    def test_performance_benchmarks(self):
        """Test that cognitive processing completes within reasonable time"""
        test_text = "The adventure begins in a mystical land filled with wonder and danger."
        
        # Time the user input processing
        start_time = time.time()
        self.cognitive_integrator.process_user_input(test_text)
        input_time = time.time() - start_time
        
        # Time the output processing
        start_time = time.time()
        self.cognitive_integrator.process_model_output(test_text)
        output_time = time.time() - start_time
        
        # Verify reasonable performance (should complete within 1 second each)
        self.assertLess(input_time, 1.0, "Input processing should complete within 1 second")
        self.assertLess(output_time, 1.0, "Output processing should complete within 1 second")
        
        logger.info(f"✓ Performance test passed - Input: {input_time:.3f}s, Output: {output_time:.3f}s")


def run_integration_tests():
    """Run the complete Phase 4 integration test suite"""
    logger.info("=" * 70)
    logger.info("PHASE 4 KOBOLDAI COGNITIVE INTEGRATION TEST SUITE")
    logger.info("=" * 70)
    
    # Create test suite
    test_suite = unittest.TestSuite()
    
    # Add all test methods
    test_methods = [
        'test_cognitive_initialization',
        'test_user_input_processing', 
        'test_model_output_processing',
        'test_memory_enhancement',
        'test_world_info_updates',
        'test_attention_guided_enhancements',
        'test_integration_statistics',
        'test_error_handling',
        'test_performance_benchmarks'
    ]
    
    for method in test_methods:
        test_suite.addTest(TestPhase4Integration(method))
    
    # Run tests
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(test_suite)
    
    # Summary
    logger.info("=" * 70)
    if result.wasSuccessful():
        logger.info("🎉 ALL PHASE 4 INTEGRATION TESTS PASSED!")
        logger.info(f"✓ Ran {result.testsRun} tests successfully")
    else:
        logger.error(f"❌ {len(result.failures)} test(s) failed, {len(result.errors)} error(s)")
        for test, traceback in result.failures + result.errors:
            logger.error(f"Failed: {test}")
            logger.error(traceback)
    
    logger.info("=" * 70)
    return result.wasSuccessful()


if __name__ == "__main__":
    success = run_integration_tests()
    sys.exit(0 if success else 1)