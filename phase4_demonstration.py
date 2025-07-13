#!/usr/bin/env python3
"""
Phase 4 KoboldAI Cognitive Integration Demonstration

This script demonstrates the key features of the cognitive architecture
integration with KoboldAI's text generation pipeline.
"""

import sys
import os
import time
import json
from typing import Dict, Any

# Add the project root to the path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

def print_header(title: str):
    """Print a formatted header"""
    print("\n" + "=" * 70)
    print(f"  {title}")
    print("=" * 70)

def print_result(result: Dict[str, Any], title: str):
    """Print a formatted result"""
    print(f"\n🔍 {title}")
    print("-" * 50)
    
    if 'error' in result:
        print(f"❌ Error: {result['error']}")
        return
    
    # Show key metrics
    if 'atomspace_patterns' in result:
        patterns = result['atomspace_patterns']
        print(f"📝 AtomSpace Patterns Generated: {len(patterns)}")
        if patterns:
            print(f"   Example: {patterns[0][:100]}..." if len(patterns[0]) > 100 else f"   Example: {patterns[0]}")
    
    if 'enhanced_text' in result:
        enhanced = result['enhanced_text']
        print(f"✨ Enhanced Text: {enhanced[:150]}..." if len(enhanced) > 150 else f"✨ Enhanced Text: {enhanced}")
    
    if 'enhanced_importance' in result:
        importance = result['enhanced_importance']
        print(f"🎯 Enhanced Importance Score: {importance:.3f}")
    
    if 'attention_elements' in result:
        attention = result['attention_elements']
        print(f"🧠 Active Attention Elements: {len(attention)}")
        for elem, values in list(attention.items())[:3]:  # Show first 3
            if isinstance(values, dict) and 'sti' in values:
                print(f"   {elem}: STI={values['sti']:.2f}")
    
    if 'task_id' in result and result['task_id']:
        print(f"⚙️  Distributed Task ID: {result['task_id'][:16]}...")
    
    print(f"⏱️  Processing Time: {result.get('processing_timestamp', 0):.3f}")

def demonstrate_cognitive_integration():
    """Demonstrate the cognitive integration features"""
    
    print_header("PHASE 4 KOBOLDAI COGNITIVE INTEGRATION DEMONSTRATION")
    
    try:
        # Import and initialize cognitive integration
        from cognitive_architecture.integration import kobold_cognitive_integrator
        
        print("\n🚀 Initializing cognitive architecture...")
        success = kobold_cognitive_integrator.initialize()
        
        if not success:
            print("❌ Failed to initialize cognitive architecture")
            return False
        
        print("✅ Cognitive architecture initialized successfully!")
        
        # Get initial status
        status = kobold_cognitive_integrator.get_integration_status()
        print(f"\n📊 System Status:")
        print(f"   • Initialized: {status['is_initialized']}")
        print(f"   • Agents Created: {status['stats']['agents_created']}")
        print(f"   • Cache Size: {status['cache_size']}")
        
        # 1. Demonstrate User Input Processing
        print_header("1. USER INPUT PROCESSING WITH COGNITIVE ENHANCEMENT")
        
        user_inputs = [
            "The brave knight ventured into the dark, mysterious forest filled with ancient magic.",
            "Cast a powerful healing spell to restore the wounded warrior's strength.",
            "The dragon's treasure hoard glittered with countless gems and golden artifacts."
        ]
        
        for i, user_input in enumerate(user_inputs, 1):
            print(f"\n📥 Processing User Input #{i}:")
            print(f"   Input: {user_input}")
            
            result = kobold_cognitive_integrator.process_user_input(
                user_input,
                context={'actionmode': 0, 'gamestarted': True, 'demo': True}
            )
            
            print_result(result, f"Cognitive Processing Result #{i}")
        
        # 2. Demonstrate Model Output Enhancement
        print_header("2. MODEL OUTPUT ENHANCEMENT WITH ATTENTION GUIDANCE")
        
        model_outputs = [
            "The knight's sword gleamed in the moonlight as he approached the castle gates. The ancient stones whispered secrets of battles long past.",
            "Magic energy crackled through the air, forming intricate patterns of light. The spell began to take shape, weaving reality itself.",
            "Deep within the cavern, the dragon's eyes reflected infinite wisdom. Its voice echoed with the weight of centuries."
        ]
        
        for i, output in enumerate(model_outputs, 1):
            print(f"\n📤 Enhancing Model Output #{i}:")
            print(f"   Original: {output}")
            
            result = kobold_cognitive_integrator.process_model_output(
                output,
                context={
                    'lastctx': 'Previous story context...',
                    'story_length': i * 2,
                    'generation_settings': {
                        'temp': 0.7,
                        'top_p': 0.9,
                        'rep_pen': 1.1
                    }
                }
            )
            
            print_result(result, f"Enhancement Result #{i}")
        
        # 3. Demonstrate Memory Enhancement
        print_header("3. COGNITIVE MEMORY ENHANCEMENT")
        
        memory_entries = [
            "The protagonist is Aeliana, a skilled battle-mage with the ability to manipulate time magic.",
            "The ancient kingdom of Thalorin was destroyed by a curse that turned all living things to crystal.",
            "The mystical Starforge Crystals are the source of all magical power in this realm."
        ]
        
        for i, memory in enumerate(memory_entries, 1):
            print(f"\n🧠 Processing Memory Entry #{i}:")
            print(f"   Content: {memory}")
            
            result = kobold_cognitive_integrator.update_context_memory(
                memory,
                importance=0.6 + (i * 0.1)  # Varying importance
            )
            
            print_result(result, f"Memory Enhancement #{i}")
        
        # 4. Demonstrate World-Info Updates
        print_header("4. DYNAMIC WORLD-INFO COGNITIVE REASONING")
        
        world_info_entries = [
            "Dragons in this realm are sentient beings with their own complex society and ancient traditions.",
            "The capital city of Luminspire is built on floating islands connected by bridges of pure light.",
            "Time magic is extremely rare and dangerous, as it can create paradoxes that tear reality apart."
        ]
        
        for i, world_info in enumerate(world_info_entries, 1):
            print(f"\n🌍 Processing World Info #{i}:")
            print(f"   Content: {world_info}")
            
            result = kobold_cognitive_integrator.update_world_info(
                world_info,
                relevance=0.7 + (i * 0.05)  # Varying relevance
            )
            
            print_result(result, f"World Info Update #{i}")
        
        # 5. Show Final Statistics
        print_header("5. FINAL INTEGRATION STATISTICS")
        
        final_status = kobold_cognitive_integrator.get_integration_status()
        stats = final_status['stats']
        
        print(f"\n📈 Processing Statistics:")
        print(f"   • Total Texts Processed: {stats['texts_processed']}")
        print(f"   • AtomSpace Patterns Generated: {stats['patterns_generated']}")
        print(f"   • Attention Cycles Completed: {stats['attention_cycles']}")
        print(f"   • System Uptime: {time.time() - stats['start_time']:.2f} seconds")
        
        print(f"\n🏗️  System Architecture:")
        mesh_status = final_status.get('mesh_status', {})
        attention_status = final_status.get('attention_status', {})
        
        print(f"   • Mesh Nodes Online: {mesh_status.get('nodes_online', 0)}")
        print(f"   • Tasks Completed: {mesh_status.get('tasks_completed', 0)}")
        print(f"   • Attention Elements: {attention_status.get('total_elements', 0)}")
        print(f"   • Average STI Score: {attention_status.get('average_sti', 0):.3f}")
        
        print_header("🎉 COGNITIVE INTEGRATION DEMONSTRATION COMPLETE")
        
        print("\n✨ Key Features Demonstrated:")
        print("   ✅ User input processing with cognitive attention allocation")
        print("   ✅ Model output enhancement with attention-guided quality improvements")
        print("   ✅ Memory importance scoring with cognitive analysis")
        print("   ✅ World-info dynamic updates via cognitive reasoning")
        print("   ✅ Real-time attention distribution and task scheduling")
        print("   ✅ AtomSpace pattern generation for enhanced reasoning")
        
        print("\n🚀 The cognitive architecture is now fully integrated with KoboldAI!")
        print("   Ready for enhanced story generation with cognitive awareness.")
        
        return True
        
    except ImportError as e:
        print(f"❌ Failed to import cognitive architecture: {e}")
        print("   Make sure all dependencies are installed (numpy, websockets, aiohttp)")
        return False
    except Exception as e:
        print(f"❌ Error during demonstration: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = demonstrate_cognitive_integration()
    sys.exit(0 if success else 1)