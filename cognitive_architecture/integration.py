"""
KoboldAI Cognitive Architecture Integration

This module integrates the cognitive architecture with the existing KoboldAI
system, providing hooks for text processing, attention allocation, and
distributed cognitive processing.
"""

import asyncio
import logging
import threading
from typing import Dict, Any, Optional, List
import time
import json

# Import cognitive architecture components
from .core import cognitive_core, CognitiveAgent, CognitiveState
from .scheme_adapters.grammar_adapter import scheme_adapter
from .ecan_attention.attention_kernel import ecan_system, AttentionValue
from .distributed_mesh.orchestrator import mesh_orchestrator, DistributedTask, MeshNode, MeshNodeType

logger = logging.getLogger(__name__)


class KoboldCognitiveIntegrator:
    """Integrates cognitive architecture with KoboldAI"""
    
    def __init__(self):
        self.is_initialized = False
        self.cognitive_thread = None
        self.attention_thread = None
        self.integration_stats = {
            'texts_processed': 0,
            'patterns_generated': 0,
            'attention_cycles': 0,
            'agents_created': 0,
            'start_time': time.time()
        }
        
        # Cache for recent translations
        self.translation_cache: Dict[str, Dict[str, Any]] = {}
        self.cache_max_size = 1000
        
        # Integration settings
        self.settings = {
            'enable_attention_allocation': True,
            'enable_scheme_translation': True,
            'enable_distributed_processing': True,
            'attention_update_interval': 0.1,
            'cognitive_cycle_interval': 0.1,
            'cache_translations': True,
            'max_concurrent_tasks': 10
        }
    
    def initialize(self) -> bool:
        """Initialize the cognitive architecture integration"""
        try:
            logger.info("Initializing KoboldAI cognitive architecture integration...")
            
            # Register default cognitive agent for KoboldAI
            kobold_agent = CognitiveAgent()
            kobold_agent.agent_id = "kobold_main_agent"
            cognitive_core.register_agent(kobold_agent)
            
            # Register attention elements for KoboldAI components
            ecan_system.register_cognitive_element("user_input", AttentionValue(sti=0.8, urgency=0.7))
            ecan_system.register_cognitive_element("model_output", AttentionValue(sti=0.7, lti=0.5))
            ecan_system.register_cognitive_element("context_memory", AttentionValue(sti=0.3, lti=0.9))
            ecan_system.register_cognitive_element("world_info", AttentionValue(sti=0.4, lti=0.8))
            ecan_system.register_cognitive_element("author_note", AttentionValue(sti=0.6, lti=0.6))
            
            # Add spreading activation links
            ecan_system.add_spreading_link("user_input", "model_output", 0.9)
            ecan_system.add_spreading_link("context_memory", "model_output", 0.7)
            ecan_system.add_spreading_link("world_info", "context_memory", 0.6)
            ecan_system.add_spreading_link("author_note", "user_input", 0.5)
            
            # Register mesh nodes for KoboldAI processing
            kobold_processor = MeshNode(
                node_id="kobold_text_processor",
                node_type=MeshNodeType.PROCESSOR,
                capabilities={"text_generation", "context_processing", "memory_management"},
                max_load=1.0
            )
            mesh_orchestrator.register_node(kobold_processor)
            
            kobold_translator = MeshNode(
                node_id="kobold_scheme_translator",
                node_type=MeshNodeType.PROCESSOR,
                capabilities={"scheme_translation", "pattern_matching", "grammar_analysis"},
                max_load=0.8
            )
            mesh_orchestrator.register_node(kobold_translator)
            
            # Register default Scheme patterns for common KoboldAI constructs
            self._register_default_patterns()
            
            # Start background threads
            self._start_background_processing()
            
            self.is_initialized = True
            self.integration_stats['agents_created'] += 1
            
            logger.info("Cognitive architecture integration initialized successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to initialize cognitive architecture: {e}")
            return False
    
    def _register_default_patterns(self):
        """Register default Scheme patterns for KoboldAI"""
        default_patterns = [
            ("character_concept", "(ConceptNode \"Character\")", 1.0),
            ("action_predicate", "(PredicateNode \"performs_action\")", 1.0),
            ("dialogue_pattern", "(EvaluationLink (PredicateNode \"says\") (ListLink (ConceptNode \"Character\") (ConceptNode \"Speech\")))", 0.9),
            ("narrative_flow", "(ImplicationLink (ConceptNode \"Event\") (ConceptNode \"Consequence\"))", 0.8),
            ("world_building", "(EvaluationLink (PredicateNode \"located_in\") (ListLink (ConceptNode \"Entity\") (ConceptNode \"Location\")))", 0.9),
            ("temporal_sequence", "(EvaluationLink (PredicateNode \"happens_after\") (ListLink (ConceptNode \"Event1\") (ConceptNode \"Event2\")))", 0.8)
        ]
        
        for name, pattern, confidence in default_patterns:
            scheme_adapter.register_pattern(name, pattern, confidence)
    
    def _start_background_processing(self):
        """Start background processing threads"""
        if self.settings['enable_attention_allocation']:
            self.attention_thread = threading.Thread(target=self._attention_loop, daemon=True)
            self.attention_thread.start()
        
        if self.settings['enable_distributed_processing']:
            self.cognitive_thread = threading.Thread(target=self._cognitive_loop, daemon=True)
            self.cognitive_thread.start()
    
    def _attention_loop(self):
        """Background attention allocation loop"""
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        try:
            while self.is_initialized:
                loop.run_until_complete(ecan_system.run_attention_cycle())
                self.integration_stats['attention_cycles'] += 1
                time.sleep(self.settings['attention_update_interval'])
        except Exception as e:
            logger.error(f"Error in attention loop: {e}")
        finally:
            loop.close()
    
    def _cognitive_loop(self):
        """Background cognitive processing loop"""
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        try:
            while self.is_initialized:
                loop.run_until_complete(cognitive_core.process_cognitive_cycle())
                time.sleep(self.settings['cognitive_cycle_interval'])
        except Exception as e:
            logger.error(f"Error in cognitive loop: {e}")
        finally:
            loop.close()
    
    def process_user_input(self, user_text: str, context: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Process user input through cognitive architecture"""
        try:
            # Update attention for user input
            if self.settings['enable_attention_allocation']:
                ecan_system.update_urgency("user_input", 0.9)
                ecan_system.update_novelty_detection("user_input", self._calculate_novelty(user_text))
            
            # Translate to AtomSpace patterns if enabled
            atomspace_patterns = []
            if self.settings['enable_scheme_translation']:
                # Check cache first
                cache_key = f"input_{hash(user_text)}"
                if self.settings['cache_translations'] and cache_key in self.translation_cache:
                    cached_result = self.translation_cache[cache_key]
                    atomspace_patterns = cached_result['patterns']
                else:
                    atomspace_patterns = scheme_adapter.translate_kobold_to_atomspace(user_text)
                    if self.settings['cache_translations']:
                        self._cache_translation(cache_key, {'patterns': atomspace_patterns, 'text': user_text})
            
            # Update cognitive agent state
            if "kobold_main_agent" in cognitive_core.agents:
                agent = cognitive_core.agents["kobold_main_agent"]
                agent.update_state(CognitiveState.ATTENDING)
            
            # Submit distributed processing task if enabled
            task_id = None
            if self.settings['enable_distributed_processing']:
                task = DistributedTask(
                    task_type="text_generation",
                    payload={
                        "user_text": user_text,
                        "atomspace_patterns": atomspace_patterns,
                        "context": context or {}
                    },
                    priority=8
                )
                task_id = mesh_orchestrator.submit_task(task)
            
            self.integration_stats['texts_processed'] += 1
            self.integration_stats['patterns_generated'] += len(atomspace_patterns)
            
            return {
                "atomspace_patterns": atomspace_patterns,
                "task_id": task_id,
                "attention_elements": self._get_attention_summary(),
                "cognitive_state": self._get_cognitive_state(),
                "processing_timestamp": time.time()
            }
            
        except Exception as e:
            logger.error(f"Error processing user input: {e}")
            return {"error": str(e)}
    
    def process_model_output(self, generated_text: str, context: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Process model output through cognitive architecture"""
        try:
            # Update attention for model output
            if self.settings['enable_attention_allocation']:
                ecan_system.update_urgency("model_output", 0.7)
                # Spread activation from model output to context memory
                ecan_system.spread_activation("model_output")
            
            # Translate output to AtomSpace patterns
            atomspace_patterns = []
            if self.settings['enable_scheme_translation']:
                cache_key = f"output_{hash(generated_text)}"
                if self.settings['cache_translations'] and cache_key in self.translation_cache:
                    cached_result = self.translation_cache[cache_key]
                    atomspace_patterns = cached_result['patterns']
                else:
                    atomspace_patterns = scheme_adapter.translate_kobold_to_atomspace(generated_text)
                    if self.settings['cache_translations']:
                        self._cache_translation(cache_key, {'patterns': atomspace_patterns, 'text': generated_text})
            
            # Update cognitive agent state
            if "kobold_main_agent" in cognitive_core.agents:
                agent = cognitive_core.agents["kobold_main_agent"]
                agent.update_state(CognitiveState.RESPONDING)
            
            # Submit analysis task
            task_id = None
            if self.settings['enable_distributed_processing']:
                task = DistributedTask(
                    task_type="output_analysis",
                    payload={
                        "generated_text": generated_text,
                        "atomspace_patterns": atomspace_patterns,
                        "context": context or {}
                    },
                    priority=6
                )
                task_id = mesh_orchestrator.submit_task(task)
            
            self.integration_stats['texts_processed'] += 1
            self.integration_stats['patterns_generated'] += len(atomspace_patterns)
            
            return {
                "atomspace_patterns": atomspace_patterns,
                "task_id": task_id,
                "attention_elements": self._get_attention_summary(),
                "cognitive_state": self._get_cognitive_state(),
                "processing_timestamp": time.time()
            }
            
        except Exception as e:
            logger.error(f"Error processing model output: {e}")
            return {"error": str(e)}
    
    def update_context_memory(self, memory_content: str, importance: float = 0.5):
        """Update context memory with attention allocation"""
        try:
            if self.settings['enable_attention_allocation']:
                # Update LTI for context memory based on importance
                if "context_memory" in ecan_system.element_attention:
                    ecan_system.element_attention["context_memory"].lti = min(1.0, importance)
                    ecan_system.element_attention["context_memory"].sti = importance * 0.3
            
            # Translate memory content
            if self.settings['enable_scheme_translation'] and memory_content:
                patterns = scheme_adapter.translate_kobold_to_atomspace(memory_content)
                
                # Submit memory update task
                if self.settings['enable_distributed_processing']:
                    task = DistributedTask(
                        task_type="memory_management",
                        payload={
                            "memory_content": memory_content,
                            "patterns": patterns,
                            "importance": importance
                        },
                        priority=4
                    )
                    mesh_orchestrator.submit_task(task)
                
                return {"patterns": patterns, "importance": importance}
            
            return {"importance": importance}
            
        except Exception as e:
            logger.error(f"Error updating context memory: {e}")
            return {"error": str(e)}
    
    def update_world_info(self, world_info: str, relevance: float = 0.6):
        """Update world info with attention allocation"""
        try:
            if self.settings['enable_attention_allocation']:
                # Update VLTI for world info (long-term persistent information)
                if "world_info" in ecan_system.element_attention:
                    ecan_system.element_attention["world_info"].vlti = min(1.0, relevance)
                    ecan_system.element_attention["world_info"].lti = relevance * 0.8
            
            # Process world info patterns
            if self.settings['enable_scheme_translation'] and world_info:
                patterns = scheme_adapter.translate_kobold_to_atomspace(world_info)
                
                # Create implication patterns for world consistency
                if len(patterns) > 1:
                    for i, pattern in enumerate(patterns[:-1]):
                        next_pattern = patterns[i + 1]
                        scheme_adapter.create_implication_pattern(pattern, next_pattern)
                
                return {"patterns": patterns, "relevance": relevance}
            
            return {"relevance": relevance}
            
        except Exception as e:
            logger.error(f"Error updating world info: {e}")
            return {"error": str(e)}
    
    def _calculate_novelty(self, text: str) -> float:
        """Calculate novelty score for text"""
        # Simple novelty calculation based on cache hits
        cache_key = f"novelty_{hash(text)}"
        if cache_key in self.translation_cache:
            return 0.1  # Low novelty for cached content
        
        # Check for unique words/patterns
        words = text.lower().split()
        unique_ratio = len(set(words)) / len(words) if words else 0
        
        return min(1.0, unique_ratio * 0.8 + 0.2)
    
    def _cache_translation(self, key: str, data: Dict[str, Any]):
        """Cache translation result"""
        if len(self.translation_cache) >= self.cache_max_size:
            # Remove oldest entry
            oldest_key = next(iter(self.translation_cache))
            del self.translation_cache[oldest_key]
        
        self.translation_cache[key] = {
            **data,
            'timestamp': time.time()
        }
    
    def _get_attention_summary(self) -> Dict[str, Any]:
        """Get summary of current attention allocation"""
        if not self.settings['enable_attention_allocation']:
            return {}
        
        try:
            attention_stats = ecan_system.get_attention_statistics()
            return {
                "total_elements": attention_stats.get("total_elements", 0),
                "average_sti": attention_stats.get("average_sti", 0),
                "mesh_load": attention_stats.get("mesh_load", 0),
                "top_elements": attention_stats.get("top_elements", [])[:3]  # Top 3 only
            }
        except Exception as e:
            logger.error(f"Error getting attention summary: {e}")
            return {"error": str(e)}
    
    def _get_cognitive_state(self) -> Dict[str, Any]:
        """Get current cognitive state"""
        try:
            if "kobold_main_agent" in cognitive_core.agents:
                agent = cognitive_core.agents["kobold_main_agent"]
                return {
                    "agent_id": agent.agent_id,
                    "state": agent.state.value,
                    "activation_level": agent.activation_level,
                    "hypergraph_nodes": len(agent.hypergraph_nodes),
                    "hypergraph_links": len(agent.hypergraph_links)
                }
            return {}
        except Exception as e:
            logger.error(f"Error getting cognitive state: {e}")
            return {"error": str(e)}
    
    def get_integration_status(self) -> Dict[str, Any]:
        """Get comprehensive integration status"""
        try:
            mesh_status = mesh_orchestrator.get_mesh_status()
            attention_stats = ecan_system.get_attention_statistics()
            scheme_stats = scheme_adapter.get_pattern_statistics()
            hypergraph = cognitive_core.get_global_hypergraph()
            
            uptime = time.time() - self.integration_stats['start_time']
            
            return {
                "is_initialized": self.is_initialized,
                "uptime_seconds": uptime,
                "settings": self.settings,
                "integration_stats": self.integration_stats,
                "cache_size": len(self.translation_cache),
                "mesh_status": {
                    "nodes_online": mesh_status.get("statistics", {}).get("nodes_online", 0),
                    "tasks_pending": mesh_status.get("tasks", {}).get("pending", 0),
                    "tasks_completed": mesh_status.get("statistics", {}).get("tasks_completed", 0)
                },
                "attention_status": {
                    "total_elements": attention_stats.get("total_elements", 0),
                    "average_sti": attention_stats.get("average_sti", 0),
                    "allocation_rounds": attention_stats.get("allocation_rounds", 0)
                },
                "scheme_status": {
                    "total_patterns": scheme_stats.get("total_patterns", 0),
                    "concept_patterns": scheme_stats.get("concept_patterns", 0),
                    "implication_patterns": scheme_stats.get("implication_patterns", 0)
                },
                "hypergraph_status": {
                    "agent_count": hypergraph.get("agent_count", 0),
                    "node_count": hypergraph.get("node_count", 0),
                    "link_count": hypergraph.get("link_count", 0)
                },
                "timestamp": time.time()
            }
            
        except Exception as e:
            logger.error(f"Error getting integration status: {e}")
            return {"error": str(e), "is_initialized": self.is_initialized}
    
    def update_settings(self, new_settings: Dict[str, Any]) -> bool:
        """Update integration settings"""
        try:
            valid_settings = {
                'enable_attention_allocation',
                'enable_scheme_translation',
                'enable_distributed_processing',
                'attention_update_interval',
                'cognitive_cycle_interval',
                'cache_translations',
                'max_concurrent_tasks'
            }
            
            for key, value in new_settings.items():
                if key in valid_settings:
                    self.settings[key] = value
                    logger.info(f"Updated setting {key} to {value}")
            
            return True
            
        except Exception as e:
            logger.error(f"Error updating settings: {e}")
            return False
    
    def shutdown(self):
        """Shutdown the cognitive integration"""
        try:
            logger.info("Shutting down cognitive architecture integration...")
            
            self.is_initialized = False
            
            # Wait for threads to finish
            if self.attention_thread and self.attention_thread.is_alive():
                self.attention_thread.join(timeout=5)
            
            if self.cognitive_thread and self.cognitive_thread.is_alive():
                self.cognitive_thread.join(timeout=5)
            
            # Shutdown components
            cognitive_core.stop()
            mesh_orchestrator.shutdown()
            
            # Clear cache
            self.translation_cache.clear()
            
            logger.info("Cognitive architecture integration shutdown complete")
            
        except Exception as e:
            logger.error(f"Error during shutdown: {e}")


# Global cognitive integrator instance
kobold_cognitive_integrator = KoboldCognitiveIntegrator()


def initialize_cognitive_architecture() -> bool:
    """Initialize the cognitive architecture for KoboldAI"""
    return kobold_cognitive_integrator.initialize()


def process_kobold_input(text: str, context: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Process KoboldAI input through cognitive architecture"""
    return kobold_cognitive_integrator.process_user_input(text, context)


def process_kobold_output(text: str, context: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Process KoboldAI output through cognitive architecture"""
    return kobold_cognitive_integrator.process_model_output(text, context)


def update_kobold_memory(memory: str, importance: float = 0.5) -> Dict[str, Any]:
    """Update KoboldAI memory through cognitive architecture"""
    return kobold_cognitive_integrator.update_context_memory(memory, importance)


def update_kobold_worldinfo(worldinfo: str, relevance: float = 0.6) -> Dict[str, Any]:
    """Update KoboldAI world info through cognitive architecture"""
    return kobold_cognitive_integrator.update_world_info(worldinfo, relevance)


def get_cognitive_status() -> Dict[str, Any]:
    """Get cognitive architecture status"""
    return kobold_cognitive_integrator.get_integration_status()


def shutdown_cognitive_architecture():
    """Shutdown the cognitive architecture"""
    kobold_cognitive_integrator.shutdown()