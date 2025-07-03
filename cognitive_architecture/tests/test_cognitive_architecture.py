"""
Comprehensive Test Suite for Cognitive Architecture

This module provides exhaustive testing for all cognitive architecture components,
ensuring no mocks are used and all transformations are real.
"""

import pytest
import asyncio
import time
import json
from typing import Dict, Any, List
import numpy as np

# Import all cognitive architecture components
from ..core import (
    CognitiveArchitectureCore, CognitiveAgent, TensorShape, CognitiveState, cognitive_core
)
from ..scheme_adapters.grammar_adapter import (
    SchemeGrammarAdapter, SchemeExpression, SchemeType, GrammarPattern, scheme_adapter
)
from ..ecan_attention.attention_kernel import (
    EconomicAttentionNetwork, AttentionValue, AttentionFocus, AttentionType, ecan_system
)
from ..distributed_mesh.orchestrator import (
    CognitiveMeshOrchestrator, MeshNode, MeshNodeType, DistributedTask, TaskStatus, mesh_orchestrator
)


class TestCognitiveArchitectureCore:
    """Test suite for the cognitive architecture core"""
    
    def test_tensor_shape_creation(self):
        """Test tensor shape creation with prime factorization"""
        tensor_shape = TensorShape(modality=512, depth=64, context=2048, salience=128, autonomy_index=32)
        
        # Verify dimensions
        assert tensor_shape.modality == 512
        assert tensor_shape.depth == 64
        assert tensor_shape.context == 2048
        assert tensor_shape.salience == 128
        assert tensor_shape.autonomy_index == 32
        
        # Verify prime signature is generated
        assert tensor_shape.prime_signature is not None
        assert len(tensor_shape.prime_signature) > 0
        
        # Test prime factorization
        factors_512 = tensor_shape._prime_factors(512)
        assert factors_512 == [2, 2, 2, 2, 2, 2, 2, 2, 2]  # 2^9 = 512
        
        factors_64 = tensor_shape._prime_factors(64)
        assert factors_64 == [2, 2, 2, 2, 2, 2]  # 2^6 = 64
    
    def test_cognitive_agent_creation(self):
        """Test cognitive agent creation and state management"""
        agent = CognitiveAgent()
        
        # Verify initial state
        assert agent.state == CognitiveState.IDLE
        assert agent.agent_id is not None
        assert len(agent.agent_id) == 16  # SHA256 hash truncated to 16 chars
        
        # Test hypergraph node creation
        assert agent.agent_id in agent.hypergraph_nodes
        node = agent.hypergraph_nodes[agent.agent_id]
        assert node['type'] == 'agent'
        assert node['state'] == CognitiveState.IDLE.value
        
        # Test tensor shape integration
        tensor_dict = agent.tensor_shape.to_dict()
        assert 'modality' in tensor_dict
        assert 'prime_signature' in tensor_dict
    
    def test_cognitive_state_transitions(self):
        """Test cognitive state transitions with hypergraph propagation"""
        agent = CognitiveAgent()
        
        # Test state transition
        agent.update_state(CognitiveState.ATTENDING)
        assert agent.state == CognitiveState.ATTENDING
        
        # Verify hypergraph link creation
        transition_links = [link for link in agent.hypergraph_links.values() 
                          if link['type'] == 'state_transition']
        assert len(transition_links) == 1
        
        transition = transition_links[0]
        assert transition['from_state'] == CognitiveState.IDLE.value
        assert transition['to_state'] == CognitiveState.ATTENDING.value
        assert transition['agent_id'] == agent.agent_id
    
    def test_cognitive_core_agent_registration(self):
        """Test agent registration in cognitive core"""
        core = CognitiveArchitectureCore()
        agent = CognitiveAgent()
        
        agent_id = core.register_agent(agent)
        
        assert agent_id in core.agents
        assert core.agents[agent_id] == agent
        
        # Verify hypergraph merge
        assert agent_id in core.global_hypergraph['nodes']
    
    def test_hypergraph_fragment_encoding(self):
        """Test hypergraph fragment encoding"""
        agent = CognitiveAgent()
        
        # Add some state transitions
        agent.update_state(CognitiveState.PROCESSING)
        agent.update_state(CognitiveState.RESPONDING)
        
        fragment = agent.encode_as_hypergraph_fragment()
        
        # Verify fragment structure
        assert 'nodes' in fragment
        assert 'links' in fragment
        assert 'tensor_shape' in fragment
        assert 'activation_level' in fragment
        
        # Verify nodes and links are populated
        assert len(fragment['nodes']) > 0
        assert len(fragment['links']) > 0
    
    @pytest.mark.asyncio
    async def test_cognitive_processing_cycle(self):
        """Test cognitive processing cycle"""
        core = CognitiveArchitectureCore()
        
        # Create and register multiple agents
        agents = []
        for i in range(3):
            agent = CognitiveAgent()
            core.register_agent(agent)
            agents.append(agent)
        
        # Run processing cycle
        await core.process_cognitive_cycle()
        
        # Verify agents may have changed states
        states = [agent.state for agent in agents]
        assert len(states) == 3
        
        # Verify global hypergraph is updated
        global_graph = core.get_global_hypergraph()
        assert global_graph['agent_count'] == 3
        assert global_graph['node_count'] >= 3
    
    def test_global_hypergraph_structure(self):
        """Test global hypergraph structure and statistics"""
        core = CognitiveArchitectureCore()
        
        # Add multiple agents
        for i in range(5):
            agent = CognitiveAgent()
            core.register_agent(agent)
        
        global_graph = core.get_global_hypergraph()
        
        # Verify structure
        assert 'hypergraph' in global_graph
        assert 'agent_count' in global_graph
        assert 'node_count' in global_graph
        assert 'link_count' in global_graph
        assert 'timestamp' in global_graph
        
        # Verify counts
        assert global_graph['agent_count'] == 5
        assert global_graph['node_count'] >= 5


class TestSchemeGrammarAdapter:
    """Test suite for Scheme grammar adapters"""
    
    def test_scheme_expression_creation(self):
        """Test Scheme expression creation and string representation"""
        # Test concept node
        concept = SchemeExpression(SchemeType.CONCEPT, "Entity")
        assert str(concept) == '(ConceptNode "Entity")'
        
        # Test predicate node
        predicate = SchemeExpression(SchemeType.PREDICATE, "exists")
        assert str(predicate) == '(PredicateNode "exists")'
        
        # Test implication with children
        implication = SchemeExpression(SchemeType.IMPLICATION, "implies")
        implication.children = [concept, predicate]
        assert str(implication) == '(ImplicationLink (ConceptNode "Entity") (PredicateNode "exists"))'
    
    def test_grammar_pattern_parsing(self):
        """Test grammar pattern parsing to Scheme expressions"""
        adapter = SchemeGrammarAdapter()
        
        # Test concept pattern
        concept_pattern = '(ConceptNode "Entity")'
        pattern = GrammarPattern("test_concept", concept_pattern)
        scheme_expr = pattern.parse_to_scheme()
        
        assert scheme_expr.type == SchemeType.CONCEPT
        assert scheme_expr.value == "Entity"
        
        # Test predicate pattern
        predicate_pattern = '(PredicateNode "exists")'
        pattern = GrammarPattern("test_predicate", predicate_pattern)
        scheme_expr = pattern.parse_to_scheme()
        
        assert scheme_expr.type == SchemeType.PREDICATE
        assert scheme_expr.value == "exists"
    
    def test_kobold_to_atomspace_translation(self):
        """Test real translation from KoboldAI text to AtomSpace patterns"""
        adapter = SchemeGrammarAdapter()
        
        # Test with actual text
        kobold_text = "The Dragon attacked the Village. The Hero defended bravely."
        
        atomspace_patterns = adapter.translate_kobold_to_atomspace(kobold_text)
        
        # Verify patterns are generated
        assert len(atomspace_patterns) > 0
        
        # Check for expected patterns
        concept_patterns = [p for p in atomspace_patterns if "ConceptNode" in p]
        predicate_patterns = [p for p in atomspace_patterns if "PredicateNode" in p]
        relationship_patterns = [p for p in atomspace_patterns if "EvaluationLink" in p]
        
        assert len(concept_patterns) > 0
        assert len(predicate_patterns) > 0
        assert len(relationship_patterns) > 0
        
        # Verify specific concepts are extracted
        dragon_pattern = any("Dragon" in p for p in concept_patterns)
        village_pattern = any("Village" in p for p in concept_patterns)
        hero_pattern = any("Hero" in p for p in concept_patterns)
        
        assert dragon_pattern or village_pattern or hero_pattern
    
    def test_atomspace_to_kobold_translation(self):
        """Test real translation from AtomSpace patterns to KoboldAI text"""
        adapter = SchemeGrammarAdapter()
        
        # Create real AtomSpace patterns
        patterns = [
            '(ConceptNode "Dragon")',
            '(ConceptNode "Village")',
            '(PredicateNode "attacks")',
            '(EvaluationLink (PredicateNode "attacks") (ConceptNode "Dragon"))'
        ]
        
        translated_text = adapter.translate_atomspace_to_kobold(patterns)
        
        # Verify translation produces readable text
        assert isinstance(translated_text, str)
        assert len(translated_text) > 0
        assert "Dragon" in translated_text
        assert "Village" in translated_text
        assert "attacks" in translated_text
    
    def test_bidirectional_translation(self):
        """Test bidirectional translation maintains information"""
        adapter = SchemeGrammarAdapter()
        
        original_text = "The Wizard casts magic spells."
        
        # Forward translation
        atomspace_patterns = adapter.translate_kobold_to_atomspace(original_text)
        
        # Backward translation
        back_translated = adapter.translate_atomspace_to_kobold(atomspace_patterns)
        
        # Verify key information is preserved
        assert "Wizard" in back_translated or "magic" in back_translated or "spells" in back_translated
    
    def test_implication_pattern_creation(self):
        """Test creation of implication patterns"""
        adapter = SchemeGrammarAdapter()
        
        antecedent = "The Hero has a sword."
        consequent = "The Hero can fight monsters."
        
        pattern_name = adapter.create_implication_pattern(antecedent, consequent)
        
        assert pattern_name != ""
        assert pattern_name in adapter.patterns
        
        pattern = adapter.patterns[pattern_name]
        assert "ImplicationLink" in pattern.pattern
    
    @pytest.mark.asyncio
    async def test_batch_translation_processing(self):
        """Test batch translation processing"""
        adapter = SchemeGrammarAdapter()
        
        texts = [
            "The Knight rides a horse.",
            "The Princess lives in a castle.",
            "The Wizard studies ancient magic."
        ]
        
        results = await adapter.process_translation_batch(texts)
        
        assert len(results) == 3
        
        for result in results:
            assert 'original_text' in result
            assert 'atomspace_patterns' in result
            assert 'back_translation' in result
            assert 'pattern_count' in result
            assert result['pattern_count'] > 0
    
    def test_pattern_statistics(self):
        """Test pattern statistics calculation"""
        adapter = SchemeGrammarAdapter()
        
        # Register some patterns
        adapter.register_pattern("test1", "(ConceptNode \"Test1\")", 0.9)
        adapter.register_pattern("test2", "(PredicateNode \"test2\")", 0.8)
        adapter.register_pattern("test3", "(ImplicationLink (ConceptNode \"A\") (ConceptNode \"B\"))", 0.7)
        
        stats = adapter.get_pattern_statistics()
        
        assert stats['total_patterns'] >= 3
        assert stats['concept_patterns'] >= 1
        assert stats['predicate_patterns'] >= 1
        assert stats['implication_patterns'] >= 1
        assert 0 <= stats['average_confidence'] <= 1


class TestECANAttentionSystem:
    """Test suite for ECAN attention allocation"""
    
    def test_attention_value_creation(self):
        """Test attention value creation and bounds checking"""
        attention = AttentionValue(sti=0.8, lti=0.6, urgency=0.9, novelty=0.7)
        
        # Verify values are within bounds
        assert 0 <= attention.sti <= 1
        assert 0 <= attention.lti <= 1
        assert 0 <= attention.urgency <= 1
        assert 0 <= attention.novelty <= 1
        
        # Test bounds enforcement
        attention_extreme = AttentionValue(sti=1.5, lti=-0.5, urgency=2.0, novelty=-1.0)
        assert attention_extreme.sti == 1.0
        assert attention_extreme.lti == 0.0
        assert attention_extreme.urgency == 1.0
        assert attention_extreme.novelty == 0.0
    
    def test_composite_attention_calculation(self):
        """Test composite attention calculation"""
        attention = AttentionValue(sti=0.8, lti=0.6, vlti=0.4, urgency=0.9, novelty=0.7)
        
        composite = attention.get_composite_attention()
        
        # Verify composite is calculated correctly
        expected = 0.4 * 0.8 + 0.3 * 0.6 + 0.1 * 0.4 + 0.1 * 0.9 + 0.1 * 0.7
        assert abs(composite - expected) < 0.001
    
    def test_attention_decay(self):
        """Test attention value decay over time"""
        attention = AttentionValue(sti=1.0, urgency=1.0, novelty=1.0)
        
        original_sti = attention.sti
        original_urgency = attention.urgency
        original_novelty = attention.novelty
        
        attention.decay(0.1)
        
        # Verify decay applied
        assert attention.sti < original_sti
        assert attention.urgency < original_urgency
        assert attention.novelty < original_novelty
    
    def test_ecan_element_registration(self):
        """Test cognitive element registration"""
        ecan = EconomicAttentionNetwork()
        
        element_id = "test_element"
        attention = AttentionValue(sti=0.5, lti=0.3)
        
        ecan.register_cognitive_element(element_id, attention)
        
        assert element_id in ecan.element_attention
        assert ecan.element_attention[element_id] == attention
    
    def test_attention_focus_creation(self):
        """Test attention focus creation and management"""
        ecan = EconomicAttentionNetwork()
        
        # Register elements
        elements = {"elem1", "elem2", "elem3"}
        for elem in elements:
            ecan.register_cognitive_element(elem, AttentionValue(sti=0.5))
        
        # Create focus
        focus_id = "test_focus"
        ecan.create_attention_focus(focus_id, elements)
        
        assert focus_id in ecan.attention_foci
        focus = ecan.attention_foci[focus_id]
        assert focus.element_ids == elements
    
    def test_spreading_activation(self):
        """Test spreading activation between elements"""
        ecan = EconomicAttentionNetwork()
        
        # Register elements
        source_id = "source"
        target_id = "target"
        
        ecan.register_cognitive_element(source_id, AttentionValue(sti=0.8))
        ecan.register_cognitive_element(target_id, AttentionValue(sti=0.2))
        
        # Add spreading link
        ecan.add_spreading_link(source_id, target_id, 0.5)
        
        # Get initial STI values
        initial_source_sti = ecan.element_attention[source_id].sti
        initial_target_sti = ecan.element_attention[target_id].sti
        
        # Spread activation
        spread_results = ecan.spread_activation(source_id)
        
        # Verify spreading occurred
        assert len(spread_results) > 0
        assert target_id in spread_results
        
        # Verify STI values changed
        final_source_sti = ecan.element_attention[source_id].sti
        final_target_sti = ecan.element_attention[target_id].sti
        
        assert final_source_sti < initial_source_sti
        assert final_target_sti > initial_target_sti
    
    def test_sti_budget_allocation(self):
        """Test STI budget allocation across elements"""
        ecan = EconomicAttentionNetwork(total_sti_budget=1000.0)
        
        # Register elements with different attention values
        ecan.register_cognitive_element("high_attention", AttentionValue(sti=0.9))
        ecan.register_cognitive_element("medium_attention", AttentionValue(sti=0.5))
        ecan.register_cognitive_element("low_attention", AttentionValue(sti=0.1))
        
        # Allocate budget
        allocation = ecan.allocate_sti_budget()
        
        # Verify allocation occurred
        assert len(allocation) > 0
        assert "high_attention" in allocation
        assert "medium_attention" in allocation
        assert "low_attention" in allocation
        
        # Verify higher attention elements get more budget
        assert allocation["high_attention"] >= allocation["medium_attention"]
        assert allocation["medium_attention"] >= allocation["low_attention"]
    
    def test_attention_focus_selection(self):
        """Test attention focus selection algorithm"""
        ecan = EconomicAttentionNetwork()
        
        # Create elements and foci
        for i in range(5):
            elem_id = f"elem_{i}"
            ecan.register_cognitive_element(elem_id, AttentionValue(sti=0.5 + i * 0.1))
            
            focus_id = f"focus_{i}"
            ecan.create_attention_focus(focus_id, {elem_id})
        
        # Select top foci
        selected_foci = ecan.select_attention_foci(max_foci=3)
        
        # Verify selection
        assert len(selected_foci) <= 3
        assert len(selected_foci) > 0
    
    @pytest.mark.asyncio
    async def test_attention_cycle_execution(self):
        """Test complete attention allocation cycle"""
        ecan = EconomicAttentionNetwork()
        
        # Register elements
        elements = ["text_input", "model_output", "user_intent"]
        for elem in elements:
            ecan.register_cognitive_element(elem, AttentionValue(sti=0.5, lti=0.3))
        
        # Add spreading links
        ecan.add_spreading_link("text_input", "model_output", 0.8)
        ecan.add_spreading_link("user_intent", "text_input", 0.6)
        
        # Run attention cycle
        result = await ecan.run_attention_cycle()
        
        # Verify cycle results
        assert 'round' in result
        assert 'sti_allocation' in result
        assert 'lti_allocation' in result
        assert 'spread_results' in result
        assert 'selected_foci' in result
        
        # Verify allocations occurred
        assert len(result['sti_allocation']) > 0
        assert len(result['lti_allocation']) > 0
    
    def test_attention_statistics(self):
        """Test attention system statistics"""
        ecan = EconomicAttentionNetwork()
        
        # Register elements
        for i in range(10):
            ecan.register_cognitive_element(f"elem_{i}", AttentionValue(sti=0.1 * i))
        
        stats = ecan.get_attention_statistics()
        
        # Verify statistics structure
        assert 'total_elements' in stats
        assert 'total_sti' in stats
        assert 'total_lti' in stats
        assert 'average_sti' in stats
        assert 'average_lti' in stats
        assert 'top_elements' in stats
        
        # Verify statistics values
        assert stats['total_elements'] == 10
        assert stats['total_sti'] > 0
        assert len(stats['top_elements']) <= 10


class TestDistributedMesh:
    """Test suite for distributed cognitive mesh"""
    
    def test_mesh_node_creation(self):
        """Test mesh node creation and properties"""
        node = MeshNode(
            node_type=MeshNodeType.AGENT,
            capabilities={"reasoning", "dialogue"},
            max_load=0.8
        )
        
        assert node.node_type == MeshNodeType.AGENT
        assert "reasoning" in node.capabilities
        assert "dialogue" in node.capabilities
        assert node.max_load == 0.8
        assert node.current_load == 0.0
        assert node.status == "online"
    
    def test_node_availability_check(self):
        """Test node availability checking"""
        node = MeshNode(max_load=1.0)
        
        # Initially available
        assert node.is_available() == True
        
        # Not available when overloaded
        node.current_load = 1.5
        assert node.is_available() == False
        
        # Not available when offline
        node.current_load = 0.5
        node.status = "offline"
        assert node.is_available() == False
    
    def test_distributed_task_creation(self):
        """Test distributed task creation and lifecycle"""
        task = DistributedTask(
            task_type="text_processing",
            payload={"text": "Hello world"},
            priority=7,
            timeout=60.0
        )
        
        assert task.task_type == "text_processing"
        assert task.payload["text"] == "Hello world"
        assert task.priority == 7
        assert task.timeout == 60.0
        assert task.status == TaskStatus.PENDING
    
    def test_task_execution_lifecycle(self):
        """Test task execution lifecycle"""
        task = DistributedTask(task_type="test_task")
        
        # Start execution
        node_id = "test_node"
        task.start_execution(node_id)
        
        assert task.status == TaskStatus.RUNNING
        assert task.assigned_node == node_id
        assert task.started_at is not None
        
        # Complete execution
        result = {"output": "task completed"}
        task.complete_execution(result)
        
        assert task.status == TaskStatus.COMPLETED
        assert task.result == result
        assert task.completed_at is not None
        
        # Verify execution time
        exec_time = task.get_execution_time()
        assert exec_time is not None
        assert exec_time > 0
    
    def test_task_failure_handling(self):
        """Test task failure handling"""
        task = DistributedTask(task_type="test_task")
        
        task.start_execution("test_node")
        
        error_msg = "Network connection failed"
        task.fail_execution(error_msg)
        
        assert task.status == TaskStatus.FAILED
        assert task.error == error_msg
        assert task.completed_at is not None
    
    def test_task_expiration(self):
        """Test task expiration checking"""
        task = DistributedTask(task_type="test_task", timeout=0.1)  # 100ms timeout
        
        # Initially not expired
        assert task.is_expired() == False
        
        # Start execution
        task.start_execution("test_node")
        
        # Wait for expiration
        time.sleep(0.2)
        
        # Should be expired now
        assert task.is_expired() == True
    
    def test_mesh_orchestrator_node_registration(self):
        """Test node registration in mesh orchestrator"""
        orchestrator = CognitiveMeshOrchestrator()
        
        node = MeshNode(
            node_type=MeshNodeType.PROCESSOR,
            capabilities={"neural_inference", "attention_allocation"}
        )
        
        node_id = orchestrator.register_node(node)
        
        assert node_id in orchestrator.nodes
        assert orchestrator.nodes[node_id] == node
        assert orchestrator.orchestration_stats['nodes_online'] > 0
    
    def test_mesh_task_submission(self):
        """Test task submission to mesh"""
        orchestrator = CognitiveMeshOrchestrator()
        
        task = DistributedTask(
            task_type="cognitive_processing",
            payload={"data": "test data"},
            priority=8
        )
        
        task_id = orchestrator.submit_task(task)
        
        assert task_id in orchestrator.tasks
        assert orchestrator.tasks[task_id] == task
        assert task in orchestrator.task_queue
    
    def test_suitable_node_finding(self):
        """Test finding suitable nodes for tasks"""
        orchestrator = CognitiveMeshOrchestrator()
        
        # Register nodes with different capabilities
        node1 = MeshNode(capabilities={"text_processing"}, current_load=0.2)
        node2 = MeshNode(capabilities={"neural_inference"}, current_load=0.8)
        node3 = MeshNode(capabilities={"text_processing", "neural_inference"}, current_load=0.5)
        
        orchestrator.register_node(node1)
        orchestrator.register_node(node2)
        orchestrator.register_node(node3)
        
        # Create task requiring text processing
        task = DistributedTask(task_type="text_processing")
        
        # Find suitable node
        suitable_node = orchestrator._find_suitable_node(task)
        
        assert suitable_node is not None
        assert suitable_node.can_handle_task("text_processing")
        assert suitable_node.is_available()
    
    def test_task_assignment_and_completion(self):
        """Test task assignment and completion flow"""
        orchestrator = CognitiveMeshOrchestrator()
        
        # Register node
        node = MeshNode(capabilities={"test_task"})
        orchestrator.register_node(node)
        
        # Submit task
        task = DistributedTask(task_type="test_task")
        task_id = orchestrator.submit_task(task)
        
        # Simulate task completion
        result = {"output": "task completed successfully"}
        orchestrator.handle_task_completion(task_id, result, node.node_id)
        
        # Verify task was completed
        completed_task = orchestrator.tasks[task_id]
        assert completed_task.status == TaskStatus.COMPLETED
        assert completed_task.result == result
        assert completed_task in orchestrator.completed_tasks
    
    def test_task_failure_handling_in_orchestrator(self):
        """Test task failure handling in orchestrator"""
        orchestrator = CognitiveMeshOrchestrator()
        
        # Register node
        node = MeshNode(capabilities={"test_task"})
        orchestrator.register_node(node)
        
        # Submit task
        task = DistributedTask(task_type="test_task")
        task_id = orchestrator.submit_task(task)
        
        # Simulate task failure
        error_msg = "Processing failed"
        orchestrator.handle_task_failure(task_id, error_msg, node.node_id)
        
        # Verify task was failed
        failed_task = orchestrator.tasks[task_id]
        assert failed_task.status == TaskStatus.FAILED
        assert failed_task.error == error_msg
        assert failed_task in orchestrator.completed_tasks
    
    def test_mesh_status_reporting(self):
        """Test mesh status reporting"""
        orchestrator = CognitiveMeshOrchestrator()
        
        # Register nodes
        for i in range(3):
            node = MeshNode(capabilities={"test_task"})
            orchestrator.register_node(node)
        
        # Submit tasks
        for i in range(5):
            task = DistributedTask(task_type="test_task")
            orchestrator.submit_task(task)
        
        # Get mesh status
        status = orchestrator.get_mesh_status()
        
        # Verify status structure
        assert 'nodes' in status
        assert 'tasks' in status
        assert 'statistics' in status
        assert 'timestamp' in status
        
        # Verify counts
        assert len(status['nodes']) == 3
        assert status['tasks']['pending'] == 5
    
    def test_node_performance_metrics(self):
        """Test node performance metrics calculation"""
        orchestrator = CognitiveMeshOrchestrator()
        
        # Register node
        node = MeshNode(capabilities={"test_task"})
        orchestrator.register_node(node)
        
        # Submit and complete tasks
        for i in range(3):
            task = DistributedTask(task_type="test_task")
            task_id = orchestrator.submit_task(task)
            
            # Simulate completion
            result = {"output": f"task {i} completed"}
            orchestrator.handle_task_completion(task_id, result, node.node_id)
        
        # Get performance metrics
        performance = orchestrator.get_node_performance(node.node_id)
        
        assert performance is not None
        assert performance['tasks_completed'] == 3
        assert performance['tasks_failed'] == 0
        assert performance['success_rate'] == 1.0
    
    def test_mesh_orchestrator_shutdown(self):
        """Test mesh orchestrator shutdown"""
        orchestrator = CognitiveMeshOrchestrator()
        
        # Register nodes and submit tasks
        node = MeshNode(capabilities={"test_task"})
        orchestrator.register_node(node)
        
        task = DistributedTask(task_type="test_task")
        orchestrator.submit_task(task)
        
        # Shutdown orchestrator
        orchestrator.shutdown()
        
        # Verify shutdown state
        assert orchestrator.is_running == False
        
        # Verify pending tasks are cancelled
        assert task.status == TaskStatus.CANCELLED


class TestIntegration:
    """Integration tests for the complete cognitive architecture"""
    
    @pytest.mark.asyncio
    async def test_full_system_integration(self):
        """Test integration of all cognitive architecture components"""
        # Initialize systems
        core = CognitiveArchitectureCore()
        ecan = EconomicAttentionNetwork()
        adapter = SchemeGrammarAdapter()
        orchestrator = CognitiveMeshOrchestrator()
        
        # Create cognitive agent
        agent = CognitiveAgent()
        agent_id = core.register_agent(agent)
        
        # Register attention elements
        ecan.register_cognitive_element(agent_id, AttentionValue(sti=0.7, lti=0.5))
        
        # Register mesh node
        node = MeshNode(capabilities={"cognitive_processing"})
        orchestrator.register_node(node)
        
        # Process text through scheme adapter
        test_text = "The intelligent agent processes complex information."
        atomspace_patterns = adapter.translate_kobold_to_atomspace(test_text)
        
        # Submit cognitive task
        task = DistributedTask(
            task_type="cognitive_processing",
            payload={"text": test_text, "patterns": atomspace_patterns}
        )
        task_id = orchestrator.submit_task(task)
        
        # Run cognitive cycle
        await core.process_cognitive_cycle()
        
        # Run attention cycle
        await ecan.run_attention_cycle()
        
        # Verify integration
        assert len(atomspace_patterns) > 0
        assert task_id in orchestrator.tasks
        assert agent_id in ecan.element_attention
        assert agent_id in core.agents
        
        # Verify attention allocation
        attention_stats = ecan.get_attention_statistics()
        assert attention_stats['total_elements'] > 0
        
        # Verify hypergraph integration
        hypergraph = core.get_global_hypergraph()
        assert hypergraph['agent_count'] > 0
        assert hypergraph['node_count'] > 0
    
    def test_scheme_to_mesh_integration(self):
        """Test integration between Scheme adapter and distributed mesh"""
        adapter = SchemeGrammarAdapter()
        orchestrator = CognitiveMeshOrchestrator()
        
        # Register translation node
        node = MeshNode(capabilities={"scheme_translation"})
        orchestrator.register_node(node)
        
        # Create translation task
        test_text = "The wizard casts powerful spells."
        task = DistributedTask(
            task_type="scheme_translation",
            payload={"text": test_text, "translation_type": "kobold_to_atomspace"}
        )
        
        task_id = orchestrator.submit_task(task)
        
        # Process translation
        atomspace_patterns = adapter.translate_kobold_to_atomspace(test_text)
        
        # Simulate task completion
        result = {"atomspace_patterns": atomspace_patterns}
        orchestrator.handle_task_completion(task_id, result, node.node_id)
        
        # Verify integration
        completed_task = orchestrator.tasks[task_id]
        assert completed_task.status == TaskStatus.COMPLETED
        assert "atomspace_patterns" in completed_task.result
        assert len(completed_task.result["atomspace_patterns"]) > 0
    
    def test_attention_to_mesh_integration(self):
        """Test integration between ECAN attention and distributed mesh"""
        ecan = EconomicAttentionNetwork()
        orchestrator = CognitiveMeshOrchestrator()
        
        # Register attention allocation node
        node = MeshNode(capabilities={"attention_allocation"})
        orchestrator.register_node(node)
        
        # Register cognitive elements
        elements = ["task_1", "task_2", "task_3"]
        for elem in elements:
            ecan.register_cognitive_element(elem, AttentionValue(sti=0.5))
        
        # Create attention allocation task
        task = DistributedTask(
            task_type="attention_allocation",
            payload={"elements": elements}
        )
        
        task_id = orchestrator.submit_task(task)
        
        # Get attention statistics
        stats = ecan.get_attention_statistics()
        
        # Simulate task completion
        result = {"attention_stats": stats}
        orchestrator.handle_task_completion(task_id, result, node.node_id)
        
        # Verify integration
        completed_task = orchestrator.tasks[task_id]
        assert completed_task.status == TaskStatus.COMPLETED
        assert "attention_stats" in completed_task.result


if __name__ == "__main__":
    # Run the tests
    pytest.main([__file__, "-v", "--tb=short"])