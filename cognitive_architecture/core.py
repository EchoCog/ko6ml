"""
Cognitive Architecture Core Module

This module provides the foundational infrastructure for the distributed agentic
cognitive grammar network, integrating with the existing KoboldAI infrastructure.
"""

import asyncio
import json
import logging
from typing import Dict, Any, Optional, List, Tuple
import numpy as np
from dataclasses import dataclass, field
from enum import Enum
import hashlib
import time

logger = logging.getLogger(__name__)


class CognitiveState(Enum):
    """States of cognitive processing"""
    IDLE = "idle"
    ATTENDING = "attending"
    PROCESSING = "processing"
    INTEGRATING = "integrating"
    RESPONDING = "responding"


@dataclass
class TensorShape:
    """Tensor shape with semantic dimensions"""
    modality: int = 512       # Input modality dimension
    depth: int = 64           # Processing depth
    context: int = 2048       # Context window size
    salience: int = 128       # Attention salience
    autonomy_index: int = 32  # Agent autonomy level
    
    def __post_init__(self):
        """Ensure tensor dimensions use prime factorization for uniqueness"""
        self.prime_signature = self._generate_prime_signature()
    
    def _generate_prime_signature(self) -> str:
        """Generate unique prime factorization signature"""
        dims = [self.modality, self.depth, self.context, self.salience, self.autonomy_index]
        signature = []
        for dim in dims:
            factors = self._prime_factors(dim)
            signature.append(f"{dim}:{','.join(map(str, factors))}")
        return "|".join(signature)
    
    def _prime_factors(self, n: int) -> List[int]:
        """Calculate prime factors of a number"""
        factors = []
        d = 2
        while d * d <= n:
            while n % d == 0:
                factors.append(d)
                n //= d
            d += 1
        if n > 1:
            factors.append(n)
        return factors
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary representation"""
        return {
            'modality': self.modality,
            'depth': self.depth,
            'context': self.context,
            'salience': self.salience,
            'autonomy_index': self.autonomy_index,
            'prime_signature': self.prime_signature
        }


@dataclass
class CognitiveAgent:
    """A cognitive agent with hypergraph state representation"""
    agent_id: str = field(default_factory=lambda: hashlib.sha256(str(time.time()).encode()).hexdigest()[:16])
    state: CognitiveState = CognitiveState.IDLE
    tensor_shape: TensorShape = field(default_factory=TensorShape)
    hypergraph_nodes: Dict[str, Any] = field(default_factory=dict)
    hypergraph_links: Dict[str, Any] = field(default_factory=dict)
    attention_weights: np.ndarray = field(default_factory=lambda: np.random.random(128))
    activation_level: float = 0.0
    
    def __post_init__(self):
        """Initialize agent with hypergraph representation"""
        self.hypergraph_nodes[self.agent_id] = {
            'type': 'agent',
            'state': self.state.value,
            'tensor_shape': self.tensor_shape.to_dict(),
            'created_at': time.time()
        }
    
    def update_state(self, new_state: CognitiveState):
        """Update cognitive state with hypergraph propagation"""
        old_state = self.state
        self.state = new_state
        
        # Update hypergraph node
        self.hypergraph_nodes[self.agent_id]['state'] = new_state.value
        self.hypergraph_nodes[self.agent_id]['updated_at'] = time.time()
        
        # Create state transition link
        transition_id = f"{self.agent_id}_transition_{time.time()}"
        self.hypergraph_links[transition_id] = {
            'type': 'state_transition',
            'from_state': old_state.value,
            'to_state': new_state.value,
            'agent_id': self.agent_id,
            'timestamp': time.time()
        }
        
        logger.info(f"Agent {self.agent_id} transitioned from {old_state.value} to {new_state.value}")
    
    def encode_as_hypergraph_fragment(self) -> Dict[str, Any]:
        """Encode agent state as hypergraph fragment"""
        return {
            'nodes': self.hypergraph_nodes,
            'links': self.hypergraph_links,
            'tensor_shape': self.tensor_shape.to_dict(),
            'activation_level': self.activation_level,
            'timestamp': time.time()
        }
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary representation"""
        return {
            'agent_id': self.agent_id,
            'state': self.state.value,
            'tensor_shape': self.tensor_shape.to_dict(),
            'hypergraph_fragment': self.encode_as_hypergraph_fragment(),
            'activation_level': self.activation_level
        }


class CognitiveArchitectureCore:
    """Core cognitive architecture system"""
    
    def __init__(self):
        self.agents: Dict[str, CognitiveAgent] = {}
        self.global_hypergraph: Dict[str, Any] = {'nodes': {}, 'links': {}}
        self.attention_allocator = None  # Will be initialized by ECAN kernel
        self.is_running = False
        
    def register_agent(self, agent: CognitiveAgent) -> str:
        """Register a cognitive agent"""
        self.agents[agent.agent_id] = agent
        
        # Merge agent hypergraph into global hypergraph
        self._merge_hypergraph_fragment(agent.encode_as_hypergraph_fragment())
        
        logger.info(f"Registered cognitive agent: {agent.agent_id}")
        return agent.agent_id
    
    def _merge_hypergraph_fragment(self, fragment: Dict[str, Any]):
        """Merge hypergraph fragment into global hypergraph"""
        # Merge nodes
        for node_id, node_data in fragment['nodes'].items():
            self.global_hypergraph['nodes'][node_id] = node_data
        
        # Merge links
        for link_id, link_data in fragment['links'].items():
            self.global_hypergraph['links'][link_id] = link_data
    
    def get_agent_state(self, agent_id: str) -> Optional[Dict[str, Any]]:
        """Get agent state representation"""
        if agent_id in self.agents:
            return self.agents[agent_id].to_dict()
        return None
    
    def get_global_hypergraph(self) -> Dict[str, Any]:
        """Get global hypergraph representation"""
        return {
            'hypergraph': self.global_hypergraph,
            'agent_count': len(self.agents),
            'node_count': len(self.global_hypergraph['nodes']),
            'link_count': len(self.global_hypergraph['links']),
            'timestamp': time.time()
        }
    
    async def process_cognitive_cycle(self):
        """Execute one cognitive processing cycle"""
        for agent in self.agents.values():
            # Simple state transition logic for demonstration
            if agent.state == CognitiveState.IDLE and np.random.random() > 0.7:
                agent.update_state(CognitiveState.ATTENDING)
            elif agent.state == CognitiveState.ATTENDING and np.random.random() > 0.5:
                agent.update_state(CognitiveState.PROCESSING)
            elif agent.state == CognitiveState.PROCESSING and np.random.random() > 0.4:
                agent.update_state(CognitiveState.INTEGRATING)
            elif agent.state == CognitiveState.INTEGRATING and np.random.random() > 0.6:
                agent.update_state(CognitiveState.RESPONDING)
            elif agent.state == CognitiveState.RESPONDING:
                agent.update_state(CognitiveState.IDLE)
            
            # Update activation level
            agent.activation_level = min(1.0, agent.activation_level + np.random.normal(0, 0.1))
            
            # Update global hypergraph
            self._merge_hypergraph_fragment(agent.encode_as_hypergraph_fragment())
    
    async def start_cognitive_loop(self):
        """Start the main cognitive processing loop"""
        self.is_running = True
        logger.info("Starting cognitive architecture loop")
        
        while self.is_running:
            await self.process_cognitive_cycle()
            await asyncio.sleep(0.1)  # 100ms cycle time
    
    def stop(self):
        """Stop the cognitive processing loop"""
        self.is_running = False
        logger.info("Stopping cognitive architecture loop")


# Global cognitive architecture instance
cognitive_core = CognitiveArchitectureCore()