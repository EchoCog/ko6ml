"""
ECAN-Inspired Attention Allocation Kernel

This module implements an Economic Attention Networks (ECAN) inspired attention allocation
system for distributed cognitive resource management and activation spreading.
"""

import numpy as np
import asyncio
import logging
from typing import Dict, Any, List, Optional, Tuple, Set
from dataclasses import dataclass, field
from enum import Enum
import time
import math
from collections import defaultdict, deque
import heapq

logger = logging.getLogger(__name__)


class AttentionType(Enum):
    """Types of attention mechanisms"""
    STI = "short_term_importance"  # Short-term importance
    LTI = "long_term_importance"   # Long-term importance
    VLTI = "very_long_term_importance"  # Very long-term importance
    URGENCY = "urgency"            # Urgency-based attention
    NOVELTY = "novelty"            # Novelty-based attention


@dataclass
class AttentionValue:
    """Represents attention values for cognitive elements"""
    sti: float = 0.0              # Short-term importance
    lti: float = 0.0              # Long-term importance
    vlti: float = 0.0             # Very long-term importance
    urgency: float = 0.0          # Urgency value
    novelty: float = 0.0          # Novelty value
    confidence: float = 0.0       # Confidence in attention values
    
    def __post_init__(self):
        """Ensure values are within valid ranges"""
        self.sti = max(0.0, min(1.0, self.sti))
        self.lti = max(0.0, min(1.0, self.lti))
        self.vlti = max(0.0, min(1.0, self.vlti))
        self.urgency = max(0.0, min(1.0, self.urgency))
        self.novelty = max(0.0, min(1.0, self.novelty))
        self.confidence = max(0.0, min(1.0, self.confidence))
    
    def get_composite_attention(self) -> float:
        """Calculate composite attention value"""
        weights = [0.4, 0.3, 0.1, 0.1, 0.1]  # STI, LTI, VLTI, urgency, novelty
        values = [self.sti, self.lti, self.vlti, self.urgency, self.novelty]
        return sum(w * v for w, v in zip(weights, values))
    
    def decay(self, decay_rate: float = 0.01):
        """Apply temporal decay to attention values"""
        self.sti *= (1 - decay_rate)
        self.urgency *= (1 - decay_rate * 2)  # Urgency decays faster
        self.novelty *= (1 - decay_rate * 1.5)  # Novelty decays moderately fast
    
    def to_dict(self) -> Dict[str, float]:
        """Convert to dictionary representation"""
        return {
            'sti': self.sti,
            'lti': self.lti,
            'vlti': self.vlti,
            'urgency': self.urgency,
            'novelty': self.novelty,
            'confidence': self.confidence,
            'composite': self.get_composite_attention()
        }


@dataclass
class AttentionFocus:
    """Represents an attention focus with associated cognitive elements"""
    focus_id: str
    element_ids: Set[str] = field(default_factory=set)
    attention_value: AttentionValue = field(default_factory=AttentionValue)
    created_at: float = field(default_factory=time.time)
    last_updated: float = field(default_factory=time.time)
    activation_history: deque = field(default_factory=lambda: deque(maxlen=100))
    
    def add_element(self, element_id: str):
        """Add cognitive element to focus"""
        self.element_ids.add(element_id)
        self.last_updated = time.time()
    
    def remove_element(self, element_id: str):
        """Remove cognitive element from focus"""
        self.element_ids.discard(element_id)
        self.last_updated = time.time()
    
    def update_attention(self, new_attention: AttentionValue):
        """Update attention values with history tracking"""
        old_composite = self.attention_value.get_composite_attention()
        self.attention_value = new_attention
        new_composite = self.attention_value.get_composite_attention()
        
        self.activation_history.append({
            'timestamp': time.time(),
            'old_attention': old_composite,
            'new_attention': new_composite,
            'change': new_composite - old_composite
        })
        
        self.last_updated = time.time()
    
    def get_activation_trend(self) -> float:
        """Get recent activation trend"""
        if len(self.activation_history) < 2:
            return 0.0
        
        recent_changes = [entry['change'] for entry in list(self.activation_history)[-10:]]
        return sum(recent_changes) / len(recent_changes)


class EconomicAttentionNetwork:
    """ECAN-inspired attention allocation system"""
    
    def __init__(self, total_sti_budget: float = 1000.0, total_lti_budget: float = 1000.0):
        self.total_sti_budget = total_sti_budget
        self.total_lti_budget = total_lti_budget
        self.attention_foci: Dict[str, AttentionFocus] = {}
        self.element_attention: Dict[str, AttentionValue] = {}
        self.spreading_activation_graph: Dict[str, List[Tuple[str, float]]] = defaultdict(list)
        self.resource_allocation_history: deque = deque(maxlen=1000)
        
        # ECAN parameters
        self.max_spread_percentage = 0.2  # Maximum percentage of STI that can be spread
        self.spread_threshold = 0.1       # Minimum STI for spreading
        self.decay_rate = 0.01           # Decay rate for attention values
        self.focus_selection_threshold = 0.5  # Threshold for focus selection
        
        # Attention allocation state
        self.current_sti_allocation = 0.0
        self.current_lti_allocation = 0.0
        self.allocation_round = 0
        
        # Priority queue for focus management
        self.focus_priority_queue = []
        
    def register_cognitive_element(self, element_id: str, initial_attention: Optional[AttentionValue] = None):
        """Register a new cognitive element for attention tracking"""
        if initial_attention is None:
            initial_attention = AttentionValue()
        
        self.element_attention[element_id] = initial_attention
        logger.info(f"Registered cognitive element: {element_id}")
    
    def create_attention_focus(self, focus_id: str, element_ids: Set[str]) -> str:
        """Create a new attention focus"""
        focus = AttentionFocus(focus_id, element_ids)
        self.attention_foci[focus_id] = focus
        
        # Add to priority queue
        heapq.heappush(self.focus_priority_queue, 
                      (-focus.attention_value.get_composite_attention(), focus_id))
        
        logger.info(f"Created attention focus: {focus_id} with {len(element_ids)} elements")
        return focus_id
    
    def add_spreading_link(self, source_id: str, target_id: str, weight: float = 1.0):
        """Add a spreading activation link between cognitive elements"""
        self.spreading_activation_graph[source_id].append((target_id, weight))
        logger.debug(f"Added spreading link: {source_id} -> {target_id} (weight: {weight})")
    
    def allocate_sti_budget(self) -> Dict[str, float]:
        """Allocate STI budget across cognitive elements"""
        allocation = {}
        
        # Get all elements sorted by composite attention
        elements_by_attention = sorted(
            self.element_attention.items(),
            key=lambda x: x[1].get_composite_attention(),
            reverse=True
        )
        
        remaining_budget = self.total_sti_budget
        
        for element_id, attention_value in elements_by_attention:
            if remaining_budget <= 0:
                break
            
            # Allocate based on composite attention and remaining budget
            composite_attention = attention_value.get_composite_attention()
            allocation_amount = min(
                remaining_budget * composite_attention,
                remaining_budget * 0.3  # Max 30% to any single element
            )
            
            allocation[element_id] = allocation_amount
            remaining_budget -= allocation_amount
            
            # Update element's STI
            attention_value.sti = min(1.0, attention_value.sti + allocation_amount / self.total_sti_budget)
        
        self.current_sti_allocation = self.total_sti_budget - remaining_budget
        return allocation
    
    def allocate_lti_budget(self) -> Dict[str, float]:
        """Allocate LTI budget across cognitive elements"""
        allocation = {}
        
        # LTI allocation based on sustained attention over time
        for element_id, attention_value in self.element_attention.items():
            # Elements with consistent STI should get more LTI
            lti_increment = attention_value.sti * 0.1  # 10% of STI contributes to LTI
            attention_value.lti = min(1.0, attention_value.lti + lti_increment)
            allocation[element_id] = lti_increment
        
        return allocation
    
    def spread_activation(self, source_id: str, spread_amount: float = None) -> Dict[str, float]:
        """Spread activation from source element to connected elements"""
        if source_id not in self.element_attention:
            return {}
        
        source_attention = self.element_attention[source_id]
        
        # Determine spread amount
        if spread_amount is None:
            spread_amount = source_attention.sti * self.max_spread_percentage
        
        # Only spread if above threshold
        if source_attention.sti < self.spread_threshold:
            return {}
        
        # Get spreading targets
        targets = self.spreading_activation_graph.get(source_id, [])
        if not targets:
            return {}
        
        # Calculate spread distribution
        total_weight = sum(weight for _, weight in targets)
        if total_weight == 0:
            return {}
        
        spread_distribution = {}
        
        for target_id, weight in targets:
            if target_id in self.element_attention:
                spread_portion = (weight / total_weight) * spread_amount
                
                # Transfer attention
                target_attention = self.element_attention[target_id]
                target_attention.sti = min(1.0, target_attention.sti + spread_portion)
                
                # Reduce source attention
                source_attention.sti = max(0.0, source_attention.sti - spread_portion)
                
                spread_distribution[target_id] = spread_portion
        
        return spread_distribution
    
    def select_attention_foci(self, max_foci: int = 3) -> List[str]:
        """Select the most important attention foci"""
        # Update priority queue with current attention values
        self.focus_priority_queue = []
        
        for focus_id, focus in self.attention_foci.items():
            # Calculate focus attention based on constituent elements
            total_attention = 0.0
            for element_id in focus.element_ids:
                if element_id in self.element_attention:
                    total_attention += self.element_attention[element_id].get_composite_attention()
            
            avg_attention = total_attention / len(focus.element_ids) if focus.element_ids else 0.0
            focus.attention_value.sti = avg_attention
            
            heapq.heappush(self.focus_priority_queue, (-avg_attention, focus_id))
        
        # Select top foci
        selected_foci = []
        temp_queue = []
        
        for _ in range(min(max_foci, len(self.focus_priority_queue))):
            if self.focus_priority_queue:
                neg_attention, focus_id = heapq.heappop(self.focus_priority_queue)
                attention = -neg_attention
                
                if attention >= self.focus_selection_threshold:
                    selected_foci.append(focus_id)
                
                temp_queue.append((neg_attention, focus_id))
        
        # Restore queue
        for item in temp_queue:
            heapq.heappush(self.focus_priority_queue, item)
        
        return selected_foci
    
    def apply_temporal_decay(self):
        """Apply temporal decay to all attention values"""
        for attention_value in self.element_attention.values():
            attention_value.decay(self.decay_rate)
        
        for focus in self.attention_foci.values():
            focus.attention_value.decay(self.decay_rate)
    
    def update_novelty_detection(self, element_id: str, novelty_score: float):
        """Update novelty detection for cognitive element"""
        if element_id in self.element_attention:
            self.element_attention[element_id].novelty = novelty_score
    
    def update_urgency(self, element_id: str, urgency_score: float):
        """Update urgency for cognitive element"""
        if element_id in self.element_attention:
            self.element_attention[element_id].urgency = urgency_score
    
    async def run_attention_cycle(self):
        """Run one complete attention allocation cycle"""
        self.allocation_round += 1
        
        # 1. Allocate STI budget
        sti_allocation = self.allocate_sti_budget()
        
        # 2. Allocate LTI budget
        lti_allocation = self.allocate_lti_budget()
        
        # 3. Spread activation
        spread_results = {}
        for element_id in list(self.element_attention.keys()):
            if self.element_attention[element_id].sti > self.spread_threshold:
                spread_results[element_id] = self.spread_activation(element_id)
        
        # 4. Select attention foci
        selected_foci = self.select_attention_foci()
        
        # 5. Apply temporal decay
        self.apply_temporal_decay()
        
        # 6. Record allocation history
        allocation_record = {
            'round': self.allocation_round,
            'timestamp': time.time(),
            'sti_allocation': sti_allocation,
            'lti_allocation': lti_allocation,
            'spread_results': spread_results,
            'selected_foci': selected_foci,
            'total_sti_allocated': self.current_sti_allocation,
            'active_elements': len(self.element_attention)
        }
        
        self.resource_allocation_history.append(allocation_record)
        
        logger.debug(f"Completed attention cycle {self.allocation_round}")
        return allocation_record
    
    def get_attention_statistics(self) -> Dict[str, Any]:
        """Get comprehensive attention allocation statistics"""
        total_sti = sum(av.sti for av in self.element_attention.values())
        total_lti = sum(av.lti for av in self.element_attention.values())
        
        avg_sti = total_sti / len(self.element_attention) if self.element_attention else 0
        avg_lti = total_lti / len(self.element_attention) if self.element_attention else 0
        
        # Get top attended elements
        top_elements = sorted(
            self.element_attention.items(),
            key=lambda x: x[1].get_composite_attention(),
            reverse=True
        )[:10]
        
        return {
            'total_elements': len(self.element_attention),
            'total_foci': len(self.attention_foci),
            'total_sti': total_sti,
            'total_lti': total_lti,
            'average_sti': avg_sti,
            'average_lti': avg_lti,
            'allocation_rounds': self.allocation_round,
            'sti_budget_utilization': self.current_sti_allocation / self.total_sti_budget,
            'top_elements': [(elem_id, av.to_dict()) for elem_id, av in top_elements],
            'active_foci': len([f for f in self.attention_foci.values() 
                              if f.attention_value.get_composite_attention() > 0.1])
        }
    
    async def start_attention_loop(self, cycle_interval: float = 0.1):
        """Start the attention allocation loop"""
        logger.info("Starting ECAN attention allocation loop")
        
        while True:
            try:
                await self.run_attention_cycle()
                await asyncio.sleep(cycle_interval)
            except Exception as e:
                logger.error(f"Error in attention cycle: {e}")
                await asyncio.sleep(cycle_interval)


# Global ECAN instance
ecan_system = EconomicAttentionNetwork()

# Register some default cognitive elements for testing
ecan_system.register_cognitive_element("text_input", AttentionValue(sti=0.5, lti=0.2))
ecan_system.register_cognitive_element("model_output", AttentionValue(sti=0.7, lti=0.3))
ecan_system.register_cognitive_element("user_intent", AttentionValue(sti=0.8, lti=0.4, urgency=0.6))
ecan_system.register_cognitive_element("context_memory", AttentionValue(sti=0.3, lti=0.8))

# Add some spreading links
ecan_system.add_spreading_link("text_input", "model_output", 0.8)
ecan_system.add_spreading_link("user_intent", "text_input", 0.9)
ecan_system.add_spreading_link("context_memory", "user_intent", 0.6)
ecan_system.add_spreading_link("model_output", "context_memory", 0.5)