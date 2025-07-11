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
        
        # AtomSpace integration
        self.atomspace_patterns: Dict[str, List[str]] = {}  # element_id -> patterns
        self.pattern_attention: Dict[str, float] = {}  # pattern -> attention weight
        
        # Task scheduling integration
        self.task_attention_mapping: Dict[str, str] = {}  # task_id -> element_id
        self.attention_based_priorities: Dict[str, float] = {}  # task_id -> priority
        
        # Performance metrics
        self.allocation_metrics = {
            'total_cycles': 0,
            'average_cycle_time': 0.0,
            'attention_distribution_entropy': 0.0,
            'focus_stability': 0.0,
            'spreading_efficiency': 0.0
        }
        
    def register_cognitive_element(self, element_id: str, initial_attention: Optional[AttentionValue] = None):
        """Register a new cognitive element for attention tracking"""
        if initial_attention is None:
            initial_attention = AttentionValue()
        
        self.element_attention[element_id] = initial_attention
        logger.info(f"Registered cognitive element: {element_id}")
    
    def register_atomspace_pattern(self, element_id: str, pattern: str, attention_weight: float = 1.0):
        """Register an AtomSpace pattern associated with a cognitive element"""
        if element_id not in self.atomspace_patterns:
            self.atomspace_patterns[element_id] = []
        
        self.atomspace_patterns[element_id].append(pattern)
        self.pattern_attention[pattern] = attention_weight
        
        # Boost attention for elements with AtomSpace patterns
        if element_id in self.element_attention:
            self.element_attention[element_id].novelty = min(1.0, 
                self.element_attention[element_id].novelty + 0.1)
        
        logger.debug(f"Registered AtomSpace pattern for {element_id}: {pattern[:50]}...")
    
    def spread_to_atomspace_patterns(self, element_id: str) -> Dict[str, float]:
        """Spread activation to related AtomSpace patterns"""
        if element_id not in self.atomspace_patterns:
            return {}
        
        element_attention = self.element_attention.get(element_id)
        if not element_attention or element_attention.sti < self.spread_threshold:
            return {}
        
        spread_results = {}
        patterns = self.atomspace_patterns[element_id]
        
        # Calculate spread amount based on pattern relevance
        total_spread = element_attention.sti * self.max_spread_percentage
        pattern_weights = [self.pattern_attention.get(p, 1.0) for p in patterns]
        total_weight = sum(pattern_weights)
        
        if total_weight == 0:
            return {}
        
        for pattern, weight in zip(patterns, pattern_weights):
            spread_amount = (weight / total_weight) * total_spread
            
            # Find elements connected to this pattern
            connected_elements = []
            for elem_id, elem_patterns in self.atomspace_patterns.items():
                if elem_id != element_id and pattern in elem_patterns:
                    connected_elements.append(elem_id)
            
            # Spread to connected elements
            for connected_elem in connected_elements:
                if connected_elem in self.element_attention:
                    spread_per_element = spread_amount / len(connected_elements)
                    
                    # Transfer attention
                    self.element_attention[connected_elem].sti = min(1.0,
                        self.element_attention[connected_elem].sti + spread_per_element)
                    
                    spread_results[connected_elem] = spread_per_element
            
            # Reduce source attention
            element_attention.sti = max(0.0, element_attention.sti - spread_amount)
        
        return spread_results
    
    def get_pattern_activation_levels(self) -> Dict[str, float]:
        """Get activation levels for all registered AtomSpace patterns"""
        pattern_activations = {}
        
        for element_id, patterns in self.atomspace_patterns.items():
            if element_id in self.element_attention:
                element_attention = self.element_attention[element_id].get_composite_attention()
                
                for pattern in patterns:
                    pattern_weight = self.pattern_attention.get(pattern, 1.0)
                    activation = element_attention * pattern_weight
                    
                    if pattern in pattern_activations:
                        pattern_activations[pattern] = max(pattern_activations[pattern], activation)
                    else:
                        pattern_activations[pattern] = activation
        
        return pattern_activations
    
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
    
    def register_task_attention_mapping(self, task_id: str, element_id: str):
        """Register mapping between a task and cognitive element for attention-based scheduling"""
        self.task_attention_mapping[task_id] = element_id
        
        # Calculate initial attention-based priority
        if element_id in self.element_attention:
            priority = self.element_attention[element_id].get_composite_attention()
            self.attention_based_priorities[task_id] = priority * 10  # Scale to 0-10 range
        
        logger.debug(f"Registered task-attention mapping: {task_id} -> {element_id}")
    
    def get_task_attention_priority(self, task_id: str) -> float:
        """Get attention-based priority for a task"""
        if task_id not in self.task_attention_mapping:
            return 5.0  # Default priority
        
        element_id = self.task_attention_mapping[task_id]
        if element_id not in self.element_attention:
            return 5.0
        
        attention = self.element_attention[element_id].get_composite_attention()
        
        # Include urgency boost
        urgency_boost = self.element_attention[element_id].urgency * 2.0
        
        # Include novelty boost  
        novelty_boost = self.element_attention[element_id].novelty * 1.5
        
        # Calculate priority (0-10 scale)
        priority = (attention + urgency_boost + novelty_boost) * 10 / 3
        return min(10.0, max(1.0, priority))
    
    def update_task_attention_from_completion(self, task_id: str, success: bool, execution_time: float):
        """Update attention based on task completion results"""
        if task_id not in self.task_attention_mapping:
            return
        
        element_id = self.task_attention_mapping[task_id]
        if element_id not in self.element_attention:
            return
        
        attention = self.element_attention[element_id]
        
        if success:
            # Successful completion increases LTI and confidence
            attention.lti = min(1.0, attention.lti + 0.05)
            attention.confidence = min(1.0, attention.confidence + 0.1)
            
            # Fast completion increases STI
            if execution_time < 30.0:  # Fast completion
                attention.sti = min(1.0, attention.sti + 0.1)
        else:
            # Failure decreases confidence and STI
            attention.confidence = max(0.0, attention.confidence - 0.1)
            attention.sti = max(0.0, attention.sti - 0.05)
        
        # Update urgency based on execution time
        if execution_time > 60.0:  # Slow execution
            attention.urgency = min(1.0, attention.urgency + 0.1)
        else:
            attention.urgency = max(0.0, attention.urgency - 0.05)
        
        logger.debug(f"Updated attention for task {task_id} completion: success={success}, time={execution_time}s")
    
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
        cycle_start_time = time.time()
        self.allocation_round += 1
        
        # 1. Allocate STI budget
        sti_allocation = self.allocate_sti_budget()
        
        # 2. Allocate LTI budget
        lti_allocation = self.allocate_lti_budget()
        
        # 3. Spread activation within cognitive elements
        spread_results = {}
        for element_id in list(self.element_attention.keys()):
            if self.element_attention[element_id].sti > self.spread_threshold:
                spread_results[element_id] = self.spread_activation(element_id)
        
        # 4. Spread activation to AtomSpace patterns
        atomspace_spread_results = {}
        for element_id in list(self.element_attention.keys()):
            if self.element_attention[element_id].sti > self.spread_threshold:
                atomspace_spread_results[element_id] = self.spread_to_atomspace_patterns(element_id)
        
        # 5. Update task priorities based on current attention
        updated_task_priorities = {}
        for task_id in self.task_attention_mapping.keys():
            new_priority = self.get_task_attention_priority(task_id)
            old_priority = self.attention_based_priorities.get(task_id, 5.0)
            self.attention_based_priorities[task_id] = new_priority
            
            if abs(new_priority - old_priority) > 0.1:  # Significant change
                updated_task_priorities[task_id] = {
                    'old_priority': old_priority,
                    'new_priority': new_priority,
                    'change': new_priority - old_priority
                }
        
        # 6. Select attention foci
        selected_foci = self.select_attention_foci()
        
        # 7. Apply temporal decay
        self.apply_temporal_decay()
        
        # 8. Calculate performance metrics
        cycle_time = time.time() - cycle_start_time
        self._update_performance_metrics(cycle_time, sti_allocation, spread_results)
        
        # 9. Record allocation history
        allocation_record = {
            'round': self.allocation_round,
            'timestamp': time.time(),
            'cycle_time': cycle_time,
            'sti_allocation': sti_allocation,
            'lti_allocation': lti_allocation,
            'spread_results': spread_results,
            'atomspace_spread_results': atomspace_spread_results,
            'updated_task_priorities': updated_task_priorities,
            'selected_foci': selected_foci,
            'total_sti_allocated': self.current_sti_allocation,
            'active_elements': len(self.element_attention),
            'active_patterns': len(self.pattern_attention),
            'active_tasks': len(self.task_attention_mapping),
            'performance_metrics': self.allocation_metrics.copy()
        }
        
        self.resource_allocation_history.append(allocation_record)
        
        logger.debug(f"Completed attention cycle {self.allocation_round} in {cycle_time:.3f}s")
        return allocation_record
    
    def _update_performance_metrics(self, cycle_time: float, sti_allocation: Dict[str, float], spread_results: Dict[str, Dict[str, float]]):
        """Update performance metrics for attention allocation"""
        self.allocation_metrics['total_cycles'] += 1
        
        # Update average cycle time
        old_avg = self.allocation_metrics['average_cycle_time']
        total_cycles = self.allocation_metrics['total_cycles']
        self.allocation_metrics['average_cycle_time'] = (old_avg * (total_cycles - 1) + cycle_time) / total_cycles
        
        # Calculate attention distribution entropy
        if sti_allocation:
            total_allocation = sum(sti_allocation.values())
            if total_allocation > 0:
                probabilities = [amount / total_allocation for amount in sti_allocation.values()]
                entropy = -sum(p * math.log2(p) if p > 0 else 0 for p in probabilities)
                self.allocation_metrics['attention_distribution_entropy'] = entropy
        
        # Calculate focus stability (how consistent are the top attention elements)
        if len(self.resource_allocation_history) > 5:
            recent_allocations = list(self.resource_allocation_history)[-5:]
            
            # Get top 3 elements from each recent allocation
            top_elements_history = []
            for record in recent_allocations:
                if record['sti_allocation']:
                    top_3 = sorted(record['sti_allocation'].items(), key=lambda x: x[1], reverse=True)[:3]
                    top_elements_history.append(set(elem for elem, _ in top_3))
            
            # Calculate stability as intersection size
            if top_elements_history:
                intersection = set.intersection(*top_elements_history) if len(top_elements_history) > 1 else top_elements_history[0]
                stability = len(intersection) / 3.0  # Normalized by top-3 size
                self.allocation_metrics['focus_stability'] = stability
        
        # Calculate spreading efficiency
        total_spread_amount = 0
        total_spread_targets = 0
        for source_results in spread_results.values():
            total_spread_amount += sum(source_results.values())
            total_spread_targets += len(source_results)
        
        if total_spread_targets > 0:
            avg_spread_per_target = total_spread_amount / total_spread_targets
            self.allocation_metrics['spreading_efficiency'] = avg_spread_per_target
    
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
        
        # Get pattern activation statistics
        pattern_activations = self.get_pattern_activation_levels()
        top_patterns = sorted(pattern_activations.items(), key=lambda x: x[1], reverse=True)[:5]
        
        # Get task priority statistics
        task_priorities = list(self.attention_based_priorities.values())
        avg_task_priority = sum(task_priorities) / len(task_priorities) if task_priorities else 5.0
        
        return {
            'total_elements': len(self.element_attention),
            'total_foci': len(self.attention_foci),
            'total_patterns': len(self.pattern_attention),
            'total_tasks': len(self.task_attention_mapping),
            'total_sti': total_sti,
            'total_lti': total_lti,
            'average_sti': avg_sti,
            'average_lti': avg_lti,
            'average_task_priority': avg_task_priority,
            'allocation_rounds': self.allocation_round,
            'sti_budget_utilization': self.current_sti_allocation / self.total_sti_budget,
            'top_elements': [(elem_id, av.to_dict()) for elem_id, av in top_elements],
            'top_patterns': [(pattern[:50] + '...', activation) for pattern, activation in top_patterns],
            'active_foci': len([f for f in self.attention_foci.values() 
                              if f.attention_value.get_composite_attention() > 0.1]),
            'performance_metrics': self.allocation_metrics,
            'spreading_graph_size': len(self.spreading_activation_graph),
            'total_spreading_links': sum(len(targets) for targets in self.spreading_activation_graph.values())
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
    
    async def benchmark_attention_allocation(self, num_elements: int = 100, num_cycles: int = 50, 
                                           num_patterns: int = 200, num_tasks: int = 30) -> Dict[str, Any]:
        """Benchmark attention allocation performance across multiple agents and tasks"""
        logger.info(f"Starting attention allocation benchmark: {num_elements} elements, {num_cycles} cycles")
        
        benchmark_start = time.time()
        
        # Register benchmark elements
        benchmark_elements = []
        for i in range(num_elements):
            element_id = f"benchmark_element_{i}"
            attention = AttentionValue(
                sti=np.random.random() * 0.8 + 0.1,  # 0.1 to 0.9
                lti=np.random.random() * 0.6 + 0.2,  # 0.2 to 0.8
                urgency=np.random.random() * 0.5,     # 0.0 to 0.5
                novelty=np.random.random() * 0.3      # 0.0 to 0.3
            )
            self.register_cognitive_element(element_id, attention)
            benchmark_elements.append(element_id)
        
        # Register benchmark AtomSpace patterns
        pattern_templates = [
            "(ConceptNode \"Element_{i}\")",
            "(PredicateNode \"process_{i}\")",
            "(EvaluationLink (PredicateNode \"relates\") (ListLink (ConceptNode \"Element_{i}\") (ConceptNode \"Element_{j}\")))",
            "(ImplicationLink (ConceptNode \"Input_{i}\") (ConceptNode \"Output_{i}\"))"
        ]
        
        for i in range(num_patterns):
            element_id = benchmark_elements[i % len(benchmark_elements)]
            pattern = pattern_templates[i % len(pattern_templates)].format(i=i, j=(i+1) % num_elements)
            self.register_atomspace_pattern(element_id, pattern, np.random.random())
        
        # Register benchmark tasks
        for i in range(num_tasks):
            task_id = f"benchmark_task_{i}"
            element_id = benchmark_elements[i % len(benchmark_elements)]
            self.register_task_attention_mapping(task_id, element_id)
        
        # Add spreading links between elements
        for i in range(num_elements):
            source = benchmark_elements[i]
            for j in range(min(5, num_elements - 1)):  # Each element connects to up to 5 others
                target_idx = (i + j + 1) % num_elements
                target = benchmark_elements[target_idx]
                weight = np.random.random() * 0.8 + 0.2  # 0.2 to 1.0
                self.add_spreading_link(source, target, weight)
        
        # Run benchmark cycles
        cycle_times = []
        allocation_distributions = []
        
        for cycle in range(num_cycles):
            cycle_start = time.time()
            
            # Run attention cycle
            allocation_result = await self.run_attention_cycle()
            
            cycle_end = time.time()
            cycle_time = cycle_end - cycle_start
            cycle_times.append(cycle_time)
            
            # Track allocation distribution
            if allocation_result['sti_allocation']:
                allocations = list(allocation_result['sti_allocation'].values())
                allocation_distributions.append({
                    'mean': np.mean(allocations),
                    'std': np.std(allocations),
                    'min': np.min(allocations),
                    'max': np.max(allocations)
                })
            
            # Simulate some task completions
            if cycle % 10 == 0:  # Every 10 cycles
                for i in range(min(5, num_tasks)):
                    task_id = f"benchmark_task_{i}"
                    success = np.random.random() > 0.2  # 80% success rate
                    exec_time = np.random.exponential(30.0)  # Exponential distribution
                    self.update_task_attention_from_completion(task_id, success, exec_time)
        
        benchmark_end = time.time()
        total_benchmark_time = benchmark_end - benchmark_start
        
        # Calculate benchmark statistics
        avg_cycle_time = np.mean(cycle_times)
        cycle_time_std = np.std(cycle_times)
        min_cycle_time = np.min(cycle_times)
        max_cycle_time = np.max(cycle_times)
        
        # Calculate allocation fairness (Gini coefficient)
        final_attentions = [av.sti for av in self.element_attention.values()]
        gini_coefficient = self._calculate_gini_coefficient(final_attentions)
        
        # Calculate convergence metrics
        early_allocation = allocation_distributions[num_cycles // 4] if allocation_distributions else {}
        late_allocation = allocation_distributions[-1] if allocation_distributions else {}
        
        convergence_stability = 0.0
        if early_allocation and late_allocation:
            mean_change = abs(late_allocation['mean'] - early_allocation['mean'])
            std_change = abs(late_allocation['std'] - early_allocation['std'])
            convergence_stability = 1.0 / (1.0 + mean_change + std_change)
        
        benchmark_results = {
            'benchmark_config': {
                'num_elements': num_elements,
                'num_cycles': num_cycles,
                'num_patterns': num_patterns,
                'num_tasks': num_tasks
            },
            'timing_metrics': {
                'total_benchmark_time': total_benchmark_time,
                'average_cycle_time': avg_cycle_time,
                'cycle_time_std': cycle_time_std,
                'min_cycle_time': min_cycle_time,
                'max_cycle_time': max_cycle_time,
                'cycles_per_second': num_cycles / total_benchmark_time
            },
            'allocation_metrics': {
                'final_gini_coefficient': gini_coefficient,
                'convergence_stability': convergence_stability,
                'allocation_distributions': allocation_distributions[:5] + allocation_distributions[-5:],  # First and last 5
            },
            'system_metrics': self.get_attention_statistics(),
            'performance_history': list(self.resource_allocation_history)[-10:]  # Last 10 cycles
        }
        
        logger.info(f"Benchmark completed in {total_benchmark_time:.2f}s, avg cycle time: {avg_cycle_time:.4f}s")
        return benchmark_results
    
    def _calculate_gini_coefficient(self, values: List[float]) -> float:
        """Calculate Gini coefficient for attention distribution fairness"""
        if not values or len(values) < 2:
            return 0.0
        
        sorted_values = sorted(values)
        n = len(sorted_values)
        cumsum = np.cumsum(sorted_values)
        
        # Calculate Gini coefficient
        gini = (n + 1 - 2 * sum((n + 1 - i) * v for i, v in enumerate(sorted_values, 1))) / (n * sum(sorted_values))
        return max(0.0, min(1.0, gini))  # Ensure valid range


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