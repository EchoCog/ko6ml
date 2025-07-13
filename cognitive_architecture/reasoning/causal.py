"""
Causal Reasoning Network for Plot Development

This module provides causal reasoning capabilities for understanding
and generating plot development through cause-effect relationships.
"""

import logging
import time
from typing import Dict, List, Set, Any, Optional, Tuple
from dataclasses import dataclass, field
from enum import Enum
import re
import networkx as nx

logger = logging.getLogger(__name__)


class CausalStrength(Enum):
    """Strength of causal relationships"""
    WEAK = 0.3          # Weak causal influence
    MODERATE = 0.5      # Moderate causal influence  
    STRONG = 0.7        # Strong causal influence
    CERTAIN = 0.9       # Nearly certain causation
    ABSOLUTE = 1.0      # Absolute causation


class CausalType(Enum):
    """Types of causal relationships"""
    DIRECT = "direct"                    # A directly causes B
    INDIRECT = "indirect"                # A causes B through intermediate steps
    ENABLING = "enabling"                # A enables B to happen
    PREVENTING = "preventing"            # A prevents B from happening
    CATALYZING = "catalyzing"           # A accelerates B
    TRIGGERING = "triggering"           # A triggers B under certain conditions
    FACILITATING = "facilitating"       # A makes B easier/more likely
    INHIBITING = "inhibiting"           # A makes B harder/less likely


@dataclass
class CausalLink:
    """Represents a causal relationship between story elements"""
    link_id: str
    cause_id: str                       # ID of the causing element
    effect_id: str                      # ID of the effect element
    causal_type: CausalType
    strength: CausalStrength
    confidence: float = 0.8             # How confident we are in this link
    conditions: List[str] = field(default_factory=list)  # Conditions required for causation
    delay: Optional[float] = None        # Time delay between cause and effect
    evidence: List[str] = field(default_factory=list)    # Evidence supporting this link
    
    def __post_init__(self):
        """Validate the causal link"""
        if not 0 <= self.confidence <= 1:
            raise ValueError("Confidence must be between 0 and 1")


@dataclass
class PlotElement:
    """Represents a plot element that can participate in causal relationships"""
    element_id: str
    element_type: str                   # 'character', 'event', 'object', 'location', etc.
    description: str
    properties: Dict[str, Any] = field(default_factory=dict)
    state: Dict[str, Any] = field(default_factory=dict)
    influence_potential: float = 0.5    # How much this element can influence others
    susceptibility: float = 0.5         # How susceptible this element is to influence
    
    def __post_init__(self):
        """Validate plot element"""
        if not 0 <= self.influence_potential <= 1:
            raise ValueError("Influence potential must be between 0 and 1")
        if not 0 <= self.susceptibility <= 1:
            raise ValueError("Susceptibility must be between 0 and 1")


@dataclass
class CausalChain:
    """Represents a chain of causal relationships"""
    chain_id: str
    links: List[str]                    # List of causal link IDs in order
    total_strength: float = 0.0         # Combined strength of the chain
    probability: float = 0.0            # Probability this chain will occur
    narrative_impact: float = 0.0       # How much this chain affects the story


class CausalReasoningNetwork:
    """
    Causal reasoning network for plot development
    
    Models cause-effect relationships in stories to support plot generation,
    consistency checking, and narrative development.
    """
    
    def __init__(self):
        self.plot_elements: Dict[str, PlotElement] = {}
        self.causal_links: Dict[str, CausalLink] = {}
        self.causal_graph = nx.DiGraph()  # Directed graph for causal relationships
        self.causal_chains: Dict[str, CausalChain] = {}
        
        self.reasoning_stats = {
            'elements_processed': 0,
            'links_created': 0,
            'chains_discovered': 0,
            'inferences_made': 0,
            'start_time': time.time()
        }
        
        # Configuration
        self.max_chain_length = 10
        self.min_link_confidence = 0.3
        self.auto_discover_chains = True
        self.inference_threshold = 0.5
    
    def add_plot_element(self, element: PlotElement) -> bool:
        """Add a plot element to the network"""
        try:
            if element.element_id in self.plot_elements:
                logger.warning(f"Plot element {element.element_id} already exists, updating")
            
            self.plot_elements[element.element_id] = element
            
            # Add to causal graph as a node
            self.causal_graph.add_node(
                element.element_id,
                element_type=element.element_type,
                description=element.description,
                influence_potential=element.influence_potential,
                susceptibility=element.susceptibility
            )
            
            self.reasoning_stats['elements_processed'] += 1
            
            logger.info(f"Added plot element: {element.element_id} ({element.element_type})")
            return True
            
        except Exception as e:
            logger.error(f"Error adding plot element {element.element_id}: {e}")
            return False
    
    def add_causal_link(self, link: CausalLink) -> bool:
        """Add a causal link to the network"""
        try:
            # Verify both elements exist
            if link.cause_id not in self.plot_elements:
                logger.error(f"Cause element {link.cause_id} not found")
                return False
            
            if link.effect_id not in self.plot_elements:
                logger.error(f"Effect element {link.effect_id} not found")
                return False
            
            self.causal_links[link.link_id] = link
            
            # Add edge to causal graph
            self.causal_graph.add_edge(
                link.cause_id,
                link.effect_id,
                link_id=link.link_id,
                causal_type=link.causal_type.value,
                strength=link.strength.value,
                confidence=link.confidence,
                conditions=link.conditions,
                delay=link.delay
            )
            
            self.reasoning_stats['links_created'] += 1
            
            # Auto-discover new causal chains if enabled
            if self.auto_discover_chains:
                self._discover_causal_chains_from_link(link)
            
            logger.info(f"Added causal link: {link.link_id} ({link.cause_id} -> {link.effect_id})")
            return True
            
        except Exception as e:
            logger.error(f"Error adding causal link {link.link_id}: {e}")
            return False
    
    def analyze_plot_causality(self, story_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Analyze causal relationships in plot data
        
        Args:
            story_data: Dictionary containing story elements and relationships
        
        Returns:
            Analysis results including causal chains and plot development insights
        """
        try:
            # Extract plot elements from story data
            plot_elements = self._extract_plot_elements(story_data)
            
            # Add elements to the network
            for element in plot_elements:
                self.add_plot_element(element)
            
            # Infer causal links
            inferred_links = self._infer_causal_links(plot_elements, story_data)
            
            # Add inferred links
            for link in inferred_links:
                self.add_causal_link(link)
            
            # Discover causal chains
            major_chains = self._discover_major_causal_chains()
            
            # Analyze plot development patterns
            plot_patterns = self._analyze_plot_patterns()
            
            # Identify potential plot developments
            plot_predictions = self._predict_plot_developments()
            
            # Detect plot inconsistencies
            inconsistencies = self._detect_causal_inconsistencies()
            
            return {
                'causal_network_stats': {
                    'total_elements': len(self.plot_elements),
                    'total_links': len(self.causal_links),
                    'total_chains': len(self.causal_chains),
                    'network_density': self._calculate_network_density(),
                    'average_influence': self._calculate_average_influence()
                },
                'major_causal_chains': [self._chain_to_dict(chain) for chain in major_chains],
                'plot_patterns': plot_patterns,
                'plot_predictions': plot_predictions,
                'causal_inconsistencies': inconsistencies,
                'influence_analysis': self._analyze_element_influence(),
                'network_visualization': self._prepare_network_visualization()
            }
            
        except Exception as e:
            logger.error(f"Error analyzing plot causality: {e}")
            return {'error': str(e)}
    
    def _extract_plot_elements(self, story_data: Dict[str, Any]) -> List[PlotElement]:
        """Extract plot elements from story data"""
        elements = []
        
        # Extract characters
        if 'characters' in story_data:
            for char_data in story_data['characters']:
                if isinstance(char_data, dict):
                    element = PlotElement(
                        element_id=char_data.get('id', char_data.get('name', f"char_{len(elements)}")),
                        element_type='character',
                        description=char_data.get('description', char_data.get('name', 'Unknown character')),
                        properties=char_data.get('properties', {}),
                        state=char_data.get('state', {}),
                        influence_potential=self._estimate_character_influence(char_data),
                        susceptibility=char_data.get('susceptibility', 0.5)
                    )
                    elements.append(element)
                elif isinstance(char_data, str):
                    element = PlotElement(
                        element_id=char_data,
                        element_type='character',
                        description=char_data,
                        influence_potential=0.5,
                        susceptibility=0.5
                    )
                    elements.append(element)
        
        # Extract events
        if 'events' in story_data:
            for event_data in story_data['events']:
                if isinstance(event_data, dict):
                    element = PlotElement(
                        element_id=event_data.get('id', f"event_{len(elements)}"),
                        element_type='event',
                        description=event_data.get('description', event_data.get('name', 'Unknown event')),
                        properties=event_data.get('properties', {}),
                        state={'occurred': event_data.get('occurred', False)},
                        influence_potential=self._estimate_event_influence(event_data),
                        susceptibility=event_data.get('susceptibility', 0.3)
                    )
                    elements.append(element)
        
        # Extract objects/items
        if 'objects' in story_data:
            for obj_data in story_data['objects']:
                if isinstance(obj_data, dict):
                    element = PlotElement(
                        element_id=obj_data.get('id', obj_data.get('name', f"obj_{len(elements)}")),
                        element_type='object',
                        description=obj_data.get('description', obj_data.get('name', 'Unknown object')),
                        properties=obj_data.get('properties', {}),
                        influence_potential=self._estimate_object_influence(obj_data),
                        susceptibility=obj_data.get('susceptibility', 0.4)
                    )
                    elements.append(element)
        
        # Extract locations
        if 'locations' in story_data:
            for loc_data in story_data['locations']:
                if isinstance(loc_data, dict):
                    element = PlotElement(
                        element_id=loc_data.get('id', loc_data.get('name', f"loc_{len(elements)}")),
                        element_type='location',
                        description=loc_data.get('description', loc_data.get('name', 'Unknown location')),
                        properties=loc_data.get('properties', {}),
                        influence_potential=self._estimate_location_influence(loc_data),
                        susceptibility=loc_data.get('susceptibility', 0.2)
                    )
                    elements.append(element)
        
        return elements
    
    def _estimate_character_influence(self, char_data: Dict[str, Any]) -> float:
        """Estimate a character's influence potential"""
        influence = 0.5  # Base influence
        
        # Adjust based on role/importance
        role = char_data.get('role', '').lower()
        if 'protagonist' in role or 'main' in role:
            influence += 0.3
        elif 'antagonist' in role or 'villain' in role:
            influence += 0.25
        elif 'supporting' in role:
            influence += 0.1
        
        # Adjust based on traits
        traits = char_data.get('traits', [])
        if isinstance(traits, list):
            powerful_traits = ['powerful', 'influential', 'charismatic', 'leader', 'magical']
            for trait in traits:
                if any(pt in trait.lower() for pt in powerful_traits):
                    influence += 0.1
        
        # Adjust based on resources/abilities
        if 'abilities' in char_data or 'powers' in char_data:
            influence += 0.15
        
        return min(1.0, influence)
    
    def _estimate_event_influence(self, event_data: Dict[str, Any]) -> float:
        """Estimate an event's influence potential"""
        influence = 0.4  # Base influence for events
        
        # Adjust based on event type/importance
        event_type = event_data.get('type', '').lower()
        major_types = ['climax', 'turning point', 'revelation', 'conflict', 'death', 'birth']
        if any(mt in event_type for mt in major_types):
            influence += 0.4
        
        # Adjust based on description keywords
        description = event_data.get('description', '').lower()
        impactful_words = ['changes', 'transforms', 'destroys', 'creates', 'reveals', 'decides']
        for word in impactful_words:
            if word in description:
                influence += 0.1
                break
        
        return min(1.0, influence)
    
    def _estimate_object_influence(self, obj_data: Dict[str, Any]) -> float:
        """Estimate an object's influence potential"""
        influence = 0.3  # Base influence for objects
        
        # Adjust based on object type
        obj_type = obj_data.get('type', '').lower()
        powerful_types = ['weapon', 'artifact', 'treasure', 'key', 'magical', 'ancient']
        if any(pt in obj_type for pt in powerful_types):
            influence += 0.3
        
        # Adjust based on properties
        properties = obj_data.get('properties', {})
        if 'magical' in properties or 'powerful' in properties:
            influence += 0.2
        
        return min(1.0, influence)
    
    def _estimate_location_influence(self, loc_data: Dict[str, Any]) -> float:
        """Estimate a location's influence potential"""
        influence = 0.2  # Base influence for locations
        
        # Adjust based on location type
        loc_type = loc_data.get('type', '').lower()
        significant_types = ['castle', 'temple', 'battlefield', 'capital', 'stronghold', 'sanctuary']
        if any(st in loc_type for st in significant_types):
            influence += 0.3
        
        # Adjust based on strategic importance
        if 'strategic' in str(loc_data.get('properties', {})).lower():
            influence += 0.2
        
        return min(1.0, influence)
    
    def _infer_causal_links(self, elements: List[PlotElement], story_data: Dict[str, Any]) -> List[CausalLink]:
        """Infer causal links between plot elements"""
        links = []
        
        # Infer character-event causality
        characters = [e for e in elements if e.element_type == 'character']
        events = [e for e in elements if e.element_type == 'event']
        
        for char in characters:
            for event in events:
                # Check if character likely causes event
                if self._character_likely_causes_event(char, event, story_data):
                    link = CausalLink(
                        link_id=f"char_event_{char.element_id}_{event.element_id}",
                        cause_id=char.element_id,
                        effect_id=event.element_id,
                        causal_type=CausalType.DIRECT,
                        strength=CausalStrength.MODERATE,
                        confidence=0.7
                    )
                    links.append(link)
        
        # Infer event-event causality
        for i, event1 in enumerate(events):
            for event2 in events[i+1:]:
                if self._events_causally_related(event1, event2, story_data):
                    link = CausalLink(
                        link_id=f"event_event_{event1.element_id}_{event2.element_id}",
                        cause_id=event1.element_id,
                        effect_id=event2.element_id,
                        causal_type=CausalType.TRIGGERING,
                        strength=CausalStrength.MODERATE,
                        confidence=0.6
                    )
                    links.append(link)
        
        # Infer object influence
        objects = [e for e in elements if e.element_type == 'object']
        for obj in objects:
            for char in characters:
                if self._object_influences_character(obj, char, story_data):
                    link = CausalLink(
                        link_id=f"obj_char_{obj.element_id}_{char.element_id}",
                        cause_id=obj.element_id,
                        effect_id=char.element_id,
                        causal_type=CausalType.ENABLING,
                        strength=CausalStrength.WEAK,
                        confidence=0.5
                    )
                    links.append(link)
        
        return links
    
    def _character_likely_causes_event(self, char: PlotElement, event: PlotElement, story_data: Dict[str, Any]) -> bool:
        """Check if a character likely causes an event"""
        # Look for linguistic patterns suggesting causation
        char_name = char.description.lower()
        event_desc = event.description.lower()
        
        # Check if character is mentioned in event description
        if char_name in event_desc:
            # Look for action words near character name
            action_words = ['performs', 'causes', 'triggers', 'initiates', 'starts', 'begins', 'creates']
            for word in action_words:
                if word in event_desc:
                    return True
        
        # Check for high influence characters
        if char.influence_potential > 0.7:
            return True
        
        return False
    
    def _events_causally_related(self, event1: PlotElement, event2: PlotElement, story_data: Dict[str, Any]) -> bool:
        """Check if two events are causally related"""
        desc1 = event1.description.lower()
        desc2 = event2.description.lower()
        
        # Look for causal language patterns
        causal_patterns = [
            ('leads to', 'results in'),
            ('causes', 'because'),
            ('triggers', 'following'),
            ('enables', 'allowing'),
            ('forces', 'compelling')
        ]
        
        for pattern1, pattern2 in causal_patterns:
            if pattern1 in desc1 and pattern2 in desc2:
                return True
            if pattern2 in desc1 and pattern1 in desc2:
                return True
        
        return False
    
    def _object_influences_character(self, obj: PlotElement, char: PlotElement, story_data: Dict[str, Any]) -> bool:
        """Check if an object influences a character"""
        obj_desc = obj.description.lower()
        char_desc = char.description.lower()
        
        # Objects with high influence potential
        if obj.influence_potential > 0.6:
            return True
        
        # Look for possession/interaction patterns
        possession_words = ['has', 'owns', 'wields', 'carries', 'possesses', 'uses']
        for word in possession_words:
            if word in char_desc and obj.element_id.lower() in char_desc:
                return True
        
        return False
    
    def _discover_causal_chains_from_link(self, new_link: CausalLink):
        """Discover new causal chains when a link is added"""
        try:
            # Find chains that end with the new link's cause
            incoming_chains = []
            for chain in self.causal_chains.values():
                if chain.links and chain.links[-1] in self.causal_links:
                    last_link = self.causal_links[chain.links[-1]]
                    if last_link.effect_id == new_link.cause_id:
                        incoming_chains.append(chain)
            
            # Create extended chains
            for incoming_chain in incoming_chains:
                if len(incoming_chain.links) < self.max_chain_length:
                    new_chain = CausalChain(
                        chain_id=f"chain_{incoming_chain.chain_id}_{new_link.link_id}",
                        links=incoming_chain.links + [new_link.link_id],
                        total_strength=self._calculate_chain_strength(incoming_chain.links + [new_link.link_id]),
                        probability=self._calculate_chain_probability(incoming_chain.links + [new_link.link_id])
                    )
                    self.causal_chains[new_chain.chain_id] = new_chain
                    self.reasoning_stats['chains_discovered'] += 1
            
            # Create a simple chain from just this link
            simple_chain = CausalChain(
                chain_id=f"simple_{new_link.link_id}",
                links=[new_link.link_id],
                total_strength=new_link.strength.value,
                probability=new_link.confidence
            )
            self.causal_chains[simple_chain.chain_id] = simple_chain
            
        except Exception as e:
            logger.error(f"Error discovering causal chains from link {new_link.link_id}: {e}")
    
    def _discover_major_causal_chains(self) -> List[CausalChain]:
        """Discover major causal chains in the network"""
        # Find all paths in the causal graph
        major_chains = []
        
        try:
            # Find strongly connected components
            for component in nx.strongly_connected_components(self.causal_graph):
                if len(component) > 1:
                    # Create chains within components
                    subgraph = self.causal_graph.subgraph(component)
                    for path in nx.all_simple_paths(subgraph, 
                                                   source=list(component)[0],
                                                   target=list(component)[-1],
                                                   cutoff=self.max_chain_length):
                        if len(path) > 2:  # Only consider chains with 3+ elements
                            chain_links = self._path_to_links(path)
                            if chain_links:
                                chain = CausalChain(
                                    chain_id=f"major_{'_'.join(path[:3])}",
                                    links=chain_links,
                                    total_strength=self._calculate_chain_strength(chain_links),
                                    probability=self._calculate_chain_probability(chain_links),
                                    narrative_impact=self._calculate_narrative_impact(chain_links)
                                )
                                major_chains.append(chain)
            
            # Sort by narrative impact
            major_chains.sort(key=lambda c: c.narrative_impact, reverse=True)
            
        except Exception as e:
            logger.error(f"Error discovering major causal chains: {e}")
        
        return major_chains[:10]  # Return top 10 chains
    
    def _path_to_links(self, path: List[str]) -> List[str]:
        """Convert a path of nodes to a list of link IDs"""
        links = []
        
        for i in range(len(path) - 1):
            # Find the link between consecutive nodes
            if self.causal_graph.has_edge(path[i], path[i+1]):
                edge_data = self.causal_graph[path[i]][path[i+1]]
                link_id = edge_data.get('link_id')
                if link_id:
                    links.append(link_id)
        
        return links
    
    def _calculate_chain_strength(self, link_ids: List[str]) -> float:
        """Calculate the total strength of a causal chain"""
        if not link_ids:
            return 0.0
        
        # Multiply strengths (each link weakens the chain)
        total_strength = 1.0
        for link_id in link_ids:
            if link_id in self.causal_links:
                link = self.causal_links[link_id]
                total_strength *= link.strength.value
        
        return total_strength
    
    def _calculate_chain_probability(self, link_ids: List[str]) -> float:
        """Calculate the probability of a causal chain occurring"""
        if not link_ids:
            return 0.0
        
        # Multiply confidence values
        total_probability = 1.0
        for link_id in link_ids:
            if link_id in self.causal_links:
                link = self.causal_links[link_id]
                total_probability *= link.confidence
        
        return total_probability
    
    def _calculate_narrative_impact(self, link_ids: List[str]) -> float:
        """Calculate the narrative impact of a causal chain"""
        if not link_ids:
            return 0.0
        
        impact = 0.0
        
        # Factor in the number of elements involved
        involved_elements = set()
        for link_id in link_ids:
            if link_id in self.causal_links:
                link = self.causal_links[link_id]
                involved_elements.add(link.cause_id)
                involved_elements.add(link.effect_id)
        
        # More elements = higher potential impact
        impact += len(involved_elements) * 0.1
        
        # Factor in the influence potential of involved elements
        for element_id in involved_elements:
            if element_id in self.plot_elements:
                element = self.plot_elements[element_id]
                impact += element.influence_potential * 0.2
        
        # Factor in chain strength and probability
        chain_strength = self._calculate_chain_strength(link_ids)
        chain_probability = self._calculate_chain_probability(link_ids)
        impact += (chain_strength * chain_probability) * 0.5
        
        return min(1.0, impact)
    
    def _analyze_plot_patterns(self) -> Dict[str, Any]:
        """Analyze patterns in the causal network"""
        patterns = {
            'dominant_causal_types': {},
            'most_influential_elements': [],
            'causal_density': 0,
            'network_centrality': {},
            'plot_complexity': 0
        }
        
        # Analyze causal types
        for link in self.causal_links.values():
            causal_type = link.causal_type.value
            patterns['dominant_causal_types'][causal_type] = patterns['dominant_causal_types'].get(causal_type, 0) + 1
        
        # Find most influential elements
        influence_scores = {}
        for element_id, element in self.plot_elements.items():
            # Count outgoing causal links
            outgoing_links = sum(1 for link in self.causal_links.values() if link.cause_id == element_id)
            # Weight by influence potential
            influence_score = outgoing_links * element.influence_potential
            influence_scores[element_id] = influence_score
        
        # Sort by influence
        sorted_influence = sorted(influence_scores.items(), key=lambda x: x[1], reverse=True)
        patterns['most_influential_elements'] = [
            {
                'element_id': elem_id,
                'influence_score': score,
                'element_type': self.plot_elements[elem_id].element_type,
                'description': self.plot_elements[elem_id].description
            }
            for elem_id, score in sorted_influence[:5]
        ]
        
        # Calculate network metrics
        if self.causal_graph.number_of_nodes() > 0:
            patterns['causal_density'] = nx.density(self.causal_graph)
            
            if self.causal_graph.number_of_nodes() > 1:
                centrality = nx.degree_centrality(self.causal_graph)
                patterns['network_centrality'] = dict(list(centrality.items())[:5])
        
        # Calculate plot complexity
        patterns['plot_complexity'] = self._calculate_plot_complexity()
        
        return patterns
    
    def _calculate_plot_complexity(self) -> float:
        """Calculate the complexity of the plot based on causal structure"""
        if not self.plot_elements:
            return 0.0
        
        complexity = 0.0
        
        # Factor in number of elements and links
        complexity += len(self.plot_elements) * 0.1
        complexity += len(self.causal_links) * 0.15
        
        # Factor in network density
        if self.causal_graph.number_of_nodes() > 1:
            density = nx.density(self.causal_graph)
            complexity += density * 0.3
        
        # Factor in number of causal chains
        complexity += len(self.causal_chains) * 0.05
        
        # Factor in variety of causal types
        causal_types = set(link.causal_type for link in self.causal_links.values())
        complexity += len(causal_types) * 0.1
        
        return min(1.0, complexity)
    
    def _predict_plot_developments(self) -> List[Dict[str, Any]]:
        """Predict potential plot developments based on causal analysis"""
        predictions = []
        
        try:
            # Look for elements with high influence but low current effect
            for element_id, element in self.plot_elements.items():
                if element.influence_potential > 0.7:
                    # Count current outgoing links
                    outgoing_count = sum(1 for link in self.causal_links.values() if link.cause_id == element_id)
                    
                    if outgoing_count < 2:  # High potential, low current impact
                        predictions.append({
                            'type': 'untapped_potential',
                            'element_id': element_id,
                            'element_type': element.element_type,
                            'description': f"{element.description} has high influence potential but limited current impact",
                            'prediction': f"Expect {element.description} to play a larger role in upcoming events",
                            'confidence': element.influence_potential * 0.8
                        })
            
            # Look for potential chain completions
            for chain in self.causal_chains.values():
                if len(chain.links) >= 2 and chain.probability > 0.6:
                    last_link = self.causal_links[chain.links[-1]]
                    effect_element = self.plot_elements[last_link.effect_id]
                    
                    # Look for potential next steps
                    potential_targets = [e for e in self.plot_elements.values() 
                                       if e.susceptibility > 0.6 and e.element_id != effect_element.element_id]
                    
                    if potential_targets:
                        target = max(potential_targets, key=lambda e: e.susceptibility)
                        predictions.append({
                            'type': 'chain_extension',
                            'current_chain': chain.chain_id,
                            'prediction': f"Causal chain may extend from {effect_element.description} to {target.description}",
                            'confidence': chain.probability * target.susceptibility * 0.7
                        })
            
            # Sort predictions by confidence
            predictions.sort(key=lambda p: p.get('confidence', 0), reverse=True)
            
        except Exception as e:
            logger.error(f"Error predicting plot developments: {e}")
        
        return predictions[:5]  # Return top 5 predictions
    
    def _detect_causal_inconsistencies(self) -> List[Dict[str, Any]]:
        """Detect inconsistencies in the causal network"""
        inconsistencies = []
        
        try:
            # Look for circular causality (should be rare but worth checking)
            cycles = list(nx.simple_cycles(self.causal_graph))
            for cycle in cycles:
                inconsistencies.append({
                    'type': 'circular_causality',
                    'elements': cycle,
                    'description': f"Circular causality detected: {' -> '.join(cycle)} -> {cycle[0]}",
                    'severity': 0.8
                })
            
            # Look for conflicting causal relationships
            for element_id in self.plot_elements:
                incoming_links = [link for link in self.causal_links.values() if link.effect_id == element_id]
                
                # Check for preventing vs enabling conflicts
                preventing = [link for link in incoming_links if link.causal_type == CausalType.PREVENTING]
                enabling = [link for link in incoming_links if link.causal_type == CausalType.ENABLING]
                
                if preventing and enabling:
                    inconsistencies.append({
                        'type': 'conflicting_causality',
                        'element_id': element_id,
                        'description': f"Element {element_id} has both preventing and enabling causal influences",
                        'severity': 0.6,
                        'preventing_causes': [link.cause_id for link in preventing],
                        'enabling_causes': [link.cause_id for link in enabling]
                    })
            
        except Exception as e:
            logger.error(f"Error detecting causal inconsistencies: {e}")
        
        return inconsistencies
    
    def _analyze_element_influence(self) -> Dict[str, Any]:
        """Analyze the influence of different plot elements"""
        analysis = {
            'top_influencers': [],
            'influence_distribution': {},
            'causal_hubs': [],
            'isolated_elements': []
        }
        
        # Calculate influence metrics for each element
        element_metrics = {}
        
        for element_id, element in self.plot_elements.items():
            outgoing_links = [link for link in self.causal_links.values() if link.cause_id == element_id]
            incoming_links = [link for link in self.causal_links.values() if link.effect_id == element_id]
            
            metrics = {
                'outgoing_count': len(outgoing_links),
                'incoming_count': len(incoming_links),
                'total_connections': len(outgoing_links) + len(incoming_links),
                'influence_potential': element.influence_potential,
                'susceptibility': element.susceptibility,
                'net_influence': len(outgoing_links) - len(incoming_links)
            }
            
            element_metrics[element_id] = metrics
        
        # Find top influencers
        top_influencers = sorted(
            element_metrics.items(),
            key=lambda x: x[1]['outgoing_count'] * x[1]['influence_potential'],
            reverse=True
        )[:5]
        
        analysis['top_influencers'] = [
            {
                'element_id': elem_id,
                'element_type': self.plot_elements[elem_id].element_type,
                'description': self.plot_elements[elem_id].description,
                'influence_score': metrics['outgoing_count'] * metrics['influence_potential'],
                'outgoing_links': metrics['outgoing_count']
            }
            for elem_id, metrics in top_influencers
        ]
        
        # Analyze influence distribution by element type
        for element_id, element in self.plot_elements.items():
            elem_type = element.element_type
            if elem_type not in analysis['influence_distribution']:
                analysis['influence_distribution'][elem_type] = {
                    'count': 0,
                    'total_influence': 0,
                    'avg_influence': 0
                }
            
            analysis['influence_distribution'][elem_type]['count'] += 1
            analysis['influence_distribution'][elem_type]['total_influence'] += element.influence_potential
        
        # Calculate averages
        for elem_type, data in analysis['influence_distribution'].items():
            data['avg_influence'] = data['total_influence'] / data['count'] if data['count'] > 0 else 0
        
        # Find causal hubs (elements with many connections)
        causal_hubs = [
            elem_id for elem_id, metrics in element_metrics.items()
            if metrics['total_connections'] >= 3
        ]
        
        analysis['causal_hubs'] = [
            {
                'element_id': elem_id,
                'element_type': self.plot_elements[elem_id].element_type,
                'description': self.plot_elements[elem_id].description,
                'total_connections': element_metrics[elem_id]['total_connections']
            }
            for elem_id in causal_hubs
        ]
        
        # Find isolated elements
        isolated = [
            elem_id for elem_id, metrics in element_metrics.items()
            if metrics['total_connections'] == 0
        ]
        
        analysis['isolated_elements'] = [
            {
                'element_id': elem_id,
                'element_type': self.plot_elements[elem_id].element_type,
                'description': self.plot_elements[elem_id].description
            }
            for elem_id in isolated
        ]
        
        return analysis
    
    def _prepare_network_visualization(self) -> Dict[str, Any]:
        """Prepare data for network visualization"""
        nodes = []
        edges = []
        
        # Prepare nodes
        for element_id, element in self.plot_elements.items():
            nodes.append({
                'id': element_id,
                'label': element.description[:20] + ('...' if len(element.description) > 20 else ''),
                'type': element.element_type,
                'influence': element.influence_potential,
                'susceptibility': element.susceptibility
            })
        
        # Prepare edges
        for link_id, link in self.causal_links.items():
            edges.append({
                'id': link_id,
                'source': link.cause_id,
                'target': link.effect_id,
                'type': link.causal_type.value,
                'strength': link.strength.value,
                'confidence': link.confidence
            })
        
        return {
            'nodes': nodes,
            'edges': edges,
            'network_stats': {
                'node_count': len(nodes),
                'edge_count': len(edges),
                'density': self._calculate_network_density()
            }
        }
    
    def _calculate_network_density(self) -> float:
        """Calculate the density of the causal network"""
        if self.causal_graph.number_of_nodes() <= 1:
            return 0.0
        
        return nx.density(self.causal_graph)
    
    def _calculate_average_influence(self) -> float:
        """Calculate average influence potential across all elements"""
        if not self.plot_elements:
            return 0.0
        
        total_influence = sum(element.influence_potential for element in self.plot_elements.values())
        return total_influence / len(self.plot_elements)
    
    def _chain_to_dict(self, chain: CausalChain) -> Dict[str, Any]:
        """Convert a causal chain to dictionary format"""
        # Get the elements involved in the chain
        elements = []
        for link_id in chain.links:
            if link_id in self.causal_links:
                link = self.causal_links[link_id]
                if not elements or elements[-1]['element_id'] != link.cause_id:
                    if link.cause_id in self.plot_elements:
                        cause_elem = self.plot_elements[link.cause_id]
                        elements.append({
                            'element_id': link.cause_id,
                            'description': cause_elem.description,
                            'type': cause_elem.element_type
                        })
                
                if link.effect_id in self.plot_elements:
                    effect_elem = self.plot_elements[link.effect_id]
                    elements.append({
                        'element_id': link.effect_id,
                        'description': effect_elem.description,
                        'type': effect_elem.element_type
                    })
        
        return {
            'chain_id': chain.chain_id,
            'elements': elements,
            'link_count': len(chain.links),
            'total_strength': chain.total_strength,
            'probability': chain.probability,
            'narrative_impact': chain.narrative_impact
        }
    
    def get_causal_statistics(self) -> Dict[str, Any]:
        """Get statistics about causal reasoning performance"""
        uptime = time.time() - self.reasoning_stats['start_time']
        
        return {
            'elements_processed': self.reasoning_stats['elements_processed'],
            'links_created': self.reasoning_stats['links_created'],
            'chains_discovered': self.reasoning_stats['chains_discovered'],
            'inferences_made': self.reasoning_stats['inferences_made'],
            'current_elements': len(self.plot_elements),
            'current_links': len(self.causal_links),
            'current_chains': len(self.causal_chains),
            'network_density': self._calculate_network_density(),
            'average_influence': self._calculate_average_influence(),
            'plot_complexity': self._calculate_plot_complexity(),
            'uptime_seconds': uptime
        }
    
    def clear_causal_network(self):
        """Clear all causal network data"""
        self.plot_elements.clear()
        self.causal_links.clear()
        self.causal_graph.clear()
        self.causal_chains.clear()
        logger.info("Causal network cleared")
    
    def export_causal_network(self) -> Dict[str, Any]:
        """Export complete causal network data"""
        return {
            'plot_elements': {
                elem_id: {
                    'element_type': elem.element_type,
                    'description': elem.description,
                    'properties': elem.properties,
                    'state': elem.state,
                    'influence_potential': elem.influence_potential,
                    'susceptibility': elem.susceptibility
                }
                for elem_id, elem in self.plot_elements.items()
            },
            'causal_links': {
                link_id: {
                    'cause_id': link.cause_id,
                    'effect_id': link.effect_id,
                    'causal_type': link.causal_type.value,
                    'strength': link.strength.value,
                    'confidence': link.confidence,
                    'conditions': link.conditions,
                    'delay': link.delay,
                    'evidence': link.evidence
                }
                for link_id, link in self.causal_links.items()
            },
            'causal_chains': {
                chain_id: self._chain_to_dict(chain)
                for chain_id, chain in self.causal_chains.items()
            },
            'network_statistics': self.get_causal_statistics()
        }