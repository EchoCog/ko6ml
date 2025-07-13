"""
Multi-Modal Processor for Advanced Cognition

This module provides multi-modal processing capabilities for handling
different types of data (text, structured data, metadata) in a unified
cognitive framework.
"""

import logging
import time
import json
import re
from typing import Dict, List, Set, Any, Optional, Tuple, Union
from dataclasses import dataclass, field
from enum import Enum

logger = logging.getLogger(__name__)


class ModalityType(Enum):
    """Types of data modalities"""
    TEXT = "text"                       # Raw text content
    STRUCTURED = "structured"           # JSON, XML, tables, etc.
    METADATA = "metadata"               # Tags, attributes, classifications
    TEMPORAL = "temporal"               # Time-based information
    SPATIAL = "spatial"                 # Location, spatial relationships
    NUMERICAL = "numerical"             # Numbers, statistics, measurements
    CATEGORICAL = "categorical"         # Categories, classifications
    RELATIONAL = "relational"           # Relationships between entities
    SEMANTIC = "semantic"               # Semantic annotations, concepts


@dataclass
class ModalData:
    """Container for multi-modal data"""
    data_id: str
    modality: ModalityType
    content: Any                        # The actual data content
    confidence: float = 1.0             # Confidence in this data
    source: Optional[str] = None        # Source of the data
    timestamp: Optional[float] = None    # When this data was created
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def __post_init__(self):
        """Set timestamp if not provided"""
        if self.timestamp is None:
            self.timestamp = time.time()


@dataclass
class ProcessingResult:
    """Result of multi-modal processing"""
    unified_representation: Dict[str, Any]  # Unified representation across modalities
    modality_analyses: Dict[str, Any]       # Analysis for each modality
    cross_modal_connections: List[Dict[str, Any]]  # Connections between modalities
    confidence_scores: Dict[str, float]     # Confidence for each analysis
    processing_time: float = 0.0
    patterns_discovered: List[str] = field(default_factory=list)
    
    def get_overall_confidence(self) -> float:
        """Calculate overall confidence across all modalities"""
        if not self.confidence_scores:
            return 0.0
        return sum(self.confidence_scores.values()) / len(self.confidence_scores)


class MultiModalProcessor:
    """
    Multi-modal processor for advanced cognitive processing
    
    Handles different types of data in a unified framework, allowing
    for cross-modal analysis and integration.
    """
    
    def __init__(self):
        self.modality_handlers: Dict[ModalityType, callable] = {}
        self.cross_modal_patterns: List[Dict[str, Any]] = []
        self.unified_knowledge: Dict[str, Any] = {}
        
        self.processing_stats = {
            'total_items_processed': 0,
            'modalities_processed': {},
            'cross_modal_connections': 0,
            'patterns_discovered': 0,
            'start_time': time.time()
        }
        
        # Configuration
        self.enable_cross_modal_analysis = True
        self.confidence_threshold = 0.5
        self.max_connections_per_item = 10
        
        # Initialize default handlers
        self._initialize_modality_handlers()
    
    def _initialize_modality_handlers(self):
        """Initialize handlers for different modalities"""
        self.modality_handlers = {
            ModalityType.TEXT: self._process_text_modality,
            ModalityType.STRUCTURED: self._process_structured_modality,
            ModalityType.METADATA: self._process_metadata_modality,
            ModalityType.TEMPORAL: self._process_temporal_modality,
            ModalityType.SPATIAL: self._process_spatial_modality,
            ModalityType.NUMERICAL: self._process_numerical_modality,
            ModalityType.CATEGORICAL: self._process_categorical_modality,
            ModalityType.RELATIONAL: self._process_relational_modality,
            ModalityType.SEMANTIC: self._process_semantic_modality
        }
        
        logger.info(f"Initialized multi-modal processor with {len(self.modality_handlers)} modality handlers")
    
    def process_multi_modal_data(self, modal_data_list: List[ModalData]) -> ProcessingResult:
        """
        Process multiple modalities of data together
        
        Args:
            modal_data_list: List of data in different modalities
        
        Returns:
            ProcessingResult with unified analysis
        """
        start_time = time.time()
        
        try:
            # Process each modality individually
            modality_analyses = {}
            confidence_scores = {}
            all_patterns = []
            
            for modal_data in modal_data_list:
                if modal_data.modality in self.modality_handlers:
                    handler = self.modality_handlers[modal_data.modality]
                    analysis = handler(modal_data)
                    
                    modality_analyses[modal_data.modality.value] = analysis
                    confidence_scores[modal_data.modality.value] = modal_data.confidence
                    
                    if 'patterns' in analysis:
                        all_patterns.extend(analysis['patterns'])
                    
                    # Update processing stats
                    modality_type = modal_data.modality.value
                    self.processing_stats['modalities_processed'][modality_type] = \
                        self.processing_stats['modalities_processed'].get(modality_type, 0) + 1
                else:
                    logger.warning(f"No handler for modality: {modal_data.modality}")
            
            # Create unified representation
            unified_representation = self._create_unified_representation(modal_data_list, modality_analyses)
            
            # Find cross-modal connections
            cross_modal_connections = []
            if self.enable_cross_modal_analysis and len(modal_data_list) > 1:
                cross_modal_connections = self._find_cross_modal_connections(modal_data_list, modality_analyses)
                self.processing_stats['cross_modal_connections'] += len(cross_modal_connections)
            
            # Discover new patterns
            discovered_patterns = self._discover_multi_modal_patterns(modal_data_list, modality_analyses)
            all_patterns.extend(discovered_patterns)
            self.processing_stats['patterns_discovered'] += len(discovered_patterns)
            
            # Update total processing count
            self.processing_stats['total_items_processed'] += len(modal_data_list)
            
            processing_time = time.time() - start_time
            
            result = ProcessingResult(
                unified_representation=unified_representation,
                modality_analyses=modality_analyses,
                cross_modal_connections=cross_modal_connections,
                confidence_scores=confidence_scores,
                processing_time=processing_time,
                patterns_discovered=all_patterns
            )
            
            logger.info(f"Processed {len(modal_data_list)} modalities in {processing_time:.3f}s, "
                       f"found {len(cross_modal_connections)} cross-modal connections")
            
            return result
            
        except Exception as e:
            logger.error(f"Error in multi-modal processing: {e}")
            return ProcessingResult(
                unified_representation={'error': str(e)},
                modality_analyses={},
                cross_modal_connections=[],
                confidence_scores={}
            )
    
    def _process_text_modality(self, modal_data: ModalData) -> Dict[str, Any]:
        """Process text modality data"""
        try:
            text = str(modal_data.content)
            
            analysis = {
                'modality': 'text',
                'length': len(text),
                'word_count': len(text.split()),
                'sentences': len(re.findall(r'[.!?]+', text)),
                'patterns': [],
                'entities': [],
                'themes': [],
                'sentiment': 'neutral'  # Simplified sentiment
            }
            
            # Extract basic patterns
            # Characters (names starting with capital letters)
            character_pattern = r'\b[A-Z][a-z]+\b'
            characters = list(set(re.findall(character_pattern, text)))
            analysis['entities'].extend([{'type': 'character', 'value': char} for char in characters[:10]])
            
            # Actions (verbs in simple pattern)
            action_words = ['runs', 'walks', 'fights', 'speaks', 'thinks', 'feels', 'sees', 'goes', 'comes', 'takes']
            actions = [word for word in action_words if word in text.lower()]
            analysis['patterns'].extend([f"text_action_{action}" for action in actions])
            
            # Themes (simple keyword detection)
            theme_keywords = {
                'adventure': ['adventure', 'quest', 'journey', 'explore'],
                'conflict': ['fight', 'battle', 'conflict', 'struggle'],
                'romance': ['love', 'heart', 'romance', 'affection'],
                'mystery': ['mystery', 'secret', 'hidden', 'unknown'],
                'magic': ['magic', 'spell', 'enchant', 'mystical']
            }
            
            for theme, keywords in theme_keywords.items():
                if any(keyword in text.lower() for keyword in keywords):
                    analysis['themes'].append(theme)
            
            # Simple sentiment analysis
            positive_words = ['good', 'great', 'wonderful', 'amazing', 'happy', 'joy', 'love']
            negative_words = ['bad', 'terrible', 'awful', 'sad', 'angry', 'hate', 'fear']
            
            positive_count = sum(1 for word in positive_words if word in text.lower())
            negative_count = sum(1 for word in negative_words if word in text.lower())
            
            if positive_count > negative_count:
                analysis['sentiment'] = 'positive'
            elif negative_count > positive_count:
                analysis['sentiment'] = 'negative'
            
            return analysis
            
        except Exception as e:
            logger.error(f"Error processing text modality: {e}")
            return {'error': str(e), 'modality': 'text'}
    
    def _process_structured_modality(self, modal_data: ModalData) -> Dict[str, Any]:
        """Process structured data modality"""
        try:
            content = modal_data.content
            
            analysis = {
                'modality': 'structured',
                'data_type': type(content).__name__,
                'patterns': [],
                'structure': {},
                'entities': []
            }
            
            if isinstance(content, dict):
                analysis['structure'] = {
                    'type': 'dictionary',
                    'keys': list(content.keys()),
                    'key_count': len(content.keys()),
                    'depth': self._calculate_dict_depth(content)
                }
                
                # Extract entities from structured data
                for key, value in content.items():
                    if isinstance(value, str):
                        analysis['entities'].append({'type': 'attribute', 'key': key, 'value': value})
                    elif isinstance(value, (int, float)):
                        analysis['entities'].append({'type': 'numeric', 'key': key, 'value': value})
                    elif isinstance(value, list):
                        analysis['entities'].append({'type': 'list', 'key': key, 'length': len(value)})
                
                # Look for common patterns
                if 'name' in content or 'title' in content:
                    analysis['patterns'].append('structured_named_entity')
                if 'id' in content:
                    analysis['patterns'].append('structured_identified_entity')
                if any(key in content for key in ['x', 'y', 'latitude', 'longitude', 'position']):
                    analysis['patterns'].append('structured_spatial_data')
                if any(key in content for key in ['time', 'date', 'timestamp', 'when']):
                    analysis['patterns'].append('structured_temporal_data')
            
            elif isinstance(content, list):
                analysis['structure'] = {
                    'type': 'list',
                    'length': len(content),
                    'item_types': list(set(type(item).__name__ for item in content))
                }
                
                # Analyze list patterns
                if all(isinstance(item, dict) for item in content):
                    analysis['patterns'].append('structured_object_list')
                elif all(isinstance(item, str) for item in content):
                    analysis['patterns'].append('structured_string_list')
                elif all(isinstance(item, (int, float)) for item in content):
                    analysis['patterns'].append('structured_numeric_list')
            
            return analysis
            
        except Exception as e:
            logger.error(f"Error processing structured modality: {e}")
            return {'error': str(e), 'modality': 'structured'}
    
    def _process_metadata_modality(self, modal_data: ModalData) -> Dict[str, Any]:
        """Process metadata modality"""
        try:
            content = modal_data.content
            
            analysis = {
                'modality': 'metadata',
                'patterns': [],
                'tags': [],
                'classifications': [],
                'attributes': {}
            }
            
            if isinstance(content, dict):
                # Extract tags
                if 'tags' in content:
                    analysis['tags'] = content['tags'] if isinstance(content['tags'], list) else [content['tags']]
                
                # Extract classifications
                if 'category' in content or 'type' in content or 'classification' in content:
                    analysis['classifications'].append(content.get('category', content.get('type', content.get('classification'))))
                
                # Extract other attributes
                for key, value in content.items():
                    if key not in ['tags', 'category', 'type', 'classification']:
                        analysis['attributes'][key] = value
                
                # Identify patterns
                if analysis['tags']:
                    analysis['patterns'].append('metadata_tagged')
                if analysis['classifications']:
                    analysis['patterns'].append('metadata_classified')
                if 'priority' in content or 'importance' in content:
                    analysis['patterns'].append('metadata_prioritized')
                if 'author' in content or 'creator' in content:
                    analysis['patterns'].append('metadata_authored')
            
            return analysis
            
        except Exception as e:
            logger.error(f"Error processing metadata modality: {e}")
            return {'error': str(e), 'modality': 'metadata'}
    
    def _process_temporal_modality(self, modal_data: ModalData) -> Dict[str, Any]:
        """Process temporal data modality"""
        try:
            content = modal_data.content
            
            analysis = {
                'modality': 'temporal',
                'patterns': [],
                'time_points': [],
                'time_ranges': [],
                'temporal_relations': []
            }
            
            # Extract temporal information
            if isinstance(content, dict):
                for key, value in content.items():
                    if 'time' in key.lower() or 'date' in key.lower() or 'when' in key.lower():
                        analysis['time_points'].append({'key': key, 'value': value})
                    elif 'duration' in key.lower() or 'period' in key.lower():
                        analysis['time_ranges'].append({'key': key, 'value': value})
                    elif any(rel in key.lower() for rel in ['before', 'after', 'during', 'while']):
                        analysis['temporal_relations'].append({'key': key, 'value': value})
                
                # Identify patterns
                if analysis['time_points']:
                    analysis['patterns'].append('temporal_timestamped')
                if analysis['time_ranges']:
                    analysis['patterns'].append('temporal_duration')
                if analysis['temporal_relations']:
                    analysis['patterns'].append('temporal_relational')
            
            elif isinstance(content, str):
                # Parse temporal expressions from text
                temporal_words = ['now', 'then', 'later', 'before', 'after', 'during', 'while', 'when']
                for word in temporal_words:
                    if word in content.lower():
                        analysis['temporal_relations'].append({'type': 'expression', 'value': word})
                
                if analysis['temporal_relations']:
                    analysis['patterns'].append('temporal_linguistic')
            
            return analysis
            
        except Exception as e:
            logger.error(f"Error processing temporal modality: {e}")
            return {'error': str(e), 'modality': 'temporal'}
    
    def _process_spatial_modality(self, modal_data: ModalData) -> Dict[str, Any]:
        """Process spatial data modality"""
        try:
            content = modal_data.content
            
            analysis = {
                'modality': 'spatial',
                'patterns': [],
                'locations': [],
                'coordinates': [],
                'spatial_relations': []
            }
            
            if isinstance(content, dict):
                # Look for coordinate data
                if any(key in content for key in ['x', 'y', 'z', 'latitude', 'longitude']):
                    coords = {}
                    for coord_key in ['x', 'y', 'z', 'latitude', 'longitude']:
                        if coord_key in content:
                            coords[coord_key] = content[coord_key]
                    analysis['coordinates'].append(coords)
                    analysis['patterns'].append('spatial_coordinates')
                
                # Look for location names
                for key, value in content.items():
                    if 'location' in key.lower() or 'place' in key.lower() or 'where' in key.lower():
                        analysis['locations'].append({'key': key, 'value': value})
                    elif any(rel in key.lower() for rel in ['near', 'far', 'above', 'below', 'inside', 'outside']):
                        analysis['spatial_relations'].append({'key': key, 'value': value})
                
                if analysis['locations']:
                    analysis['patterns'].append('spatial_named_locations')
                if analysis['spatial_relations']:
                    analysis['patterns'].append('spatial_relational')
            
            elif isinstance(content, str):
                # Parse spatial expressions from text
                spatial_words = ['here', 'there', 'near', 'far', 'above', 'below', 'inside', 'outside', 'north', 'south', 'east', 'west']
                for word in spatial_words:
                    if word in content.lower():
                        analysis['spatial_relations'].append({'type': 'expression', 'value': word})
                
                if analysis['spatial_relations']:
                    analysis['patterns'].append('spatial_linguistic')
            
            return analysis
            
        except Exception as e:
            logger.error(f"Error processing spatial modality: {e}")
            return {'error': str(e), 'modality': 'spatial'}
    
    def _process_numerical_modality(self, modal_data: ModalData) -> Dict[str, Any]:
        """Process numerical data modality"""
        try:
            content = modal_data.content
            
            analysis = {
                'modality': 'numerical',
                'patterns': [],
                'statistics': {},
                'ranges': {},
                'distributions': {}
            }
            
            if isinstance(content, (int, float)):
                analysis['statistics'] = {
                    'value': content,
                    'type': type(content).__name__
                }
                analysis['patterns'].append('numerical_single_value')
            
            elif isinstance(content, list) and all(isinstance(x, (int, float)) for x in content):
                # Calculate basic statistics
                analysis['statistics'] = {
                    'count': len(content),
                    'min': min(content),
                    'max': max(content),
                    'mean': sum(content) / len(content) if content else 0,
                    'sum': sum(content)
                }
                
                analysis['ranges'] = {
                    'range': analysis['statistics']['max'] - analysis['statistics']['min'],
                    'normalized': [(x - analysis['statistics']['min']) / analysis['ranges']['range'] 
                                  for x in content] if analysis['statistics']['max'] != analysis['statistics']['min'] else [0] * len(content)
                }
                
                analysis['patterns'].append('numerical_series')
                
                # Simple distribution analysis
                if len(content) > 1:
                    variance = sum((x - analysis['statistics']['mean']) ** 2 for x in content) / len(content)
                    analysis['distributions']['variance'] = variance
                    analysis['distributions']['std_dev'] = variance ** 0.5
            
            elif isinstance(content, dict):
                # Extract numerical values from dictionary
                numerical_values = []
                for key, value in content.items():
                    if isinstance(value, (int, float)):
                        numerical_values.append(value)
                        analysis['statistics'][key] = value
                
                if numerical_values:
                    analysis['patterns'].append('numerical_structured')
                    analysis['statistics']['total_values'] = len(numerical_values)
                    analysis['statistics']['value_sum'] = sum(numerical_values)
            
            return analysis
            
        except Exception as e:
            logger.error(f"Error processing numerical modality: {e}")
            return {'error': str(e), 'modality': 'numerical'}
    
    def _process_categorical_modality(self, modal_data: ModalData) -> Dict[str, Any]:
        """Process categorical data modality"""
        try:
            content = modal_data.content
            
            analysis = {
                'modality': 'categorical',
                'patterns': [],
                'categories': [],
                'distributions': {},
                'hierarchies': []
            }
            
            if isinstance(content, str):
                analysis['categories'] = [content]
                analysis['patterns'].append('categorical_single')
            
            elif isinstance(content, list):
                analysis['categories'] = list(set(content))  # Unique categories
                
                # Calculate category distribution
                for category in analysis['categories']:
                    analysis['distributions'][category] = content.count(category)
                
                analysis['patterns'].append('categorical_multiple')
                
                # Check for hierarchical patterns (simple)
                hierarchical_patterns = ['/', '\\', '->', '=>', '::']
                for category in analysis['categories']:
                    if any(pattern in str(category) for pattern in hierarchical_patterns):
                        analysis['hierarchies'].append(category)
                
                if analysis['hierarchies']:
                    analysis['patterns'].append('categorical_hierarchical')
            
            elif isinstance(content, dict):
                # Extract categories from dictionary structure
                if 'category' in content:
                    analysis['categories'].append(content['category'])
                if 'type' in content:
                    analysis['categories'].append(content['type'])
                if 'class' in content:
                    analysis['categories'].append(content['class'])
                
                # Look for categorical attributes
                for key, value in content.items():
                    if isinstance(value, str) and len(value.split()) <= 3:  # Likely categorical
                        analysis['categories'].append(value)
                
                if analysis['categories']:
                    analysis['patterns'].append('categorical_structured')
            
            return analysis
            
        except Exception as e:
            logger.error(f"Error processing categorical modality: {e}")
            return {'error': str(e), 'modality': 'categorical'}
    
    def _process_relational_modality(self, modal_data: ModalData) -> Dict[str, Any]:
        """Process relational data modality"""
        try:
            content = modal_data.content
            
            analysis = {
                'modality': 'relational',
                'patterns': [],
                'relationships': [],
                'entities': [],
                'relation_types': []
            }
            
            if isinstance(content, dict):
                # Look for explicit relationships
                for key, value in content.items():
                    if any(rel_word in key.lower() for rel_word in ['relates', 'connected', 'linked', 'associated']):
                        analysis['relationships'].append({
                            'type': key,
                            'value': value
                        })
                    
                    # Common relationship patterns
                    if key.lower() in ['parent', 'child', 'sibling', 'friend', 'enemy', 'ally']:
                        analysis['relationships'].append({
                            'type': key,
                            'target': value
                        })
                        analysis['relation_types'].append(key)
                    
                    # Extract entities involved in relationships
                    if isinstance(value, str):
                        analysis['entities'].append(value)
                
                if analysis['relationships']:
                    analysis['patterns'].append('relational_explicit')
                if len(set(analysis['relation_types'])) > 1:
                    analysis['patterns'].append('relational_multi_type')
            
            elif isinstance(content, list):
                # Look for relationship tuples or pairs
                if all(isinstance(item, (list, tuple)) and len(item) >= 2 for item in content):
                    for item in content:
                        analysis['relationships'].append({
                            'source': item[0],
                            'target': item[1],
                            'type': item[2] if len(item) > 2 else 'related'
                        })
                    analysis['patterns'].append('relational_tuple_list')
            
            return analysis
            
        except Exception as e:
            logger.error(f"Error processing relational modality: {e}")
            return {'error': str(e), 'modality': 'relational'}
    
    def _process_semantic_modality(self, modal_data: ModalData) -> Dict[str, Any]:
        """Process semantic data modality"""
        try:
            content = modal_data.content
            
            analysis = {
                'modality': 'semantic',
                'patterns': [],
                'concepts': [],
                'semantic_relations': [],
                'annotations': {}
            }
            
            if isinstance(content, dict):
                # Look for semantic annotations
                if 'concepts' in content:
                    analysis['concepts'] = content['concepts'] if isinstance(content['concepts'], list) else [content['concepts']]
                
                if 'semantic_type' in content:
                    analysis['annotations']['semantic_type'] = content['semantic_type']
                
                if 'meaning' in content or 'definition' in content:
                    analysis['annotations']['meaning'] = content.get('meaning', content.get('definition'))
                
                # Look for semantic relationships
                semantic_relations = ['is_a', 'part_of', 'similar_to', 'opposite_of', 'causes', 'enables']
                for relation in semantic_relations:
                    if relation in content:
                        analysis['semantic_relations'].append({
                            'relation': relation,
                            'target': content[relation]
                        })
                
                if analysis['concepts']:
                    analysis['patterns'].append('semantic_conceptual')
                if analysis['semantic_relations']:
                    analysis['patterns'].append('semantic_relational')
                if analysis['annotations']:
                    analysis['patterns'].append('semantic_annotated')
            
            elif isinstance(content, str):
                # Simple semantic analysis of text
                # Look for semantic markers
                semantic_markers = ['means', 'refers to', 'is defined as', 'represents', 'symbolizes']
                for marker in semantic_markers:
                    if marker in content.lower():
                        analysis['patterns'].append('semantic_definitional')
                        break
                
                # Extract potential concepts (capitalized words)
                concepts = re.findall(r'\b[A-Z][a-z]+\b', content)
                analysis['concepts'] = list(set(concepts))
                
                if analysis['concepts']:
                    analysis['patterns'].append('semantic_text_concepts')
            
            return analysis
            
        except Exception as e:
            logger.error(f"Error processing semantic modality: {e}")
            return {'error': str(e), 'modality': 'semantic'}
    
    def _calculate_dict_depth(self, d: dict, current_depth: int = 1) -> int:
        """Calculate the depth of a nested dictionary"""
        if not isinstance(d, dict):
            return current_depth
        
        max_depth = current_depth
        for value in d.values():
            if isinstance(value, dict):
                depth = self._calculate_dict_depth(value, current_depth + 1)
                max_depth = max(max_depth, depth)
        
        return max_depth
    
    def _create_unified_representation(self, modal_data_list: List[ModalData], 
                                     modality_analyses: Dict[str, Any]) -> Dict[str, Any]:
        """Create a unified representation across all modalities"""
        unified = {
            'total_modalities': len(modal_data_list),
            'modalities_present': [data.modality.value for data in modal_data_list],
            'unified_entities': [],
            'unified_patterns': [],
            'unified_attributes': {},
            'confidence_summary': {}
        }
        
        # Collect entities from all modalities
        all_entities = []
        for analysis in modality_analyses.values():
            if 'entities' in analysis:
                all_entities.extend(analysis['entities'])
        
        # Deduplicate and organize entities
        entity_map = {}
        for entity in all_entities:
            if isinstance(entity, dict):
                entity_type = entity.get('type', 'unknown')
                entity_value = entity.get('value', entity.get('key', str(entity)))
                
                if entity_type not in entity_map:
                    entity_map[entity_type] = set()
                entity_map[entity_type].add(str(entity_value))
        
        unified['unified_entities'] = {
            entity_type: list(entities) 
            for entity_type, entities in entity_map.items()
        }
        
        # Collect patterns from all modalities
        all_patterns = []
        for analysis in modality_analyses.values():
            if 'patterns' in analysis:
                all_patterns.extend(analysis['patterns'])
        
        unified['unified_patterns'] = list(set(all_patterns))
        
        # Collect attributes that appear across modalities
        common_attributes = {}
        for modality, analysis in modality_analyses.items():
            if 'attributes' in analysis:
                for attr, value in analysis['attributes'].items():
                    if attr not in common_attributes:
                        common_attributes[attr] = []
                    common_attributes[attr].append({
                        'modality': modality,
                        'value': value
                    })
        
        unified['unified_attributes'] = common_attributes
        
        # Calculate confidence summary
        for modal_data in modal_data_list:
            unified['confidence_summary'][modal_data.modality.value] = modal_data.confidence
        
        return unified
    
    def _find_cross_modal_connections(self, modal_data_list: List[ModalData], 
                                    modality_analyses: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Find connections between different modalities"""
        connections = []
        
        # Compare each pair of modalities
        for i, data1 in enumerate(modal_data_list):
            for data2 in modal_data_list[i+1:]:
                modality1 = data1.modality.value
                modality2 = data2.modality.value
                
                if modality1 in modality_analyses and modality2 in modality_analyses:
                    analysis1 = modality_analyses[modality1]
                    analysis2 = modality_analyses[modality2]
                    
                    # Find shared entities
                    entities1 = set()
                    entities2 = set()
                    
                    if 'entities' in analysis1:
                        entities1 = set(str(e.get('value', e)) if isinstance(e, dict) else str(e) 
                                      for e in analysis1['entities'])
                    
                    if 'entities' in analysis2:
                        entities2 = set(str(e.get('value', e)) if isinstance(e, dict) else str(e) 
                                      for e in analysis2['entities'])
                    
                    shared_entities = entities1 & entities2
                    if shared_entities:
                        connections.append({
                            'type': 'shared_entities',
                            'modality1': modality1,
                            'modality2': modality2,
                            'shared_entities': list(shared_entities),
                            'strength': len(shared_entities) / min(len(entities1), len(entities2)) if entities1 and entities2 else 0
                        })
                    
                    # Find pattern similarities
                    patterns1 = set(analysis1.get('patterns', []))
                    patterns2 = set(analysis2.get('patterns', []))
                    shared_patterns = patterns1 & patterns2
                    
                    if shared_patterns:
                        connections.append({
                            'type': 'shared_patterns',
                            'modality1': modality1,
                            'modality2': modality2,
                            'shared_patterns': list(shared_patterns),
                            'strength': len(shared_patterns) / min(len(patterns1), len(patterns2)) if patterns1 and patterns2 else 0
                        })
                    
                    # Check for semantic connections
                    semantic_connection = self._detect_semantic_connection(analysis1, analysis2)
                    if semantic_connection:
                        connections.append({
                            'type': 'semantic_connection',
                            'modality1': modality1,
                            'modality2': modality2,
                            'connection_type': semantic_connection['type'],
                            'description': semantic_connection['description'],
                            'strength': semantic_connection['strength']
                        })
        
        # Sort by strength
        connections.sort(key=lambda c: c.get('strength', 0), reverse=True)
        
        return connections[:self.max_connections_per_item]
    
    def _detect_semantic_connection(self, analysis1: Dict[str, Any], analysis2: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Detect semantic connections between two modality analyses"""
        # Check for temporal-spatial connections
        if analysis1.get('modality') == 'temporal' and analysis2.get('modality') == 'spatial':
            if analysis1.get('time_points') and analysis2.get('locations'):
                return {
                    'type': 'spatiotemporal',
                    'description': 'Events linked to locations in time and space',
                    'strength': 0.7
                }
        
        # Check for text-metadata connections
        if analysis1.get('modality') == 'text' and analysis2.get('modality') == 'metadata':
            if analysis1.get('themes') and analysis2.get('tags'):
                theme_set = set(analysis1['themes'])
                tag_set = set(analysis2['tags'])
                overlap = theme_set & tag_set
                if overlap:
                    return {
                        'type': 'thematic_annotation',
                        'description': f'Text themes match metadata tags: {list(overlap)}',
                        'strength': len(overlap) / max(len(theme_set), len(tag_set))
                    }
        
        # Check for numerical-categorical connections
        if analysis1.get('modality') == 'numerical' and analysis2.get('modality') == 'categorical':
            if analysis1.get('statistics') and analysis2.get('categories'):
                return {
                    'type': 'quantitative_qualitative',
                    'description': 'Numerical data categorized qualitatively',
                    'strength': 0.6
                }
        
        return None
    
    def _discover_multi_modal_patterns(self, modal_data_list: List[ModalData], 
                                     modality_analyses: Dict[str, Any]) -> List[str]:
        """Discover patterns that emerge from multi-modal analysis"""
        patterns = []
        
        # Check for complete story structure
        has_text = any(data.modality == ModalityType.TEXT for data in modal_data_list)
        has_temporal = any(data.modality == ModalityType.TEMPORAL for data in modal_data_list)
        has_spatial = any(data.modality == ModalityType.SPATIAL for data in modal_data_list)
        has_metadata = any(data.modality == ModalityType.METADATA for data in modal_data_list)
        
        if has_text and has_temporal and has_spatial:
            patterns.append('multimodal_narrative_complete')
        
        if has_text and has_metadata:
            patterns.append('multimodal_annotated_content')
        
        if has_temporal and has_spatial:
            patterns.append('multimodal_spatiotemporal')
        
        # Check for rich entity representation
        total_entities = 0
        entity_types = set()
        
        for analysis in modality_analyses.values():
            if 'entities' in analysis:
                total_entities += len(analysis['entities'])
                for entity in analysis['entities']:
                    if isinstance(entity, dict) and 'type' in entity:
                        entity_types.add(entity['type'])
        
        if total_entities > 10 and len(entity_types) > 3:
            patterns.append('multimodal_rich_entities')
        
        # Check for structured narrative
        if (has_text and 
            any('structured' in data.modality.value for data in modal_data_list) and
            any('patterns' in analysis and analysis['patterns'] for analysis in modality_analyses.values())):
            patterns.append('multimodal_structured_narrative')
        
        return patterns
    
    def analyze_story_multimodality(self, story_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Analyze story data across multiple modalities
        
        Args:
            story_data: Dictionary containing various types of story data
        
        Returns:
            Multi-modal analysis results
        """
        try:
            # Convert story data to modal data objects
            modal_data_list = self._convert_story_data_to_modalities(story_data)
            
            # Process all modalities
            result = self.process_multi_modal_data(modal_data_list)
            
            # Add story-specific analysis
            story_analysis = {
                'story_complexity': self._assess_story_complexity(result),
                'narrative_coherence': self._assess_narrative_coherence(result),
                'multi_modal_richness': self._assess_multi_modal_richness(result),
                'story_completeness': self._assess_story_completeness(result)
            }
            
            # Combine results
            final_result = {
                'multi_modal_processing': result.__dict__,
                'story_analysis': story_analysis,
                'recommendations': self._generate_story_recommendations(result, story_analysis)
            }
            
            return final_result
            
        except Exception as e:
            logger.error(f"Error in multi-modal story analysis: {e}")
            return {'error': str(e)}
    
    def _convert_story_data_to_modalities(self, story_data: Dict[str, Any]) -> List[ModalData]:
        """Convert story data to modal data objects"""
        modal_data_list = []
        
        # Text content
        if 'text' in story_data or 'content' in story_data:
            text_content = story_data.get('text', story_data.get('content', ''))
            modal_data_list.append(ModalData(
                data_id='story_text',
                modality=ModalityType.TEXT,
                content=text_content,
                confidence=0.9
            ))
        
        # Structured data (characters, events, etc.)
        structured_data = {}
        for key in ['characters', 'events', 'locations', 'objects', 'plot']:
            if key in story_data:
                structured_data[key] = story_data[key]
        
        if structured_data:
            modal_data_list.append(ModalData(
                data_id='story_structure',
                modality=ModalityType.STRUCTURED,
                content=structured_data,
                confidence=0.8
            ))
        
        # Metadata
        metadata = {}
        for key in ['genre', 'tags', 'category', 'author', 'title', 'rating']:
            if key in story_data:
                metadata[key] = story_data[key]
        
        if metadata:
            modal_data_list.append(ModalData(
                data_id='story_metadata',
                modality=ModalityType.METADATA,
                content=metadata,
                confidence=0.7
            ))
        
        # Temporal data
        if 'timeline' in story_data or 'events' in story_data:
            temporal_content = story_data.get('timeline', story_data.get('events', []))
            modal_data_list.append(ModalData(
                data_id='story_temporal',
                modality=ModalityType.TEMPORAL,
                content=temporal_content,
                confidence=0.6
            ))
        
        # Spatial data
        if 'locations' in story_data or 'settings' in story_data:
            spatial_content = story_data.get('locations', story_data.get('settings', []))
            modal_data_list.append(ModalData(
                data_id='story_spatial',
                modality=ModalityType.SPATIAL,
                content=spatial_content,
                confidence=0.6
            ))
        
        return modal_data_list
    
    def _assess_story_complexity(self, result: ProcessingResult) -> float:
        """Assess the complexity of the story based on multi-modal analysis"""
        complexity = 0.0
        
        # Factor in number of modalities
        complexity += len(result.modality_analyses) * 0.1
        
        # Factor in cross-modal connections
        complexity += len(result.cross_modal_connections) * 0.15
        
        # Factor in patterns discovered
        complexity += len(result.patterns_discovered) * 0.1
        
        # Factor in unified entity diversity
        if 'unified_entities' in result.unified_representation:
            entity_types = len(result.unified_representation['unified_entities'])
            complexity += entity_types * 0.1
        
        return min(1.0, complexity)
    
    def _assess_narrative_coherence(self, result: ProcessingResult) -> float:
        """Assess narrative coherence across modalities"""
        coherence = 0.5  # Base coherence
        
        # High cross-modal connections suggest coherence
        if result.cross_modal_connections:
            avg_connection_strength = sum(conn.get('strength', 0) for conn in result.cross_modal_connections) / len(result.cross_modal_connections)
            coherence += avg_connection_strength * 0.3
        
        # Consistent patterns across modalities
        if 'multimodal_narrative_complete' in result.patterns_discovered:
            coherence += 0.2
        
        # Overall confidence
        overall_confidence = result.get_overall_confidence()
        coherence += overall_confidence * 0.2
        
        return min(1.0, coherence)
    
    def _assess_multi_modal_richness(self, result: ProcessingResult) -> float:
        """Assess the richness of multi-modal representation"""
        richness = 0.0
        
        # Number of modalities present
        richness += len(result.modality_analyses) * 0.2
        
        # Rich entity representation
        if 'multimodal_rich_entities' in result.patterns_discovered:
            richness += 0.3
        
        # Presence of complete patterns
        complete_patterns = [p for p in result.patterns_discovered if 'complete' in p]
        richness += len(complete_patterns) * 0.1
        
        # Cross-modal pattern diversity
        pattern_types = set()
        for conn in result.cross_modal_connections:
            pattern_types.add(conn.get('type', ''))
        richness += len(pattern_types) * 0.1
        
        return min(1.0, richness)
    
    def _assess_story_completeness(self, result: ProcessingResult) -> float:
        """Assess how complete the story representation is"""
        completeness = 0.0
        
        # Check for essential story elements
        essential_modalities = ['text', 'structured', 'metadata']
        present_modalities = result.unified_representation.get('modalities_present', [])
        
        for modality in essential_modalities:
            if modality in present_modalities:
                completeness += 0.25
        
        # Bonus for temporal and spatial information
        if 'temporal' in present_modalities:
            completeness += 0.1
        if 'spatial' in present_modalities:
            completeness += 0.1
        
        # Check for narrative completeness patterns
        if 'multimodal_narrative_complete' in result.patterns_discovered:
            completeness += 0.15
        
        return min(1.0, completeness)
    
    def _generate_story_recommendations(self, result: ProcessingResult, story_analysis: Dict[str, Any]) -> List[str]:
        """Generate recommendations for improving story representation"""
        recommendations = []
        
        # Check completeness
        if story_analysis['story_completeness'] < 0.7:
            missing_modalities = []
            present = result.unified_representation.get('modalities_present', [])
            
            if 'temporal' not in present:
                missing_modalities.append('temporal information (timeline, event sequence)')
            if 'spatial' not in present:
                missing_modalities.append('spatial information (locations, settings)')
            if 'metadata' not in present:
                missing_modalities.append('metadata (genre, tags, classifications)')
            
            if missing_modalities:
                recommendations.append(f"Consider adding: {', '.join(missing_modalities)}")
        
        # Check coherence
        if story_analysis['narrative_coherence'] < 0.6:
            recommendations.append("Improve connections between different story elements")
            recommendations.append("Ensure consistent character and event references across modalities")
        
        # Check complexity
        if story_analysis['story_complexity'] < 0.4:
            recommendations.append("Consider adding more detailed character information")
            recommendations.append("Expand plot structure with more events and relationships")
        
        # Check richness
        if story_analysis['multi_modal_richness'] < 0.5:
            recommendations.append("Add more diverse entity types (objects, locations, concepts)")
            recommendations.append("Include more detailed metadata and annotations")
        
        return recommendations
    
    def get_processing_statistics(self) -> Dict[str, Any]:
        """Get statistics about multi-modal processing performance"""
        uptime = time.time() - self.processing_stats['start_time']
        
        return {
            'total_items_processed': self.processing_stats['total_items_processed'],
            'modalities_processed': dict(self.processing_stats['modalities_processed']),
            'cross_modal_connections': self.processing_stats['cross_modal_connections'],
            'patterns_discovered': self.processing_stats['patterns_discovered'],
            'supported_modalities': [modality.value for modality in ModalityType],
            'uptime_seconds': uptime,
            'processing_rate': self.processing_stats['total_items_processed'] / uptime if uptime > 0 else 0
        }
    
    def clear_processing_data(self):
        """Clear processing data and reset statistics"""
        self.unified_knowledge.clear()
        self.cross_modal_patterns.clear()
        
        # Reset stats but keep start time
        start_time = self.processing_stats['start_time']
        self.processing_stats = {
            'total_items_processed': 0,
            'modalities_processed': {},
            'cross_modal_connections': 0,
            'patterns_discovered': 0,
            'start_time': start_time
        }
        
        logger.info("Multi-modal processing data cleared")