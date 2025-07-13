"""
Advanced Reasoning Engine - Phase 5 Integration

This module integrates all reasoning components (logical inference, temporal reasoning,
causal networks, and multi-modal processing) into a unified advanced reasoning system.
"""

import logging
import time
import asyncio
from typing import Dict, List, Set, Any, Optional, Tuple
from dataclasses import dataclass, field

from .inference import LogicalInferenceEngine, InferenceResult
from .temporal import TemporalReasoningEngine, TemporalEvent, TemporalConstraint
from .causal import CausalReasoningNetwork, PlotElement, CausalLink
from .multimodal import MultiModalProcessor, ModalData, ProcessingResult

logger = logging.getLogger(__name__)


@dataclass
class ReasoningRequest:
    """Request for advanced reasoning processing"""
    request_id: str
    story_data: Dict[str, Any]
    reasoning_types: List[str] = field(default_factory=lambda: ['logical', 'temporal', 'causal', 'multimodal'])
    context: Dict[str, Any] = field(default_factory=dict)
    priority: float = 0.5
    
    def __post_init__(self):
        """Validate request"""
        valid_types = ['logical', 'temporal', 'causal', 'multimodal']
        self.reasoning_types = [t for t in self.reasoning_types if t in valid_types]
        if not self.reasoning_types:
            self.reasoning_types = ['logical']


@dataclass
class AdvancedReasoningResult:
    """Result of advanced reasoning processing"""
    request_id: str
    logical_analysis: Optional[Dict[str, Any]] = None
    temporal_analysis: Optional[Dict[str, Any]] = None
    causal_analysis: Optional[Dict[str, Any]] = None
    multimodal_analysis: Optional[Dict[str, Any]] = None
    integrated_insights: Dict[str, Any] = field(default_factory=dict)
    reasoning_patterns: List[str] = field(default_factory=list)
    cognitive_schemas: Dict[str, Any] = field(default_factory=dict)
    processing_time: float = 0.0
    overall_confidence: float = 0.0
    
    def __post_init__(self):
        """Calculate overall confidence"""
        confidences = []
        
        if self.logical_analysis and 'reasoning_confidence' in self.logical_analysis:
            confidences.append(self.logical_analysis['reasoning_confidence'])
        
        if self.temporal_analysis and 'continuity_score' in self.temporal_analysis:
            confidences.append(self.temporal_analysis['continuity_score'])
        
        if self.causal_analysis and 'causal_network_stats' in self.causal_analysis:
            avg_influence = self.causal_analysis['causal_network_stats'].get('average_influence', 0)
            confidences.append(avg_influence)
        
        if self.multimodal_analysis and 'multi_modal_processing' in self.multimodal_analysis:
            mm_result = self.multimodal_analysis['multi_modal_processing']
            if isinstance(mm_result, dict) and 'confidence_scores' in mm_result:
                scores = mm_result['confidence_scores']
                if scores:
                    avg_mm_conf = sum(scores.values()) / len(scores)
                    confidences.append(avg_mm_conf)
        
        # If no specific confidences found, use a default based on presence of analyses
        if not confidences:
            analysis_count = 0
            if self.logical_analysis and not self.logical_analysis.get('error'):
                analysis_count += 1
            if self.temporal_analysis and not self.temporal_analysis.get('error'):
                analysis_count += 1
            if self.causal_analysis and not self.causal_analysis.get('error'):
                analysis_count += 1
            if self.multimodal_analysis and not self.multimodal_analysis.get('error'):
                analysis_count += 1
            
            if analysis_count > 0:
                confidences.append(0.5)  # Default confidence when analyses complete successfully
        
        self.overall_confidence = sum(confidences) / len(confidences) if confidences else 0.0
    
    def _recalculate_confidence(self):
        """Recalculate overall confidence after all analyses are complete"""
        confidences = []
        
        if self.logical_analysis and not self.logical_analysis.get('error'):
            conf = self.logical_analysis.get('reasoning_confidence', 0.5)
            confidences.append(conf)
        
        if self.temporal_analysis and not self.temporal_analysis.get('error'):
            conf = self.temporal_analysis.get('continuity_score', 0.5)
            confidences.append(conf)
        
        if self.causal_analysis and not self.causal_analysis.get('error'):
            causal_stats = self.causal_analysis.get('causal_network_stats', {})
            # Use network density as a proxy for confidence
            conf = causal_stats.get('network_density', 0.3)
            confidences.append(conf)
        
        if self.multimodal_analysis and not self.multimodal_analysis.get('error'):
            story_analysis = self.multimodal_analysis.get('story_analysis', {})
            # Use narrative coherence as confidence
            conf = story_analysis.get('narrative_coherence', 0.5)
            confidences.append(conf)
        
        # Update overall confidence
        self.overall_confidence = sum(confidences) / len(confidences) if confidences else 0.0


class AdvancedReasoningEngine:
    """
    Advanced Reasoning Engine for Phase 5
    
    Integrates logical inference, temporal reasoning, causal networks,
    and multi-modal processing for comprehensive story understanding
    and generation.
    """
    
    def __init__(self):
        # Initialize component engines
        self.logical_engine = LogicalInferenceEngine()
        self.temporal_engine = TemporalReasoningEngine()
        self.causal_network = CausalReasoningNetwork()
        self.multimodal_processor = MultiModalProcessor()
        
        # Integration state
        self.reasoning_cache: Dict[str, AdvancedReasoningResult] = {}
        self.cognitive_schemas: Dict[str, Dict[str, Any]] = {}
        self.cross_engine_patterns: List[Dict[str, Any]] = []
        
        # Statistics
        self.reasoning_stats = {
            'requests_processed': 0,
            'logical_inferences': 0,
            'temporal_analyses': 0,
            'causal_analyses': 0,
            'multimodal_analyses': 0,
            'schema_updates': 0,
            'start_time': time.time()
        }
        
        # Configuration
        self.enable_cross_engine_integration = True
        self.cache_results = True
        self.max_cache_size = 100
        
        # Initialize default cognitive schemas
        self._initialize_cognitive_schemas()
        
        logger.info("Advanced Reasoning Engine initialized with all component engines")
    
    def _initialize_cognitive_schemas(self):
        """Initialize default cognitive schemas for story reasoning"""
        
        # Narrative schema
        self.cognitive_schemas['narrative'] = {
            'components': ['characters', 'plot', 'setting', 'theme', 'conflict'],
            'relationships': {
                'character_plot': 'Characters drive plot through actions and decisions',
                'setting_atmosphere': 'Setting influences atmosphere and mood',
                'conflict_tension': 'Conflict creates narrative tension',
                'theme_meaning': 'Theme provides deeper meaning'
            },
            'patterns': [
                'exposition -> rising_action -> climax -> falling_action -> resolution',
                'character_introduction -> character_development -> character_arc',
                'problem_identification -> complications -> resolution'
            ]
        }
        
        # Character schema
        self.cognitive_schemas['character'] = {
            'attributes': ['name', 'traits', 'goals', 'motivations', 'relationships'],
            'development_stages': ['introduction', 'development', 'transformation', 'resolution'],
            'relationship_types': ['ally', 'enemy', 'neutral', 'romantic', 'familial'],
            'action_patterns': {
                'goal_directed': 'Characters act to achieve their goals',
                'trait_consistent': 'Character actions reflect their established traits',
                'motivation_driven': 'Character decisions stem from their motivations'
            }
        }
        
        # Plot schema
        self.cognitive_schemas['plot'] = {
            'structure_types': ['linear', 'non_linear', 'episodic', 'circular'],
            'conflict_types': ['person_vs_person', 'person_vs_nature', 'person_vs_society', 'person_vs_self'],
            'progression_patterns': {
                'cause_effect': 'Events follow logical cause-effect relationships',
                'escalation': 'Conflicts escalate to create tension',
                'resolution': 'Conflicts are resolved through character actions'
            },
            'pacing_elements': ['action_scenes', 'dialogue', 'description', 'reflection']
        }
        
        # World-building schema
        self.cognitive_schemas['world'] = {
            'elements': ['geography', 'culture', 'history', 'rules', 'inhabitants'],
            'consistency_rules': {
                'physical_laws': 'World follows consistent physical or magical laws',
                'cultural_patterns': 'Cultures have consistent beliefs and practices',
                'historical_continuity': 'Events follow logical historical progression'
            },
            'interaction_patterns': {
                'character_world': 'Characters are shaped by and shape their world',
                'world_plot': 'World constraints and opportunities drive plot events'
            }
        }
        
        logger.info(f"Initialized {len(self.cognitive_schemas)} cognitive schemas")
    
    async def process_reasoning_request(self, request: ReasoningRequest) -> AdvancedReasoningResult:
        """
        Process a comprehensive reasoning request
        
        Args:
            request: ReasoningRequest containing story data and parameters
        
        Returns:
            AdvancedReasoningResult with comprehensive analysis
        """
        start_time = time.time()
        
        try:
            logger.info(f"Processing reasoning request {request.request_id} with types: {request.reasoning_types}")
            
            # Initialize result
            result = AdvancedReasoningResult(request_id=request.request_id)
            
            # Process each requested reasoning type
            if 'logical' in request.reasoning_types:
                result.logical_analysis = await self._process_logical_reasoning(request.story_data, request.context)
                self.reasoning_stats['logical_inferences'] += 1
            
            if 'temporal' in request.reasoning_types:
                result.temporal_analysis = await self._process_temporal_reasoning(request.story_data, request.context)
                self.reasoning_stats['temporal_analyses'] += 1
            
            if 'causal' in request.reasoning_types:
                result.causal_analysis = await self._process_causal_reasoning(request.story_data, request.context)
                self.reasoning_stats['causal_analyses'] += 1
            
            if 'multimodal' in request.reasoning_types:
                result.multimodal_analysis = await self._process_multimodal_reasoning(request.story_data, request.context)
                self.reasoning_stats['multimodal_analyses'] += 1
            
            # Integrate insights across reasoning types
            if self.enable_cross_engine_integration:
                result.integrated_insights = self._integrate_reasoning_insights(result)
                result.reasoning_patterns = self._discover_reasoning_patterns(result)
                result.cognitive_schemas = self._update_cognitive_schemas(result)
                self.reasoning_stats['schema_updates'] += 1
            
            # Calculate processing time
            result.processing_time = time.time() - start_time
            
            # Recalculate overall confidence after all analyses are complete
            result._recalculate_confidence()
            
            # Cache result if enabled
            if self.cache_results:
                self._cache_result(request.request_id, result)
            
            # Update statistics
            self.reasoning_stats['requests_processed'] += 1
            
            logger.info(f"Completed reasoning request {request.request_id} in {result.processing_time:.3f}s with confidence {result.overall_confidence:.3f}")
            
            return result
            
        except Exception as e:
            logger.error(f"Error processing reasoning request {request.request_id}: {e}")
            return AdvancedReasoningResult(
                request_id=request.request_id,
                integrated_insights={'error': str(e)},
                processing_time=time.time() - start_time
            )
    
    async def _process_logical_reasoning(self, story_data: Dict[str, Any], context: Dict[str, Any]) -> Dict[str, Any]:
        """Process logical reasoning for the story"""
        try:
            # Extract story elements for logical analysis
            story_elements = {
                'characters': story_data.get('characters', []),
                'events': story_data.get('events', []),
                'locations': story_data.get('locations', []),
                'objects': story_data.get('objects', [])
            }
            
            # Perform narrative reasoning
            reasoning_result = self.logical_engine.reason_about_narrative(story_elements)
            
            # Add logical consistency checks
            if 'text' in story_data:
                # Extract patterns from text
                text = story_data['text']
                patterns = self.logical_engine._story_elements_to_patterns(story_elements)
                self.logical_engine.add_knowledge(patterns)
                
                # Perform inference
                inference_result = self.logical_engine.infer_new_knowledge()
                reasoning_result['inference_results'] = {
                    'new_patterns': len(inference_result.new_knowledge),
                    'rules_applied': len(set(inference_result.applied_rules)),
                    'reasoning_steps': len(inference_result.reasoning_path)
                }
            
            # Add cognitive schema validation
            reasoning_result['schema_compliance'] = self._validate_against_schemas(story_elements)
            
            # Ensure we have a reasoning confidence score
            if 'reasoning_confidence' not in reasoning_result:
                # Calculate confidence based on successful pattern generation and implications
                implications = reasoning_result.get('narrative_implications', [])
                patterns_count = reasoning_result.get('patterns_derived', 0)
                
                if implications or patterns_count > 0:
                    reasoning_result['reasoning_confidence'] = min(0.8, 0.3 + len(implications) * 0.1 + patterns_count * 0.05)
                else:
                    reasoning_result['reasoning_confidence'] = 0.5  # Default confidence
            
            return reasoning_result
            
        except Exception as e:
            logger.error(f"Error in logical reasoning: {e}")
            return {'error': str(e)}
    
    async def _process_temporal_reasoning(self, story_data: Dict[str, Any], context: Dict[str, Any]) -> Dict[str, Any]:
        """Process temporal reasoning for story continuity"""
        try:
            # Extract events for temporal analysis
            story_events = []
            
            if 'events' in story_data:
                story_events.extend(story_data['events'])
            
            if 'timeline' in story_data:
                story_events.extend(story_data['timeline'])
            
            # Extract events from text if present
            if 'text' in story_data and not story_events:
                # Simple event extraction from text
                text = story_data['text']
                sentences = text.split('.')
                for i, sentence in enumerate(sentences):
                    if sentence.strip():
                        story_events.append({
                            'id': f'text_event_{i}',
                            'description': sentence.strip(),
                            'timestamp': i,
                            'participants': self._extract_participants_from_text(sentence)
                        })
            
            # Analyze story continuity
            continuity_result = self.temporal_engine.analyze_story_continuity(story_events)
            
            # Add temporal cognitive schema validation
            continuity_result['temporal_schema_compliance'] = self._validate_temporal_schemas(story_events)
            
            return continuity_result
            
        except Exception as e:
            logger.error(f"Error in temporal reasoning: {e}")
            return {'error': str(e)}
    
    async def _process_causal_reasoning(self, story_data: Dict[str, Any], context: Dict[str, Any]) -> Dict[str, Any]:
        """Process causal reasoning for plot development"""
        try:
            # Analyze plot causality
            causal_result = self.causal_network.analyze_plot_causality(story_data)
            
            # Add plot development predictions
            if causal_result and 'plot_predictions' in causal_result:
                # Enhance predictions with cognitive schemas
                enhanced_predictions = self._enhance_plot_predictions(causal_result['plot_predictions'])
                causal_result['enhanced_predictions'] = enhanced_predictions
            
            # Add causal schema validation
            causal_result['causal_schema_compliance'] = self._validate_causal_schemas(story_data)
            
            return causal_result
            
        except Exception as e:
            logger.error(f"Error in causal reasoning: {e}")
            return {'error': str(e)}
    
    async def _process_multimodal_reasoning(self, story_data: Dict[str, Any], context: Dict[str, Any]) -> Dict[str, Any]:
        """Process multi-modal reasoning"""
        try:
            # Analyze story multimodality
            multimodal_result = self.multimodal_processor.analyze_story_multimodality(story_data)
            
            # Add multi-modal cognitive schema validation
            multimodal_result['multimodal_schema_compliance'] = self._validate_multimodal_schemas(story_data)
            
            return multimodal_result
            
        except Exception as e:
            logger.error(f"Error in multi-modal reasoning: {e}")
            return {'error': str(e)}
    
    def _integrate_reasoning_insights(self, result: AdvancedReasoningResult) -> Dict[str, Any]:
        """Integrate insights from different reasoning engines"""
        insights = {
            'cross_engine_correlations': [],
            'unified_confidence': result.overall_confidence,
            'story_assessment': {},
            'reasoning_synthesis': {}
        }
        
        # Find correlations between logical and causal reasoning
        if result.logical_analysis and result.causal_analysis:
            logical_implications = result.logical_analysis.get('narrative_implications', [])
            causal_chains = result.causal_analysis.get('major_causal_chains', [])
            
            # Check for overlapping elements
            logical_elements = set()
            for impl in logical_implications:
                if 'pattern' in impl:
                    # Extract concept nodes from patterns
                    import re
                    concepts = re.findall(r'ConceptNode "([^"]*)"', impl['pattern'])
                    logical_elements.update(concepts)
            
            causal_elements = set()
            for chain in causal_chains:
                if 'elements' in chain:
                    for elem in chain['elements']:
                        if 'element_id' in elem:
                            causal_elements.add(elem['element_id'])
            
            overlap = logical_elements & causal_elements
            if overlap:
                insights['cross_engine_correlations'].append({
                    'type': 'logical_causal_overlap',
                    'shared_elements': list(overlap),
                    'strength': len(overlap) / max(len(logical_elements), len(causal_elements)) if logical_elements and causal_elements else 0
                })
        
        # Find correlations between temporal and causal reasoning
        if result.temporal_analysis and result.causal_analysis:
            temporal_events = result.temporal_analysis.get('timeline', [])
            causal_network_stats = result.causal_analysis.get('causal_network_stats', {})
            
            if temporal_events and causal_network_stats.get('total_links', 0) > 0:
                # Correlate temporal continuity with causal complexity
                continuity_score = result.temporal_analysis.get('continuity_score', 0)
                network_density = causal_network_stats.get('network_density', 0)
                
                correlation_strength = min(continuity_score, network_density)
                insights['cross_engine_correlations'].append({
                    'type': 'temporal_causal_correlation',
                    'continuity_score': continuity_score,
                    'causal_density': network_density,
                    'correlation_strength': correlation_strength
                })
        
        # Assess overall story quality
        story_assessment = {
            'logical_consistency': 0.5,
            'temporal_coherence': 0.5,
            'causal_complexity': 0.5,
            'modal_richness': 0.5
        }
        
        if result.logical_analysis:
            story_assessment['logical_consistency'] = result.logical_analysis.get('reasoning_confidence', 0.5)
        
        if result.temporal_analysis:
            story_assessment['temporal_coherence'] = result.temporal_analysis.get('continuity_score', 0.5)
        
        if result.causal_analysis:
            causal_stats = result.causal_analysis.get('causal_network_stats', {})
            story_assessment['causal_complexity'] = causal_stats.get('network_density', 0.5)
        
        if result.multimodal_analysis:
            mm_analysis = result.multimodal_analysis.get('story_analysis', {})
            story_assessment['modal_richness'] = mm_analysis.get('multi_modal_richness', 0.5)
        
        insights['story_assessment'] = story_assessment
        
        # Create reasoning synthesis
        synthesis = {
            'overall_story_quality': sum(story_assessment.values()) / len(story_assessment),
            'strongest_reasoning_aspect': max(story_assessment, key=story_assessment.get),
            'weakest_reasoning_aspect': min(story_assessment, key=story_assessment.get),
            'improvement_suggestions': []
        }
        
        # Generate improvement suggestions
        for aspect, score in story_assessment.items():
            if score < 0.6:
                if aspect == 'logical_consistency':
                    synthesis['improvement_suggestions'].append("Add more logical connections between story elements")
                elif aspect == 'temporal_coherence':
                    synthesis['improvement_suggestions'].append("Improve temporal flow and event sequencing")
                elif aspect == 'causal_complexity':
                    synthesis['improvement_suggestions'].append("Develop stronger cause-effect relationships")
                elif aspect == 'modal_richness':
                    synthesis['improvement_suggestions'].append("Enhance multi-modal story representation")
        
        insights['reasoning_synthesis'] = synthesis
        
        return insights
    
    def _discover_reasoning_patterns(self, result: AdvancedReasoningResult) -> List[str]:
        """Discover patterns across different reasoning engines"""
        patterns = []
        
        # Check for consistent narrative elements
        if (result.logical_analysis and result.temporal_analysis and 
            result.logical_analysis.get('reasoning_confidence', 0) > 0.7 and
            result.temporal_analysis.get('continuity_score', 0) > 0.7):
            patterns.append('high_narrative_consistency')
        
        # Check for complex causal structures
        if result.causal_analysis:
            causal_stats = result.causal_analysis.get('causal_network_stats', {})
            if (causal_stats.get('total_links', 0) > 5 and 
                causal_stats.get('network_density', 0) > 0.3):
                patterns.append('complex_causal_structure')
        
        # Check for rich multi-modal representation
        if result.multimodal_analysis:
            mm_analysis = result.multimodal_analysis.get('story_analysis', {})
            if mm_analysis.get('multi_modal_richness', 0) > 0.7:
                patterns.append('rich_multimodal_representation')
        
        # Check for temporal-causal alignment
        if (result.temporal_analysis and result.causal_analysis):
            temporal_events = len(result.temporal_analysis.get('timeline', []))
            causal_elements = result.causal_analysis.get('causal_network_stats', {}).get('total_elements', 0)
            
            if temporal_events > 0 and causal_elements > 0:
                ratio = min(temporal_events, causal_elements) / max(temporal_events, causal_elements)
                if ratio > 0.8:
                    patterns.append('temporal_causal_alignment')
        
        # Check for logical-multimodal integration
        if (result.logical_analysis and result.multimodal_analysis):
            logical_patterns = len(result.logical_analysis.get('narrative_implications', []))
            mm_patterns = len(result.multimodal_analysis.get('multi_modal_processing', {}).get('patterns_discovered', []))
            
            if logical_patterns > 0 and mm_patterns > 0:
                patterns.append('logical_multimodal_integration')
        
        return patterns
    
    def _update_cognitive_schemas(self, result: AdvancedReasoningResult) -> Dict[str, Any]:
        """Update cognitive schemas based on reasoning results"""
        schema_updates = {}
        
        # Update narrative schema based on logical analysis
        if result.logical_analysis:
            narrative_implications = result.logical_analysis.get('narrative_implications', [])
            if narrative_implications:
                schema_updates['narrative'] = {
                    'new_patterns_discovered': len(narrative_implications),
                    'confidence_in_structure': result.logical_analysis.get('reasoning_confidence', 0)
                }
        
        # Update character schema based on causal analysis
        if result.causal_analysis:
            influence_analysis = result.causal_analysis.get('influence_analysis', {})
            if influence_analysis.get('top_influencers'):
                character_influencers = [
                    inf for inf in influence_analysis['top_influencers'] 
                    if inf.get('element_type') == 'character'
                ]
                if character_influencers:
                    schema_updates['character'] = {
                        'dominant_characters': len(character_influencers),
                        'average_influence': sum(ch.get('influence_score', 0) for ch in character_influencers) / len(character_influencers)
                    }
        
        # Update plot schema based on temporal analysis
        if result.temporal_analysis:
            timeline = result.temporal_analysis.get('timeline', [])
            if timeline:
                schema_updates['plot'] = {
                    'event_count': len(timeline),
                    'temporal_complexity': len(result.temporal_analysis.get('inconsistencies', [])),
                    'continuity_score': result.temporal_analysis.get('continuity_score', 0)
                }
        
        # Update world schema based on multimodal analysis
        if result.multimodal_analysis:
            mm_processing = result.multimodal_analysis.get('multi_modal_processing', {})
            if isinstance(mm_processing, dict):
                unified_rep = mm_processing.get('unified_representation', {})
                if unified_rep.get('unified_entities'):
                    schema_updates['world'] = {
                        'entity_diversity': len(unified_rep['unified_entities']),
                        'modal_richness': mm_processing.get('confidence_scores', {})
                    }
        
        return schema_updates
    
    def _extract_participants_from_text(self, text: str) -> List[str]:
        """Extract character names/participants from text"""
        import re
        # Simple pattern for names (capitalized words)
        participants = re.findall(r'\b[A-Z][a-z]+\b', text)
        # Filter out common non-name words
        common_words = {'The', 'He', 'She', 'It', 'They', 'This', 'That', 'There', 'Here', 'When', 'Where', 'What', 'How', 'Why'}
        return [p for p in participants if p not in common_words]
    
    def _validate_against_schemas(self, story_elements: Dict[str, Any]) -> Dict[str, float]:
        """Validate story elements against cognitive schemas"""
        validation_scores = {}
        
        # Validate narrative structure
        narrative_score = 0.5
        if story_elements.get('characters'):
            narrative_score += 0.2
        if story_elements.get('events'):
            narrative_score += 0.2
        if story_elements.get('locations'):
            narrative_score += 0.1
        validation_scores['narrative'] = min(1.0, narrative_score)
        
        # Validate character development
        character_score = 0.0
        characters = story_elements.get('characters', [])
        if characters:
            # Check for character attributes
            for char in characters:
                if isinstance(char, dict):
                    if char.get('name'):
                        character_score += 0.1
                    if char.get('traits'):
                        character_score += 0.1
                    if char.get('goals') or char.get('motivations'):
                        character_score += 0.1
        validation_scores['character'] = min(1.0, character_score / max(1, len(characters)))
        
        return validation_scores
    
    def _validate_temporal_schemas(self, story_events: List[Dict[str, Any]]) -> Dict[str, float]:
        """Validate temporal aspects against schemas"""
        if not story_events:
            return {'temporal_structure': 0.0}
        
        # Check for temporal ordering
        timestamped_events = [e for e in story_events if e.get('timestamp')]
        temporal_score = len(timestamped_events) / len(story_events) if story_events else 0
        
        return {'temporal_structure': temporal_score}
    
    def _validate_causal_schemas(self, story_data: Dict[str, Any]) -> Dict[str, float]:
        """Validate causal aspects against schemas"""
        causal_score = 0.5
        
        # Check for explicit causal relationships
        if 'events' in story_data:
            events = story_data['events']
            causal_indicators = 0
            
            for event in events:
                if isinstance(event, dict):
                    desc = str(event.get('description', ''))
                    if any(word in desc.lower() for word in ['because', 'causes', 'leads to', 'results in']):
                        causal_indicators += 1
            
            if events:
                causal_score = causal_indicators / len(events)
        
        return {'causal_structure': causal_score}
    
    def _validate_multimodal_schemas(self, story_data: Dict[str, Any]) -> Dict[str, float]:
        """Validate multi-modal aspects against schemas"""
        modality_count = 0
        
        # Count different modalities present
        if story_data.get('text'):
            modality_count += 1
        if story_data.get('characters') or story_data.get('events'):
            modality_count += 1
        if story_data.get('metadata') or story_data.get('tags'):
            modality_count += 1
        if story_data.get('timeline'):
            modality_count += 1
        if story_data.get('locations'):
            modality_count += 1
        
        multimodal_score = min(1.0, modality_count / 3.0)  # Expect at least 3 modalities for full score
        
        return {'multimodal_richness': multimodal_score}
    
    def _enhance_plot_predictions(self, predictions: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Enhance plot predictions with cognitive schema knowledge"""
        enhanced = []
        
        for prediction in predictions:
            enhanced_pred = prediction.copy()
            
            # Add schema-based enhancements
            if prediction.get('type') == 'untapped_potential':
                # Add narrative schema insights
                enhanced_pred['schema_insight'] = "Character with high potential may become central to plot resolution"
                enhanced_pred['narrative_role'] = "potential_protagonist" if prediction.get('confidence', 0) > 0.8 else "supporting_character"
            
            elif prediction.get('type') == 'chain_extension':
                # Add causal schema insights
                enhanced_pred['schema_insight'] = "Causal chain may follow escalation pattern leading to climax"
                enhanced_pred['plot_function'] = "rising_action" if prediction.get('confidence', 0) > 0.7 else "complication"
            
            enhanced.append(enhanced_pred)
        
        return enhanced
    
    def _cache_result(self, request_id: str, result: AdvancedReasoningResult):
        """Cache reasoning result"""
        if len(self.reasoning_cache) >= self.max_cache_size:
            # Remove oldest entry
            oldest_key = min(self.reasoning_cache.keys())
            del self.reasoning_cache[oldest_key]
        
        self.reasoning_cache[request_id] = result
    
    def get_cached_result(self, request_id: str) -> Optional[AdvancedReasoningResult]:
        """Get cached reasoning result"""
        return self.reasoning_cache.get(request_id)
    
    def reason_about_story(self, story_data: Dict[str, Any], 
                          reasoning_types: Optional[List[str]] = None) -> AdvancedReasoningResult:
        """
        Convenience method for reasoning about a story
        
        Args:
            story_data: Story data to analyze
            reasoning_types: Types of reasoning to apply (default: all)
        
        Returns:
            AdvancedReasoningResult with comprehensive analysis
        """
        request = ReasoningRequest(
            request_id=f"story_analysis_{int(time.time())}",
            story_data=story_data,
            reasoning_types=reasoning_types or ['logical', 'temporal', 'causal', 'multimodal']
        )
        
        # Run synchronously for convenience
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            result = loop.run_until_complete(self.process_reasoning_request(request))
            return result
        finally:
            loop.close()
    
    def get_reasoning_statistics(self) -> Dict[str, Any]:
        """Get comprehensive reasoning statistics"""
        uptime = time.time() - self.reasoning_stats['start_time']
        
        return {
            'overall_stats': {
                'requests_processed': self.reasoning_stats['requests_processed'],
                'uptime_seconds': uptime,
                'processing_rate': self.reasoning_stats['requests_processed'] / uptime if uptime > 0 else 0
            },
            'component_stats': {
                'logical_inferences': self.reasoning_stats['logical_inferences'],
                'temporal_analyses': self.reasoning_stats['temporal_analyses'],
                'causal_analyses': self.reasoning_stats['causal_analyses'],
                'multimodal_analyses': self.reasoning_stats['multimodal_analyses'],
                'schema_updates': self.reasoning_stats['schema_updates']
            },
            'engine_stats': {
                'logical_engine': self.logical_engine.get_inference_statistics(),
                'temporal_engine': self.temporal_engine.get_temporal_statistics(),
                'causal_network': self.causal_network.get_causal_statistics(),
                'multimodal_processor': self.multimodal_processor.get_processing_statistics()
            },
            'cache_stats': {
                'cached_results': len(self.reasoning_cache),
                'cache_hit_rate': 0.0  # Would need to track hits/misses for this
            },
            'schema_stats': {
                'total_schemas': len(self.cognitive_schemas),
                'schema_names': list(self.cognitive_schemas.keys())
            }
        }
    
    def get_cognitive_schema(self, schema_name: str) -> Optional[Dict[str, Any]]:
        """Get a specific cognitive schema"""
        return self.cognitive_schemas.get(schema_name)
    
    def update_cognitive_schema(self, schema_name: str, schema_data: Dict[str, Any]) -> bool:
        """Update a cognitive schema"""
        try:
            if schema_name in self.cognitive_schemas:
                self.cognitive_schemas[schema_name].update(schema_data)
            else:
                self.cognitive_schemas[schema_name] = schema_data
            
            logger.info(f"Updated cognitive schema: {schema_name}")
            return True
            
        except Exception as e:
            logger.error(f"Error updating cognitive schema {schema_name}: {e}")
            return False
    
    def clear_reasoning_data(self):
        """Clear all reasoning data and caches"""
        self.reasoning_cache.clear()
        self.cross_engine_patterns.clear()
        
        # Clear component engines
        self.logical_engine.clear_knowledge_base()
        self.temporal_engine.clear_temporal_data()
        self.causal_network.clear_causal_network()
        self.multimodal_processor.clear_processing_data()
        
        logger.info("Advanced reasoning data cleared")
    
    def export_reasoning_state(self) -> Dict[str, Any]:
        """Export complete reasoning state"""
        return {
            'cognitive_schemas': self.cognitive_schemas,
            'cross_engine_patterns': self.cross_engine_patterns,
            'reasoning_statistics': self.get_reasoning_statistics(),
            'component_exports': {
                'logical_knowledge': self.logical_engine.export_knowledge_base(),
                'temporal_timeline': self.temporal_engine.export_timeline(),
                'causal_network': self.causal_network.export_causal_network()
            }
        }