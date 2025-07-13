"""
Logical Inference Engine using AtomSpace

This module provides formal logical reasoning capabilities for the cognitive architecture,
using AtomSpace patterns to represent knowledge and inference rules.
"""

import logging
import time
from typing import Dict, List, Set, Any, Optional, Tuple
from dataclasses import dataclass, field
from enum import Enum
import re

logger = logging.getLogger(__name__)


class InferenceType(Enum):
    """Types of logical inference"""
    MODUS_PONENS = "modus_ponens"           # If A implies B and A, then B
    MODUS_TOLLENS = "modus_tollens"         # If A implies B and not B, then not A
    UNIVERSAL_INSTANTIATION = "universal_instantiation"  # If for all X, P(X), then P(a)
    DEDUCTION = "deduction"                 # General deductive reasoning
    INDUCTION = "induction"                 # Pattern-based inductive reasoning
    ABDUCTION = "abduction"                 # Best explanation reasoning
    CONJUNCTION = "conjunction"             # A and B
    DISJUNCTION = "disjunction"             # A or B
    SYLLOGISM = "syllogism"                 # A->B, B->C, therefore A->C


@dataclass
class InferenceRule:
    """Represents a logical inference rule"""
    rule_id: str
    rule_type: InferenceType
    premises: List[str]  # AtomSpace patterns as premises
    conclusion: str      # AtomSpace pattern as conclusion
    confidence: float = 1.0
    priority: int = 5
    application_count: int = 0
    
    def __post_init__(self):
        """Validate the inference rule"""
        if not self.premises:
            raise ValueError("Inference rule must have at least one premise")
        if not self.conclusion:
            raise ValueError("Inference rule must have a conclusion")
        if not 0 <= self.confidence <= 1:
            raise ValueError("Confidence must be between 0 and 1")


@dataclass
class InferenceResult:
    """Result of an inference operation"""
    new_knowledge: List[str]  # New AtomSpace patterns derived
    applied_rules: List[str]  # IDs of rules that were applied
    confidence_scores: List[float]  # Confidence for each new pattern
    reasoning_path: List[Dict[str, Any]]  # Step-by-step reasoning trace
    processing_time: float = 0.0
    
    def __post_init__(self):
        """Ensure consistent lengths"""
        if len(self.new_knowledge) != len(self.confidence_scores):
            # Pad with default confidence if needed
            while len(self.confidence_scores) < len(self.new_knowledge):
                self.confidence_scores.append(0.8)


class LogicalInferenceEngine:
    """
    Advanced logical inference engine using AtomSpace patterns
    
    Provides formal reasoning capabilities for story generation and plot development.
    """
    
    def __init__(self):
        self.inference_rules: Dict[str, InferenceRule] = {}
        self.knowledge_base: Set[str] = set()  # Known AtomSpace patterns
        self.confidence_threshold = 0.3
        self.max_inference_depth = 5
        self.inference_stats = {
            'total_inferences': 0,
            'successful_inferences': 0,
            'rules_applied': {},
            'knowledge_base_size': 0,
            'start_time': time.time()
        }
        
        # Initialize with default inference rules for story reasoning
        self._initialize_default_rules()
    
    def _initialize_default_rules(self):
        """Initialize with common inference rules for narrative reasoning"""
        
        # Character-based inference rules
        self.add_inference_rule(InferenceRule(
            rule_id="character_action_motive",
            rule_type=InferenceType.MODUS_PONENS,
            premises=[
                "(EvaluationLink (PredicateNode \"has_motive\") (ListLink (ConceptNode \"$CHARACTER\") (ConceptNode \"$MOTIVE\")))",
                "(EvaluationLink (PredicateNode \"enables_action\") (ListLink (ConceptNode \"$MOTIVE\") (ConceptNode \"$ACTION\")))"
            ],
            conclusion="(EvaluationLink (PredicateNode \"likely_to_perform\") (ListLink (ConceptNode \"$CHARACTER\") (ConceptNode \"$ACTION\")))",
            confidence=0.85,
            priority=8
        ))
        
        # Plot development rules
        self.add_inference_rule(InferenceRule(
            rule_id="conflict_resolution",
            rule_type=InferenceType.DEDUCTION,
            premises=[
                "(EvaluationLink (PredicateNode \"has_conflict\") (ListLink (ConceptNode \"$CHARACTER1\") (ConceptNode \"$CHARACTER2\")))",
                "(EvaluationLink (PredicateNode \"shares_goal\") (ListLink (ConceptNode \"$CHARACTER1\") (ConceptNode \"$CHARACTER2\")))"
            ],
            conclusion="(EvaluationLink (PredicateNode \"potential_alliance\") (ListLink (ConceptNode \"$CHARACTER1\") (ConceptNode \"$CHARACTER2\")))",
            confidence=0.75,
            priority=6
        ))
        
        # World-building inference
        self.add_inference_rule(InferenceRule(
            rule_id="location_accessibility",
            rule_type=InferenceType.UNIVERSAL_INSTANTIATION,
            premises=[
                "(EvaluationLink (PredicateNode \"located_in\") (ListLink (ConceptNode \"$ENTITY\") (ConceptNode \"$LOCATION\")))",
                "(EvaluationLink (PredicateNode \"accessible_from\") (ListLink (ConceptNode \"$LOCATION\") (ConceptNode \"$OTHER_LOCATION\")))"
            ],
            conclusion="(EvaluationLink (PredicateNode \"can_reach\") (ListLink (ConceptNode \"$ENTITY\") (ConceptNode \"$OTHER_LOCATION\")))",
            confidence=0.9,
            priority=7
        ))
        
        # Narrative consistency rules
        self.add_inference_rule(InferenceRule(
            rule_id="temporal_consistency",
            rule_type=InferenceType.MODUS_TOLLENS,
            premises=[
                "(EvaluationLink (PredicateNode \"happens_before\") (ListLink (ConceptNode \"$EVENT1\") (ConceptNode \"$EVENT2\")))",
                "(EvaluationLink (PredicateNode \"prevents\") (ListLink (ConceptNode \"$EVENT1\") (ConceptNode \"$EVENT2\")))"
            ],
            conclusion="(EvaluationLink (PredicateNode \"cannot_occur\") (ListLink (ConceptNode \"$EVENT2\")))",
            confidence=0.95,
            priority=9
        ))
        
        logger.info(f"Initialized logical inference engine with {len(self.inference_rules)} default rules")
    
    def add_inference_rule(self, rule: InferenceRule) -> bool:
        """Add a new inference rule to the engine"""
        try:
            if rule.rule_id in self.inference_rules:
                logger.warning(f"Inference rule {rule.rule_id} already exists, replacing")
            
            self.inference_rules[rule.rule_id] = rule
            self.inference_stats['rules_applied'][rule.rule_id] = 0
            
            logger.info(f"Added inference rule: {rule.rule_id} ({rule.rule_type.value})")
            return True
            
        except Exception as e:
            logger.error(f"Error adding inference rule {rule.rule_id}: {e}")
            return False
    
    def add_knowledge(self, atomspace_patterns: List[str]) -> int:
        """Add knowledge patterns to the knowledge base"""
        added_count = 0
        
        for pattern in atomspace_patterns:
            if pattern and pattern not in self.knowledge_base:
                self.knowledge_base.add(pattern)
                added_count += 1
        
        self.inference_stats['knowledge_base_size'] = len(self.knowledge_base)
        
        if added_count > 0:
            logger.info(f"Added {added_count} new knowledge patterns to knowledge base")
        
        return added_count
    
    def infer_new_knowledge(self, context_patterns: Optional[List[str]] = None) -> InferenceResult:
        """
        Perform logical inference to derive new knowledge
        
        Args:
            context_patterns: Additional patterns to consider for this inference cycle
        
        Returns:
            InferenceResult with newly derived knowledge
        """
        start_time = time.time()
        
        # Combine knowledge base with context patterns
        working_set = self.knowledge_base.copy()
        if context_patterns:
            working_set.update(context_patterns)
        
        new_knowledge = []
        applied_rules = []
        confidence_scores = []
        reasoning_path = []
        
        # Sort rules by priority (higher priority first)
        sorted_rules = sorted(self.inference_rules.values(), 
                            key=lambda r: r.priority, reverse=True)
        
        for rule in sorted_rules:
            try:
                # Attempt to apply the rule
                matches = self._find_rule_matches(rule, working_set)
                
                for match in matches:
                    new_pattern = self._apply_rule_match(rule, match)
                    
                    if new_pattern and new_pattern not in working_set:
                        new_knowledge.append(new_pattern)
                        applied_rules.append(rule.rule_id)
                        confidence_scores.append(rule.confidence)
                        
                        # Update working set with new knowledge
                        working_set.add(new_pattern)
                        
                        # Track reasoning step
                        reasoning_path.append({
                            'rule_id': rule.rule_id,
                            'rule_type': rule.rule_type.value,
                            'premises_matched': match['premises'],
                            'conclusion': new_pattern,
                            'confidence': rule.confidence
                        })
                        
                        # Update rule application count
                        rule.application_count += 1
                        self.inference_stats['rules_applied'][rule.rule_id] += 1
                        
                        logger.debug(f"Applied rule {rule.rule_id}: {new_pattern}")
                        
                        # Limit inference to prevent infinite loops
                        if len(new_knowledge) >= 20:  # Max new patterns per cycle
                            break
                
                if len(new_knowledge) >= 20:
                    break
                    
            except Exception as e:
                logger.error(f"Error applying inference rule {rule.rule_id}: {e}")
        
        # Update statistics
        self.inference_stats['total_inferences'] += 1
        if new_knowledge:
            self.inference_stats['successful_inferences'] += 1
        
        # Add new knowledge to knowledge base
        self.knowledge_base.update(new_knowledge)
        self.inference_stats['knowledge_base_size'] = len(self.knowledge_base)
        
        processing_time = time.time() - start_time
        
        result = InferenceResult(
            new_knowledge=new_knowledge,
            applied_rules=applied_rules,
            confidence_scores=confidence_scores,
            reasoning_path=reasoning_path,
            processing_time=processing_time
        )
        
        if new_knowledge:
            logger.info(f"Inference cycle derived {len(new_knowledge)} new patterns using {len(set(applied_rules))} rules")
        
        return result
    
    def _find_rule_matches(self, rule: InferenceRule, knowledge_set: Set[str]) -> List[Dict[str, Any]]:
        """Find all possible matches for a rule's premises in the knowledge set"""
        matches = []
        
        try:
            # Extract variables from premises
            variables = self._extract_variables(rule.premises)
            
            if not variables:
                # No variables, try direct pattern matching
                if all(premise in knowledge_set for premise in rule.premises):
                    matches.append({'premises': rule.premises, 'bindings': {}})
            else:
                # Find variable bindings that satisfy all premises
                variable_bindings = self._find_variable_bindings(rule.premises, knowledge_set, variables)
                
                for binding in variable_bindings:
                    bound_premises = self._bind_variables(rule.premises, binding)
                    if all(premise in knowledge_set for premise in bound_premises):
                        matches.append({'premises': bound_premises, 'bindings': binding})
        
        except Exception as e:
            logger.error(f"Error finding rule matches for {rule.rule_id}: {e}")
        
        return matches
    
    def _extract_variables(self, patterns: List[str]) -> Set[str]:
        """Extract variables (starting with $) from AtomSpace patterns"""
        variables = set()
        
        for pattern in patterns:
            # Find all $VARIABLE tokens
            vars_in_pattern = re.findall(r'\$[A-Z_][A-Z0-9_]*', pattern)
            variables.update(vars_in_pattern)
        
        return variables
    
    def _find_variable_bindings(self, premises: List[str], knowledge_set: Set[str], 
                              variables: Set[str]) -> List[Dict[str, str]]:
        """Find all possible variable bindings that satisfy the premises"""
        if not variables:
            return [{}]
        
        # For now, implement a simple binding strategy
        # In a more sophisticated version, this would use proper unification
        bindings = []
        
        # Extract potential values for each variable from the knowledge base
        variable_candidates = {var: set() for var in variables}
        
        for pattern in knowledge_set:
            for premise in premises:
                candidates = self._extract_binding_candidates(premise, pattern, variables)
                for var, value in candidates.items():
                    if value:
                        variable_candidates[var].add(value)
        
        # Generate all possible combinations (limited to prevent explosion)
        bindings = self._generate_binding_combinations(variable_candidates)
        
        # Limit to first 10 bindings to prevent performance issues
        return bindings[:10]
    
    def _extract_binding_candidates(self, premise_pattern: str, knowledge_pattern: str, 
                                  variables: Set[str]) -> Dict[str, str]:
        """Extract potential variable bindings from a knowledge pattern"""
        candidates = {}
        
        # Simple pattern matching - replace with proper unification in production
        for var in variables:
            if var in premise_pattern:
                # Try to find what this variable could bind to
                # This is a simplified approach
                if "ConceptNode" in premise_pattern and "ConceptNode" in knowledge_pattern:
                    # Extract concept node values
                    premise_concepts = re.findall(r'ConceptNode "([^"]*)"', premise_pattern)
                    knowledge_concepts = re.findall(r'ConceptNode "([^"]*)"', knowledge_pattern)
                    
                    if premise_concepts and knowledge_concepts:
                        for p_concept in premise_concepts:
                            if var in p_concept:
                                for k_concept in knowledge_concepts:
                                    candidates[var] = k_concept
                                    break
        
        return candidates
    
    def _generate_binding_combinations(self, variable_candidates: Dict[str, Set[str]]) -> List[Dict[str, str]]:
        """Generate all possible combinations of variable bindings"""
        if not variable_candidates:
            return [{}]
        
        # Simple Cartesian product (limited)
        variables = list(variable_candidates.keys())
        if not variables:
            return [{}]
        
        # Start with first variable
        first_var = variables[0]
        first_candidates = list(variable_candidates[first_var])[:5]  # Limit candidates
        
        if len(variables) == 1:
            return [{first_var: candidate} for candidate in first_candidates]
        
        # Recursively combine with remaining variables
        remaining_candidates = {var: variable_candidates[var] for var in variables[1:]}
        remaining_combinations = self._generate_binding_combinations(remaining_candidates)
        
        combinations = []
        for candidate in first_candidates:
            for remaining_combo in remaining_combinations:
                combo = {first_var: candidate}
                combo.update(remaining_combo)
                combinations.append(combo)
                
                # Limit total combinations
                if len(combinations) >= 20:
                    break
            if len(combinations) >= 20:
                break
        
        return combinations
    
    def _bind_variables(self, patterns: List[str], bindings: Dict[str, str]) -> List[str]:
        """Replace variables in patterns with their bindings"""
        bound_patterns = []
        
        for pattern in patterns:
            bound_pattern = pattern
            for var, value in bindings.items():
                bound_pattern = bound_pattern.replace(var, f'"{value}"')
            bound_patterns.append(bound_pattern)
        
        return bound_patterns
    
    def _apply_rule_match(self, rule: InferenceRule, match: Dict[str, Any]) -> Optional[str]:
        """Apply a rule match to generate a conclusion"""
        try:
            bindings = match['bindings']
            
            # Apply bindings to conclusion
            conclusion = rule.conclusion
            for var, value in bindings.items():
                conclusion = conclusion.replace(var, f'"{value}"')
            
            return conclusion
            
        except Exception as e:
            logger.error(f"Error applying rule match: {e}")
            return None
    
    def reason_about_narrative(self, story_elements: Dict[str, Any]) -> Dict[str, Any]:
        """
        Apply logical reasoning to narrative elements
        
        Args:
            story_elements: Dictionary containing characters, events, locations, etc.
        
        Returns:
            Dictionary with reasoning results and new narrative implications
        """
        try:
            # Convert story elements to AtomSpace patterns
            narrative_patterns = self._story_elements_to_patterns(story_elements)
            
            # Add to knowledge base temporarily
            original_kb_size = len(self.knowledge_base)
            self.add_knowledge(narrative_patterns)
            
            # Perform inference
            inference_result = self.infer_new_knowledge()
            
            # Interpret results back to narrative terms
            narrative_implications = self._patterns_to_narrative_implications(
                inference_result.new_knowledge
            )
            
            return {
                'narrative_implications': narrative_implications,
                'reasoning_confidence': sum(inference_result.confidence_scores) / len(inference_result.confidence_scores) if inference_result.confidence_scores else 0,
                'patterns_derived': len(inference_result.new_knowledge),
                'rules_applied': len(set(inference_result.applied_rules)),
                'reasoning_path': inference_result.reasoning_path,
                'processing_time': inference_result.processing_time
            }
            
        except Exception as e:
            logger.error(f"Error in narrative reasoning: {e}")
            return {'error': str(e)}
    
    def _story_elements_to_patterns(self, story_elements: Dict[str, Any]) -> List[str]:
        """Convert story elements to AtomSpace patterns"""
        patterns = []
        
        # Handle characters
        if 'characters' in story_elements:
            for character in story_elements['characters']:
                if isinstance(character, dict):
                    name = character.get('name', 'Unknown')
                    patterns.append(f'(ConceptNode "{name}")')
                    
                    if 'traits' in character:
                        for trait in character['traits']:
                            patterns.append(f'(EvaluationLink (PredicateNode "has_trait") (ListLink (ConceptNode "{name}") (ConceptNode "{trait}")))')
                    
                    if 'location' in character:
                        patterns.append(f'(EvaluationLink (PredicateNode "located_in") (ListLink (ConceptNode "{name}") (ConceptNode "{character["location"]}")))')
                elif isinstance(character, str):
                    patterns.append(f'(ConceptNode "{character}")')
        
        # Handle events
        if 'events' in story_elements:
            for event in story_elements['events']:
                if isinstance(event, dict):
                    event_name = event.get('name', 'Unknown Event')
                    patterns.append(f'(ConceptNode "{event_name}")')
                    
                    if 'participants' in event:
                        for participant in event['participants']:
                            patterns.append(f'(EvaluationLink (PredicateNode "participates_in") (ListLink (ConceptNode "{participant}") (ConceptNode "{event_name}")))')
                elif isinstance(event, str):
                    patterns.append(f'(ConceptNode "{event}")')
        
        # Handle locations
        if 'locations' in story_elements:
            for location in story_elements['locations']:
                if isinstance(location, str):
                    patterns.append(f'(ConceptNode "{location}")')
        
        return patterns
    
    def _patterns_to_narrative_implications(self, patterns: List[str]) -> List[Dict[str, Any]]:
        """Convert AtomSpace patterns back to narrative implications"""
        implications = []
        
        for pattern in patterns:
            try:
                implication = {'pattern': pattern, 'type': 'general'}
                
                # Detect different types of implications
                if 'likely_to_perform' in pattern:
                    implication['type'] = 'character_action'
                    implication['description'] = self._extract_character_action_description(pattern)
                elif 'potential_alliance' in pattern:
                    implication['type'] = 'relationship'
                    implication['description'] = self._extract_relationship_description(pattern)
                elif 'can_reach' in pattern:
                    implication['type'] = 'accessibility'
                    implication['description'] = self._extract_accessibility_description(pattern)
                elif 'cannot_occur' in pattern:
                    implication['type'] = 'constraint'
                    implication['description'] = self._extract_constraint_description(pattern)
                else:
                    implication['description'] = f"New logical relationship derived: {pattern[:100]}..."
                
                implications.append(implication)
                
            except Exception as e:
                logger.error(f"Error interpreting pattern {pattern}: {e}")
        
        return implications
    
    def _extract_character_action_description(self, pattern: str) -> str:
        """Extract character action description from pattern"""
        try:
            # Simple extraction - could be made more sophisticated
            concepts = re.findall(r'ConceptNode "([^"]*)"', pattern)
            if len(concepts) >= 2:
                return f"{concepts[0]} is likely to {concepts[1]}"
            return "Character action potential identified"
        except:
            return "Character action relationship derived"
    
    def _extract_relationship_description(self, pattern: str) -> str:
        """Extract relationship description from pattern"""
        try:
            concepts = re.findall(r'ConceptNode "([^"]*)"', pattern)
            if len(concepts) >= 2:
                return f"Potential alliance between {concepts[0]} and {concepts[1]}"
            return "Character relationship potential identified"
        except:
            return "Character relationship derived"
    
    def _extract_accessibility_description(self, pattern: str) -> str:
        """Extract accessibility description from pattern"""
        try:
            concepts = re.findall(r'ConceptNode "([^"]*)"', pattern)
            if len(concepts) >= 2:
                return f"{concepts[0]} can reach {concepts[1]}"
            return "Location accessibility identified"
        except:
            return "Accessibility relationship derived"
    
    def _extract_constraint_description(self, pattern: str) -> str:
        """Extract constraint description from pattern"""
        try:
            concepts = re.findall(r'ConceptNode "([^"]*)"', pattern)
            if len(concepts) >= 1:
                return f"{concepts[0]} cannot occur under current conditions"
            return "Narrative constraint identified"
        except:
            return "Constraint relationship derived"
    
    def get_inference_statistics(self) -> Dict[str, Any]:
        """Get statistics about inference engine performance"""
        uptime = time.time() - self.inference_stats['start_time']
        
        return {
            'total_inferences': self.inference_stats['total_inferences'],
            'successful_inferences': self.inference_stats['successful_inferences'],
            'success_rate': (self.inference_stats['successful_inferences'] / 
                           max(1, self.inference_stats['total_inferences'])),
            'knowledge_base_size': self.inference_stats['knowledge_base_size'],
            'total_rules': len(self.inference_rules),
            'rule_usage': dict(self.inference_stats['rules_applied']),
            'uptime_seconds': uptime,
            'confidence_threshold': self.confidence_threshold,
            'max_inference_depth': self.max_inference_depth
        }
    
    def clear_knowledge_base(self):
        """Clear the knowledge base (keeping inference rules)"""
        self.knowledge_base.clear()
        self.inference_stats['knowledge_base_size'] = 0
        logger.info("Knowledge base cleared")
    
    def export_knowledge_base(self) -> List[str]:
        """Export current knowledge base patterns"""
        return list(self.knowledge_base)
    
    def import_knowledge_base(self, patterns: List[str]) -> int:
        """Import patterns into knowledge base"""
        return self.add_knowledge(patterns)