"""
Scheme-based Cognitive Grammar Adapters

This module implements microservices for agentic grammar translation between
ko6ml primitives and AtomSpace hypergraph patterns using Scheme.
"""

import json
import logging
from typing import Dict, Any, List, Optional, Tuple
import re
import asyncio
from dataclasses import dataclass, field
from enum import Enum
import time

logger = logging.getLogger(__name__)


class SchemeType(Enum):
    """Types of Scheme expressions"""
    ATOM = "atom"
    LIST = "list"
    SYMBOL = "symbol"
    NUMBER = "number"
    STRING = "string"
    CONCEPT = "concept"
    PREDICATE = "predicate"
    IMPLICATION = "implication"


@dataclass
class SchemeExpression:
    """Represents a Scheme expression for cognitive grammar"""
    type: SchemeType
    value: Any
    children: List['SchemeExpression'] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def __str__(self) -> str:
        """Convert to Scheme representation"""
        if self.type == SchemeType.ATOM:
            return str(self.value)
        elif self.type == SchemeType.SYMBOL:
            return f"'{self.value}"
        elif self.type == SchemeType.STRING:
            return f'"{self.value}"'
        elif self.type == SchemeType.LIST:
            children_str = " ".join(str(child) for child in self.children)
            return f"({children_str})"
        elif self.type == SchemeType.CONCEPT:
            return f"(ConceptNode \"{self.value}\")"
        elif self.type == SchemeType.PREDICATE:
            return f"(PredicateNode \"{self.value}\")"
        elif self.type == SchemeType.IMPLICATION:
            if len(self.children) == 2:
                return f"(ImplicationLink {self.children[0]} {self.children[1]})"
        return str(self.value)
    
    def to_atomspace_pattern(self) -> str:
        """Convert to AtomSpace hypergraph pattern"""
        if self.type == SchemeType.CONCEPT:
            return f"(ConceptNode \"{self.value}\")"
        elif self.type == SchemeType.PREDICATE:
            return f"(PredicateNode \"{self.value}\")"
        elif self.type == SchemeType.IMPLICATION:
            if len(self.children) == 2:
                antecedent = self.children[0].to_atomspace_pattern()
                consequent = self.children[1].to_atomspace_pattern()
                return f"(ImplicationLink {antecedent} {consequent})"
        elif self.type == SchemeType.LIST:
            children_patterns = [child.to_atomspace_pattern() for child in self.children]
            return f"(ListLink {' '.join(children_patterns)})"
        return str(self)


class GrammarPattern:
    """Represents a cognitive grammar pattern"""
    
    def __init__(self, name: str, pattern: str, confidence: float = 1.0):
        self.name = name
        self.pattern = pattern
        self.confidence = confidence
        self.scheme_expr = None
        self.atomspace_pattern = None
        self.created_at = time.time()
    
    def parse_to_scheme(self) -> SchemeExpression:
        """Parse pattern to Scheme expression"""
        # Simple parser for demonstration - in real implementation would use proper Scheme parser
        if self.pattern.startswith("(") and self.pattern.endswith(")"):
            # Extract function and arguments
            inner = self.pattern[1:-1].strip()
            parts = self._tokenize(inner)
            
            if not parts:
                return SchemeExpression(SchemeType.LIST, [])
            
            func_name = parts[0]
            args = parts[1:]
            
            # Create appropriate Scheme expression based on function
            if func_name == "ConceptNode":
                return SchemeExpression(SchemeType.CONCEPT, args[0].strip('"') if args else "")
            elif func_name == "PredicateNode":
                return SchemeExpression(SchemeType.PREDICATE, args[0].strip('"') if args else "")
            elif func_name == "ImplicationLink":
                expr = SchemeExpression(SchemeType.IMPLICATION, func_name)
                for arg in args:
                    child_pattern = GrammarPattern("temp", arg)
                    expr.children.append(child_pattern.parse_to_scheme())
                return expr
            else:
                # Generic list
                expr = SchemeExpression(SchemeType.LIST, func_name)
                for arg in args:
                    if arg.startswith('"') and arg.endswith('"'):
                        expr.children.append(SchemeExpression(SchemeType.STRING, arg.strip('"')))
                    elif arg.isdigit():
                        expr.children.append(SchemeExpression(SchemeType.NUMBER, int(arg)))
                    else:
                        expr.children.append(SchemeExpression(SchemeType.SYMBOL, arg))
                return expr
        else:
            # Simple atom
            return SchemeExpression(SchemeType.ATOM, self.pattern)
    
    def _tokenize(self, text: str) -> List[str]:
        """Simple tokenizer for Scheme expressions"""
        tokens = []
        current = ""
        in_string = False
        paren_depth = 0
        
        for char in text:
            if char == '"' and not in_string:
                in_string = True
                current += char
            elif char == '"' and in_string:
                in_string = False
                current += char
            elif char == '(' and not in_string:
                if current.strip():
                    tokens.append(current.strip())
                    current = ""
                current += char
                paren_depth += 1
            elif char == ')' and not in_string:
                current += char
                paren_depth -= 1
                if paren_depth == 0:
                    tokens.append(current.strip())
                    current = ""
            elif char == ' ' and not in_string and paren_depth == 0:
                if current.strip():
                    tokens.append(current.strip())
                    current = ""
            else:
                current += char
        
        if current.strip():
            tokens.append(current.strip())
        
        return tokens
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary representation"""
        return {
            'name': self.name,
            'pattern': self.pattern,
            'confidence': self.confidence,
            'scheme_expr': str(self.scheme_expr) if self.scheme_expr else None,
            'atomspace_pattern': self.atomspace_pattern,
            'created_at': self.created_at
        }


class SchemeGrammarAdapter:
    """Adapter for translating between ko6ml primitives and AtomSpace patterns"""
    
    def __init__(self):
        self.patterns: Dict[str, GrammarPattern] = {}
        self.transformations: Dict[str, Dict[str, Any]] = {}
        self.active_translations: Dict[str, Any] = {}
        
    def register_pattern(self, name: str, pattern: str, confidence: float = 1.0) -> str:
        """Register a new grammar pattern"""
        grammar_pattern = GrammarPattern(name, pattern, confidence)
        grammar_pattern.scheme_expr = grammar_pattern.parse_to_scheme()
        grammar_pattern.atomspace_pattern = grammar_pattern.scheme_expr.to_atomspace_pattern()
        
        self.patterns[name] = grammar_pattern
        logger.info(f"Registered grammar pattern: {name}")
        return name
    
    def translate_kobold_to_atomspace(self, kobold_text: str) -> List[str]:
        """Translate KoboldAI text to AtomSpace patterns"""
        atomspace_patterns = []
        
        # Extract concepts (nouns)
        concepts = re.findall(r'\b[A-Z][a-z]+\b', kobold_text)
        for concept in concepts:
            pattern = f"(ConceptNode \"{concept}\")"
            atomspace_patterns.append(pattern)
        
        # Extract predicates (verbs)
        predicates = re.findall(r'\b[a-z]+ed\b|\b[a-z]+ing\b|\b[a-z]+s\b', kobold_text)
        for predicate in predicates:
            pattern = f"(PredicateNode \"{predicate}\")"
            atomspace_patterns.append(pattern)
        
        # Create relationships
        sentences = re.split(r'[.!?]', kobold_text)
        for sentence in sentences:
            if sentence.strip():
                words = sentence.strip().split()
                if len(words) >= 2:
                    # Simple subject-predicate relationship
                    subject = words[0]
                    predicate = words[1] if len(words) > 1 else "exists"
                    
                    relationship = f"(EvaluationLink (PredicateNode \"{predicate}\") (ConceptNode \"{subject}\"))"
                    atomspace_patterns.append(relationship)
        
        return atomspace_patterns
    
    def translate_atomspace_to_kobold(self, atomspace_patterns: List[str]) -> str:
        """Translate AtomSpace patterns to KoboldAI-compatible text"""
        concepts = []
        predicates = []
        relationships = []
        
        for pattern in atomspace_patterns:
            if "ConceptNode" in pattern:
                concept_match = re.search(r'ConceptNode "([^"]+)"', pattern)
                if concept_match:
                    concepts.append(concept_match.group(1))
            elif "PredicateNode" in pattern:
                predicate_match = re.search(r'PredicateNode "([^"]+)"', pattern)
                if predicate_match:
                    predicates.append(predicate_match.group(1))
            elif "EvaluationLink" in pattern:
                relationships.append(pattern)
        
        # Generate text from patterns
        text_parts = []
        
        # Add concepts
        if concepts:
            text_parts.append(f"The concepts involved are: {', '.join(concepts)}.")
        
        # Add predicates
        if predicates:
            text_parts.append(f"The actions include: {', '.join(predicates)}.")
        
        # Generate simple sentences from relationships
        for relationship in relationships:
            # Extract subject and predicate from EvaluationLink
            concept_match = re.search(r'ConceptNode "([^"]+)"', relationship)
            predicate_match = re.search(r'PredicateNode "([^"]+)"', relationship)
            
            if concept_match and predicate_match:
                subject = concept_match.group(1)
                predicate = predicate_match.group(1)
                text_parts.append(f"{subject} {predicate}.")
        
        return " ".join(text_parts)
    
    def create_implication_pattern(self, antecedent: str, consequent: str) -> str:
        """Create an implication pattern"""
        pattern_name = f"implication_{len(self.patterns)}"
        antecedent_patterns = self.translate_kobold_to_atomspace(antecedent)
        consequent_patterns = self.translate_kobold_to_atomspace(consequent)
        
        if antecedent_patterns and consequent_patterns:
            # Create implication link
            implication = f"(ImplicationLink (AndLink {' '.join(antecedent_patterns)}) (AndLink {' '.join(consequent_patterns)}))"
            self.register_pattern(pattern_name, implication, 0.8)
            return pattern_name
        
        return ""
    
    def get_pattern_statistics(self) -> Dict[str, Any]:
        """Get statistics about registered patterns"""
        total_patterns = len(self.patterns)
        concept_patterns = sum(1 for p in self.patterns.values() if "ConceptNode" in p.pattern)
        predicate_patterns = sum(1 for p in self.patterns.values() if "PredicateNode" in p.pattern)
        implication_patterns = sum(1 for p in self.patterns.values() if "ImplicationLink" in p.pattern)
        
        return {
            'total_patterns': total_patterns,
            'concept_patterns': concept_patterns,
            'predicate_patterns': predicate_patterns,
            'implication_patterns': implication_patterns,
            'average_confidence': sum(p.confidence for p in self.patterns.values()) / total_patterns if total_patterns > 0 else 0,
            'patterns': {name: pattern.to_dict() for name, pattern in self.patterns.items()}
        }
    
    async def process_translation_batch(self, texts: List[str]) -> List[Dict[str, Any]]:
        """Process a batch of translations asynchronously"""
        results = []
        
        for text in texts:
            atomspace_patterns = self.translate_kobold_to_atomspace(text)
            back_translation = self.translate_atomspace_to_kobold(atomspace_patterns)
            
            result = {
                'original_text': text,
                'atomspace_patterns': atomspace_patterns,
                'back_translation': back_translation,
                'pattern_count': len(atomspace_patterns),
                'timestamp': time.time()
            }
            results.append(result)
            
            # Simulate processing time
            await asyncio.sleep(0.01)
        
        return results


# Global scheme adapter instance
scheme_adapter = SchemeGrammarAdapter()

# Register some default patterns
scheme_adapter.register_pattern("basic_concept", "(ConceptNode \"Entity\")", 1.0)
scheme_adapter.register_pattern("basic_predicate", "(PredicateNode \"exists\")", 1.0)
scheme_adapter.register_pattern("basic_relationship", "(EvaluationLink (PredicateNode \"relates_to\") (ListLink (ConceptNode \"X\") (ConceptNode \"Y\")))", 0.9)