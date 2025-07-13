"""
Temporal Reasoning Engine for Story Continuity

This module provides temporal reasoning capabilities for maintaining
story continuity and narrative flow across time.
"""

import logging
import time
from typing import Dict, List, Set, Any, Optional, Tuple
from dataclasses import dataclass, field
from enum import Enum
import re
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


class TemporalRelation(Enum):
    """Types of temporal relationships between events"""
    BEFORE = "before"                # A happens before B
    AFTER = "after"                  # A happens after B
    DURING = "during"                # A happens during B
    OVERLAPS = "overlaps"            # A overlaps with B
    SIMULTANEOUS = "simultaneous"    # A and B happen at the same time
    CAUSES = "causes"                # A temporally causes B
    ENABLES = "enables"              # A temporally enables B
    PREVENTS = "prevents"            # A prevents B from happening
    SEQUENCE = "sequence"            # A and B are in sequence


class TimeFrame(Enum):
    """Time granularity for temporal reasoning"""
    IMMEDIATE = "immediate"          # Within the same scene/moment
    SHORT_TERM = "short_term"        # Within the same chapter/episode
    MEDIUM_TERM = "medium_term"      # Within the same arc/book
    LONG_TERM = "long_term"          # Across multiple arcs/books
    ETERNAL = "eternal"              # Timeless/permanent facts


@dataclass
class TemporalEvent:
    """Represents a temporal event in the story"""
    event_id: str
    description: str
    time_frame: TimeFrame
    timestamp: Optional[float] = None  # Relative timestamp in story
    duration: Optional[float] = None   # Duration of the event
    participants: List[str] = field(default_factory=list)
    location: Optional[str] = None
    preconditions: List[str] = field(default_factory=list)
    effects: List[str] = field(default_factory=list)
    certainty: float = 1.0  # How certain we are this event happens
    
    def __post_init__(self):
        """Set default timestamp if not provided"""
        if self.timestamp is None:
            self.timestamp = time.time()


@dataclass
class TemporalConstraint:
    """Represents a temporal constraint between events"""
    constraint_id: str
    event1_id: str
    event2_id: str
    relation: TemporalRelation
    strength: float = 1.0  # How strong this constraint is (0-1)
    reasoning: Optional[str] = None  # Why this constraint exists
    
    def __post_init__(self):
        """Validate constraint"""
        if not 0 <= self.strength <= 1:
            raise ValueError("Constraint strength must be between 0 and 1")


@dataclass
class TemporalInconsistency:
    """Represents a detected temporal inconsistency"""
    inconsistency_id: str
    conflicting_events: List[str]
    conflicting_constraints: List[str]
    description: str
    severity: float = 1.0  # How severe this inconsistency is
    suggested_resolution: Optional[str] = None


class TemporalReasoningEngine:
    """
    Temporal reasoning engine for story continuity
    
    Maintains temporal relationships between story events and detects
    inconsistencies that could break narrative flow.
    """
    
    def __init__(self):
        self.events: Dict[str, TemporalEvent] = {}
        self.constraints: Dict[str, TemporalConstraint] = {}
        self.timeline: List[str] = []  # Ordered list of event IDs
        self.inconsistencies: List[TemporalInconsistency] = []
        
        self.reasoning_stats = {
            'events_processed': 0,
            'constraints_created': 0,
            'inconsistencies_detected': 0,
            'timeline_updates': 0,
            'start_time': time.time()
        }
        
        # Configuration
        self.max_timeline_length = 1000
        self.inconsistency_threshold = 0.7
        self.auto_resolve_conflicts = True
    
    def add_event(self, event: TemporalEvent) -> bool:
        """Add a new temporal event"""
        try:
            if event.event_id in self.events:
                logger.warning(f"Event {event.event_id} already exists, updating")
            
            self.events[event.event_id] = event
            self._update_timeline()
            self.reasoning_stats['events_processed'] += 1
            
            # Check for new inconsistencies
            self._detect_inconsistencies_for_event(event.event_id)
            
            logger.info(f"Added temporal event: {event.event_id} ({event.time_frame.value})")
            return True
            
        except Exception as e:
            logger.error(f"Error adding temporal event {event.event_id}: {e}")
            return False
    
    def add_constraint(self, constraint: TemporalConstraint) -> bool:
        """Add a temporal constraint between events"""
        try:
            # Verify both events exist
            if constraint.event1_id not in self.events:
                logger.error(f"Event {constraint.event1_id} not found for constraint")
                return False
            
            if constraint.event2_id not in self.events:
                logger.error(f"Event {constraint.event2_id} not found for constraint")
                return False
            
            self.constraints[constraint.constraint_id] = constraint
            self._update_timeline()
            self.reasoning_stats['constraints_created'] += 1
            
            # Check if this constraint creates inconsistencies
            if not self._validate_constraint(constraint):
                self.inconsistencies.append(TemporalInconsistency(
                    inconsistency_id=f"constraint_conflict_{constraint.constraint_id}",
                    conflicting_events=[constraint.event1_id, constraint.event2_id],
                    conflicting_constraints=[constraint.constraint_id],
                    description=f"New constraint {constraint.constraint_id} conflicts with existing timeline",
                    severity=constraint.strength
                ))
                self.reasoning_stats['inconsistencies_detected'] += 1
            
            logger.info(f"Added temporal constraint: {constraint.constraint_id} ({constraint.relation.value})")
            return True
            
        except Exception as e:
            logger.error(f"Error adding temporal constraint {constraint.constraint_id}: {e}")
            return False
    
    def analyze_story_continuity(self, story_events: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Analyze story continuity based on provided events
        
        Args:
            story_events: List of events with temporal information
        
        Returns:
            Analysis results including timeline and inconsistencies
        """
        try:
            # Convert story events to temporal events
            temporal_events = self._convert_story_events(story_events)
            
            # Add events to the system
            for event in temporal_events:
                self.add_event(event)
            
            # Infer temporal constraints
            inferred_constraints = self._infer_temporal_constraints(temporal_events)
            
            # Add inferred constraints
            for constraint in inferred_constraints:
                self.add_constraint(constraint)
            
            # Analyze continuity
            continuity_score = self._calculate_continuity_score()
            
            # Generate timeline
            ordered_timeline = self._generate_ordered_timeline()
            
            # Detect plot holes
            plot_holes = self._detect_plot_holes()
            
            return {
                'continuity_score': continuity_score,
                'timeline': ordered_timeline,
                'inconsistencies': [self._inconsistency_to_dict(inc) for inc in self.inconsistencies],
                'plot_holes': plot_holes,
                'temporal_patterns': self._analyze_temporal_patterns(),
                'story_pacing': self._analyze_story_pacing(),
                'events_analyzed': len(temporal_events),
                'constraints_inferred': len(inferred_constraints)
            }
            
        except Exception as e:
            logger.error(f"Error analyzing story continuity: {e}")
            return {'error': str(e)}
    
    def _convert_story_events(self, story_events: List[Dict[str, Any]]) -> List[TemporalEvent]:
        """Convert story events to temporal events"""
        temporal_events = []
        
        for i, event_data in enumerate(story_events):
            try:
                event = TemporalEvent(
                    event_id=event_data.get('id', f"event_{i}"),
                    description=event_data.get('description', event_data.get('text', f"Event {i}")),
                    time_frame=self._determine_time_frame(event_data),
                    timestamp=event_data.get('timestamp', time.time() + i),
                    duration=event_data.get('duration'),
                    participants=event_data.get('participants', event_data.get('characters', [])),
                    location=event_data.get('location'),
                    preconditions=event_data.get('preconditions', []),
                    effects=event_data.get('effects', []),
                    certainty=event_data.get('certainty', 1.0)
                )
                temporal_events.append(event)
                
            except Exception as e:
                logger.error(f"Error converting story event {i}: {e}")
        
        return temporal_events
    
    def _determine_time_frame(self, event_data: Dict[str, Any]) -> TimeFrame:
        """Determine appropriate time frame for an event"""
        if 'time_frame' in event_data:
            try:
                return TimeFrame(event_data['time_frame'])
            except ValueError:
                pass
        
        # Heuristic determination based on event characteristics
        description = event_data.get('description', '').lower()
        
        if any(word in description for word in ['suddenly', 'immediately', 'instantly', 'now']):
            return TimeFrame.IMMEDIATE
        elif any(word in description for word in ['later', 'soon', 'shortly', 'next']):
            return TimeFrame.SHORT_TERM
        elif any(word in description for word in ['eventually', 'after', 'following']):
            return TimeFrame.MEDIUM_TERM
        elif any(word in description for word in ['years', 'decades', 'generations', 'ages']):
            return TimeFrame.LONG_TERM
        elif any(word in description for word in ['always', 'never', 'eternal', 'forever']):
            return TimeFrame.ETERNAL
        else:
            return TimeFrame.SHORT_TERM  # Default
    
    def _infer_temporal_constraints(self, events: List[TemporalEvent]) -> List[TemporalConstraint]:
        """Infer temporal constraints from events"""
        constraints = []
        
        # Sort events by timestamp
        sorted_events = sorted(events, key=lambda e: e.timestamp or 0)
        
        # Infer sequential relationships
        for i in range(len(sorted_events) - 1):
            event1 = sorted_events[i]
            event2 = sorted_events[i + 1]
            
            # Check if they should be ordered
            if self._should_be_ordered(event1, event2):
                constraint = TemporalConstraint(
                    constraint_id=f"seq_{event1.event_id}_{event2.event_id}",
                    event1_id=event1.event_id,
                    event2_id=event2.event_id,
                    relation=TemporalRelation.BEFORE,
                    strength=0.8,
                    reasoning="Inferred from timestamp order"
                )
                constraints.append(constraint)
        
        # Infer causal relationships
        for event1 in events:
            for event2 in events:
                if event1.event_id != event2.event_id:
                    if self._could_be_causal(event1, event2):
                        constraint = TemporalConstraint(
                            constraint_id=f"causal_{event1.event_id}_{event2.event_id}",
                            event1_id=event1.event_id,
                            event2_id=event2.event_id,
                            relation=TemporalRelation.CAUSES,
                            strength=0.6,
                            reasoning="Inferred causal relationship"
                        )
                        constraints.append(constraint)
        
        # Infer simultaneity for events in same location with same participants
        for i, event1 in enumerate(events):
            for event2 in events[i+1:]:
                if self._could_be_simultaneous(event1, event2):
                    constraint = TemporalConstraint(
                        constraint_id=f"simul_{event1.event_id}_{event2.event_id}",
                        event1_id=event1.event_id,
                        event2_id=event2.event_id,
                        relation=TemporalRelation.SIMULTANEOUS,
                        strength=0.7,
                        reasoning="Same location and participants"
                    )
                    constraints.append(constraint)
        
        return constraints
    
    def _should_be_ordered(self, event1: TemporalEvent, event2: TemporalEvent) -> bool:
        """Check if two events should be temporally ordered"""
        # Check for obvious temporal indicators in descriptions
        time_indicators_before = ['before', 'first', 'initially', 'earlier', 'previously']
        time_indicators_after = ['after', 'then', 'next', 'later', 'subsequently', 'finally']
        
        desc1 = event1.description.lower()
        desc2 = event2.description.lower()
        
        # If event1 has "after" indicators and event2 has "before" indicators, order them
        if any(indicator in desc1 for indicator in time_indicators_after) and \
           any(indicator in desc2 for indicator in time_indicators_before):
            return False  # event2 should come before event1
        
        # Check for effect-cause relationships
        if any(effect in event2.preconditions for effect in event1.effects):
            return True  # event1 enables event2
        
        # Default to timestamp order if available
        if event1.timestamp and event2.timestamp:
            return event1.timestamp < event2.timestamp
        
        return False
    
    def _could_be_causal(self, event1: TemporalEvent, event2: TemporalEvent) -> bool:
        """Check if event1 could causally lead to event2"""
        # Check if event1's effects match event2's preconditions
        if any(effect in event2.preconditions for effect in event1.effects):
            return True
        
        # Check for causal language patterns
        causal_patterns = [
            ('causes', 'because of'),
            ('leads to', 'results in'),
            ('triggers', 'provokes'),
            ('enables', 'allows')
        ]
        
        desc1 = event1.description.lower()
        desc2 = event2.description.lower()
        
        for pattern1, pattern2 in causal_patterns:
            if pattern1 in desc1 or pattern2 in desc2:
                return True
        
        # Check if same participants in sequence suggest causality
        if (set(event1.participants) & set(event2.participants) and
            event1.timestamp and event2.timestamp and
            event2.timestamp > event1.timestamp):
            return True
        
        return False
    
    def _could_be_simultaneous(self, event1: TemporalEvent, event2: TemporalEvent) -> bool:
        """Check if two events could be simultaneous"""
        # Same location and overlapping participants
        if (event1.location == event2.location and 
            event1.location is not None and
            set(event1.participants) & set(event2.participants)):
            return True
        
        # Similar timestamps
        if (event1.timestamp and event2.timestamp and
            abs(event1.timestamp - event2.timestamp) < 1.0):  # Within 1 time unit
            return True
        
        # Simultaneous language indicators
        simul_indicators = ['while', 'during', 'as', 'simultaneously', 'at the same time']
        
        desc1 = event1.description.lower()
        desc2 = event2.description.lower()
        
        if any(indicator in desc1 or indicator in desc2 for indicator in simul_indicators):
            return True
        
        return False
    
    def _update_timeline(self):
        """Update the ordered timeline based on current events and constraints"""
        try:
            # Start with events sorted by timestamp
            timeline_candidates = sorted(
                self.events.keys(),
                key=lambda eid: self.events[eid].timestamp or 0
            )
            
            # Apply constraints to refine ordering
            timeline_candidates = self._apply_constraints_to_timeline(timeline_candidates)
            
            # Update timeline
            self.timeline = timeline_candidates
            self.reasoning_stats['timeline_updates'] += 1
            
        except Exception as e:
            logger.error(f"Error updating timeline: {e}")
    
    def _apply_constraints_to_timeline(self, initial_timeline: List[str]) -> List[str]:
        """Apply temporal constraints to refine timeline ordering"""
        # Topological sort based on constraints
        timeline = initial_timeline.copy()
        
        # Simple constraint application (could be improved with proper topological sort)
        changed = True
        iterations = 0
        max_iterations = len(timeline) * 2
        
        while changed and iterations < max_iterations:
            changed = False
            iterations += 1
            
            for constraint in self.constraints.values():
                if constraint.event1_id in timeline and constraint.event2_id in timeline:
                    idx1 = timeline.index(constraint.event1_id)
                    idx2 = timeline.index(constraint.event2_id)
                    
                    # Apply constraint based on relation
                    should_swap = False
                    
                    if constraint.relation == TemporalRelation.BEFORE and idx1 > idx2:
                        should_swap = True
                    elif constraint.relation == TemporalRelation.AFTER and idx1 < idx2:
                        should_swap = True
                    elif constraint.relation == TemporalRelation.CAUSES and idx1 > idx2:
                        should_swap = True
                    elif constraint.relation == TemporalRelation.ENABLES and idx1 > idx2:
                        should_swap = True
                    
                    if should_swap and constraint.strength > 0.5:
                        # Swap elements to satisfy constraint
                        timeline[idx1], timeline[idx2] = timeline[idx2], timeline[idx1]
                        changed = True
        
        return timeline
    
    def _detect_inconsistencies_for_event(self, event_id: str):
        """Detect inconsistencies involving a specific event"""
        event = self.events[event_id]
        
        # Check for temporal paradoxes
        for constraint in self.constraints.values():
            if constraint.event1_id == event_id or constraint.event2_id == event_id:
                if not self._validate_constraint(constraint):
                    inconsistency = TemporalInconsistency(
                        inconsistency_id=f"paradox_{event_id}_{constraint.constraint_id}",
                        conflicting_events=[constraint.event1_id, constraint.event2_id],
                        conflicting_constraints=[constraint.constraint_id],
                        description=f"Temporal paradox involving {event_id}",
                        severity=constraint.strength
                    )
                    self.inconsistencies.append(inconsistency)
                    self.reasoning_stats['inconsistencies_detected'] += 1
    
    def _validate_constraint(self, constraint: TemporalConstraint) -> bool:
        """Validate that a constraint doesn't create inconsistencies"""
        try:
            event1 = self.events[constraint.event1_id]
            event2 = self.events[constraint.event2_id]
            
            # Check timestamp consistency
            if event1.timestamp and event2.timestamp:
                if constraint.relation == TemporalRelation.BEFORE:
                    return event1.timestamp < event2.timestamp
                elif constraint.relation == TemporalRelation.AFTER:
                    return event1.timestamp > event2.timestamp
                elif constraint.relation == TemporalRelation.SIMULTANEOUS:
                    return abs(event1.timestamp - event2.timestamp) < 1.0
            
            # Check logical consistency
            if constraint.relation == TemporalRelation.CAUSES:
                # A cause cannot happen after its effect
                if event1.timestamp and event2.timestamp:
                    return event1.timestamp <= event2.timestamp
            
            return True  # Assume valid if we can't determine otherwise
            
        except Exception as e:
            logger.error(f"Error validating constraint {constraint.constraint_id}: {e}")
            return False
    
    def _calculate_continuity_score(self) -> float:
        """Calculate overall story continuity score (0-1)"""
        if not self.events:
            return 1.0
        
        # Factors that affect continuity
        total_events = len(self.events)
        total_inconsistencies = len(self.inconsistencies)
        
        # Base score starts high
        score = 1.0
        
        # Subtract for inconsistencies
        if total_inconsistencies > 0:
            inconsistency_penalty = min(0.8, total_inconsistencies * 0.1)
            score -= inconsistency_penalty
        
        # Subtract for missing constraints (events without temporal relationships)
        unconnected_events = self._count_unconnected_events()
        if unconnected_events > 0:
            isolation_penalty = min(0.3, unconnected_events * 0.05)
            score -= isolation_penalty
        
        # Bonus for well-structured timeline
        if len(self.timeline) == total_events and total_inconsistencies == 0:
            score = min(1.0, score + 0.1)
        
        return max(0.0, score)
    
    def _count_unconnected_events(self) -> int:
        """Count events that have no temporal constraints"""
        connected_events = set()
        
        for constraint in self.constraints.values():
            connected_events.add(constraint.event1_id)
            connected_events.add(constraint.event2_id)
        
        return len(self.events) - len(connected_events)
    
    def _generate_ordered_timeline(self) -> List[Dict[str, Any]]:
        """Generate an ordered timeline with event details"""
        timeline_data = []
        
        for event_id in self.timeline:
            if event_id in self.events:
                event = self.events[event_id]
                timeline_data.append({
                    'event_id': event_id,
                    'description': event.description,
                    'time_frame': event.time_frame.value,
                    'timestamp': event.timestamp,
                    'participants': event.participants,
                    'location': event.location,
                    'certainty': event.certainty
                })
        
        return timeline_data
    
    def _detect_plot_holes(self) -> List[Dict[str, Any]]:
        """Detect potential plot holes in the story"""
        plot_holes = []
        
        # Look for events with unfulfilled preconditions
        for event in self.events.values():
            for precondition in event.preconditions:
                if not self._is_precondition_satisfied(event.event_id, precondition):
                    plot_holes.append({
                        'type': 'unfulfilled_precondition',
                        'event_id': event.event_id,
                        'description': f"Event '{event.description}' requires '{precondition}' but it's not established",
                        'severity': 0.8
                    })
        
        # Look for effects that are never utilized
        all_effects = set()
        all_preconditions = set()
        
        for event in self.events.values():
            all_effects.update(event.effects)
            all_preconditions.update(event.preconditions)
        
        unused_effects = all_effects - all_preconditions
        for effect in unused_effects:
            # Find the event that produces this unused effect
            producing_events = [e for e in self.events.values() if effect in e.effects]
            if producing_events:
                plot_holes.append({
                    'type': 'unused_effect',
                    'event_id': producing_events[0].event_id,
                    'description': f"Effect '{effect}' is established but never used",
                    'severity': 0.4
                })
        
        return plot_holes
    
    def _is_precondition_satisfied(self, event_id: str, precondition: str) -> bool:
        """Check if a precondition is satisfied by earlier events"""
        event = self.events[event_id]
        
        # Look for earlier events that produce this precondition as an effect
        for other_event in self.events.values():
            if (other_event.event_id != event_id and
                precondition in other_event.effects):
                
                # Check if this other event comes before our event
                if self._happens_before(other_event.event_id, event_id):
                    return True
        
        return False
    
    def _happens_before(self, event1_id: str, event2_id: str) -> bool:
        """Check if event1 happens before event2"""
        if event1_id in self.timeline and event2_id in self.timeline:
            return self.timeline.index(event1_id) < self.timeline.index(event2_id)
        
        # Check constraints
        for constraint in self.constraints.values():
            if (constraint.event1_id == event1_id and 
                constraint.event2_id == event2_id and
                constraint.relation in [TemporalRelation.BEFORE, TemporalRelation.CAUSES, TemporalRelation.ENABLES]):
                return True
        
        # Check timestamps
        event1 = self.events.get(event1_id)
        event2 = self.events.get(event2_id)
        
        if event1 and event2 and event1.timestamp and event2.timestamp:
            return event1.timestamp < event2.timestamp
        
        return False
    
    def _analyze_temporal_patterns(self) -> Dict[str, Any]:
        """Analyze temporal patterns in the story"""
        patterns = {
            'sequential_events': 0,
            'causal_chains': 0,
            'simultaneous_events': 0,
            'time_frames_used': {},
            'average_event_duration': 0,
            'temporal_complexity': 0
        }
        
        # Count different types of relationships
        for constraint in self.constraints.values():
            if constraint.relation == TemporalRelation.BEFORE:
                patterns['sequential_events'] += 1
            elif constraint.relation == TemporalRelation.CAUSES:
                patterns['causal_chains'] += 1
            elif constraint.relation == TemporalRelation.SIMULTANEOUS:
                patterns['simultaneous_events'] += 1
        
        # Analyze time frames
        for event in self.events.values():
            frame = event.time_frame.value
            patterns['time_frames_used'][frame] = patterns['time_frames_used'].get(frame, 0) + 1
        
        # Calculate average event duration
        durations = [e.duration for e in self.events.values() if e.duration]
        if durations:
            patterns['average_event_duration'] = sum(durations) / len(durations)
        
        # Calculate temporal complexity (number of constraints per event)
        if self.events:
            patterns['temporal_complexity'] = len(self.constraints) / len(self.events)
        
        return patterns
    
    def _analyze_story_pacing(self) -> Dict[str, Any]:
        """Analyze story pacing based on temporal structure"""
        pacing = {
            'event_density': 0,  # Events per time unit
            'pacing_consistency': 0,  # How consistent the pacing is
            'tension_points': [],  # Events that create temporal tension
            'pacing_rhythm': 'unknown'
        }
        
        if not self.events:
            return pacing
        
        # Calculate event density
        timestamps = [e.timestamp for e in self.events.values() if e.timestamp]
        if len(timestamps) > 1:
            time_span = max(timestamps) - min(timestamps)
            if time_span > 0:
                pacing['event_density'] = len(timestamps) / time_span
        
        # Analyze pacing consistency
        if len(timestamps) > 2:
            intervals = []
            sorted_timestamps = sorted(timestamps)
            for i in range(1, len(sorted_timestamps)):
                intervals.append(sorted_timestamps[i] - sorted_timestamps[i-1])
            
            if intervals:
                avg_interval = sum(intervals) / len(intervals)
                variance = sum((interval - avg_interval) ** 2 for interval in intervals) / len(intervals)
                consistency = 1.0 / (1.0 + variance)  # Higher consistency = lower variance
                pacing['pacing_consistency'] = consistency
        
        # Identify tension points (events with many constraints)
        for event_id, event in self.events.items():
            constraint_count = sum(1 for c in self.constraints.values() 
                                 if c.event1_id == event_id or c.event2_id == event_id)
            if constraint_count > 3:  # Arbitrary threshold
                pacing['tension_points'].append({
                    'event_id': event_id,
                    'description': event.description,
                    'constraint_count': constraint_count
                })
        
        return pacing
    
    def _inconsistency_to_dict(self, inconsistency: TemporalInconsistency) -> Dict[str, Any]:
        """Convert inconsistency to dictionary format"""
        return {
            'inconsistency_id': inconsistency.inconsistency_id,
            'conflicting_events': inconsistency.conflicting_events,
            'conflicting_constraints': inconsistency.conflicting_constraints,
            'description': inconsistency.description,
            'severity': inconsistency.severity,
            'suggested_resolution': inconsistency.suggested_resolution
        }
    
    def get_temporal_statistics(self) -> Dict[str, Any]:
        """Get statistics about temporal reasoning performance"""
        uptime = time.time() - self.reasoning_stats['start_time']
        
        return {
            'events_processed': self.reasoning_stats['events_processed'],
            'constraints_created': self.reasoning_stats['constraints_created'],
            'inconsistencies_detected': self.reasoning_stats['inconsistencies_detected'],
            'timeline_updates': self.reasoning_stats['timeline_updates'],
            'current_events': len(self.events),
            'current_constraints': len(self.constraints),
            'current_inconsistencies': len(self.inconsistencies),
            'timeline_length': len(self.timeline),
            'continuity_score': self._calculate_continuity_score(),
            'uptime_seconds': uptime
        }
    
    def clear_temporal_data(self):
        """Clear all temporal data"""
        self.events.clear()
        self.constraints.clear()
        self.timeline.clear()
        self.inconsistencies.clear()
        logger.info("Temporal data cleared")
    
    def export_timeline(self) -> Dict[str, Any]:
        """Export complete timeline data"""
        return {
            'events': {eid: {
                'description': event.description,
                'time_frame': event.time_frame.value,
                'timestamp': event.timestamp,
                'duration': event.duration,
                'participants': event.participants,
                'location': event.location,
                'preconditions': event.preconditions,
                'effects': event.effects,
                'certainty': event.certainty
            } for eid, event in self.events.items()},
            'constraints': {cid: {
                'event1_id': constraint.event1_id,
                'event2_id': constraint.event2_id,
                'relation': constraint.relation.value,
                'strength': constraint.strength,
                'reasoning': constraint.reasoning
            } for cid, constraint in self.constraints.items()},
            'timeline_order': self.timeline,
            'inconsistencies': [self._inconsistency_to_dict(inc) for inc in self.inconsistencies]
        }