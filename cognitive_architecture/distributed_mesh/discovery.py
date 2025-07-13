"""
Enhanced Mesh Node Discovery Protocols

This module implements advanced node discovery mechanisms for the distributed
cognitive mesh, including automatic discovery, capability broadcasting, and
dynamic mesh topology updates.
"""

import asyncio
import json
import logging
import time
import uuid
import socket
import threading
from typing import Dict, Any, List, Optional, Set, Callable
from dataclasses import dataclass, field
from enum import Enum
import hashlib
import random

logger = logging.getLogger(__name__)


class DiscoveryProtocol(Enum):
    """Types of discovery protocols"""
    MULTICAST = "multicast"
    BROADCAST = "broadcast"
    GOSSIP = "gossip"
    CENTRALIZED = "centralized"


@dataclass
class NodeAdvertisement:
    """Advertisement message for node discovery"""
    node_id: str
    node_type: str
    capabilities: Set[str]
    endpoint: str
    port: int
    load_capacity: float
    current_load: float
    metadata: Dict[str, Any] = field(default_factory=dict)
    timestamp: float = field(default_factory=time.time)
    ttl: int = 30  # Time to live in seconds
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'node_id': self.node_id,
            'node_type': self.node_type,
            'capabilities': list(self.capabilities),
            'endpoint': self.endpoint,
            'port': self.port,
            'load_capacity': self.load_capacity,
            'current_load': self.current_load,
            'metadata': self.metadata,
            'timestamp': self.timestamp,
            'ttl': self.ttl
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'NodeAdvertisement':
        return cls(
            node_id=data['node_id'],
            node_type=data['node_type'],
            capabilities=set(data['capabilities']),
            endpoint=data['endpoint'],
            port=data['port'],
            load_capacity=data['load_capacity'],
            current_load=data['current_load'],
            metadata=data.get('metadata', {}),
            timestamp=data['timestamp'],
            ttl=data.get('ttl', 30)
        )
    
    def is_expired(self) -> bool:
        """Check if advertisement has expired"""
        return time.time() - self.timestamp > self.ttl


@dataclass
class DiscoveryConfig:
    """Configuration for mesh discovery"""
    protocol: DiscoveryProtocol = DiscoveryProtocol.MULTICAST
    multicast_group: str = "224.0.0.1"
    multicast_port: int = 9999
    discovery_interval: float = 10.0  # seconds
    advertisement_ttl: int = 30  # seconds
    max_discovery_retries: int = 3
    gossip_fanout: int = 3  # Number of nodes to gossip to
    enable_capability_matching: bool = True
    enable_load_balancing: bool = True


class CapabilityMatcher:
    """Advanced capability matching algorithms for agent selection"""
    
    def __init__(self):
        self.capability_weights: Dict[str, float] = {}
        self.capability_dependencies: Dict[str, Set[str]] = {}
        self.performance_history: Dict[str, List[float]] = {}
    
    def add_capability_weight(self, capability: str, weight: float):
        """Add weight for capability importance"""
        self.capability_weights[capability] = weight
    
    def add_capability_dependency(self, capability: str, dependencies: Set[str]):
        """Add dependencies for a capability"""
        self.capability_dependencies[capability] = dependencies
    
    def record_performance(self, node_id: str, performance: float):
        """Record performance metric for a node"""
        if node_id not in self.performance_history:
            self.performance_history[node_id] = []
        self.performance_history[node_id].append(performance)
        # Keep only last 10 performance records
        if len(self.performance_history[node_id]) > 10:
            self.performance_history[node_id] = self.performance_history[node_id][-10:]
    
    def get_average_performance(self, node_id: str) -> float:
        """Get average performance for a node"""
        if node_id not in self.performance_history:
            return 0.5  # Default neutral performance
        
        performances = self.performance_history[node_id]
        return sum(performances) / len(performances) if performances else 0.5
    
    def calculate_capability_score(self, node_capabilities: Set[str], 
                                 required_capabilities: Set[str]) -> float:
        """Calculate capability matching score"""
        if not required_capabilities:
            return 1.0
        
        # Base score: fraction of required capabilities that are available
        available_required = node_capabilities.intersection(required_capabilities)
        base_score = len(available_required) / len(required_capabilities)
        
        # Weighted score: consider capability importance
        weighted_score = 0.0
        total_weight = 0.0
        
        for capability in required_capabilities:
            weight = self.capability_weights.get(capability, 1.0)
            total_weight += weight
            
            if capability in node_capabilities:
                weighted_score += weight
                
                # Check dependencies
                dependencies = self.capability_dependencies.get(capability, set())
                if dependencies:
                    dep_available = node_capabilities.intersection(dependencies)
                    dep_bonus = len(dep_available) / len(dependencies) * 0.2  # 20% bonus
                    weighted_score += dep_bonus * weight
        
        final_score = weighted_score / total_weight if total_weight > 0 else base_score
        return min(1.0, final_score)
    
    def rank_nodes(self, nodes: List[NodeAdvertisement], 
                   required_capabilities: Set[str],
                   task_priority: float = 1.0) -> List[tuple]:
        """Rank nodes for capability matching with load balancing"""
        ranked_nodes = []
        
        for node in nodes:
            # Calculate capability score
            capability_score = self.calculate_capability_score(
                node.capabilities, required_capabilities
            )
            
            # Consider node load (prefer less loaded nodes)
            load_factor = 1.0 - (node.current_load / node.load_capacity) if node.load_capacity > 0 else 0.0
            
            # Consider historical performance
            performance_score = self.get_average_performance(node.node_id)
            
            # Combine scores with weights
            combined_score = (
                capability_score * 0.5 +       # 50% capability matching
                load_factor * 0.3 +            # 30% load balancing
                performance_score * 0.2        # 20% historical performance
            )
            
            # Apply task priority modifier
            final_score = combined_score * (1.0 + task_priority * 0.1)
            
            ranked_nodes.append((node, final_score, {
                'capability_score': capability_score,
                'load_factor': load_factor,
                'performance_score': performance_score,
                'combined_score': combined_score,
                'final_score': final_score
            }))
        
        # Sort by final score (highest first)
        ranked_nodes.sort(key=lambda x: x[1], reverse=True)
        return ranked_nodes


class MeshDiscoveryService:
    """Enhanced mesh discovery service with multiple protocols"""
    
    def __init__(self, config: DiscoveryConfig = None):
        self.config = config or DiscoveryConfig()
        self.local_node: Optional[NodeAdvertisement] = None
        self.discovered_nodes: Dict[str, NodeAdvertisement] = {}
        self.capability_matcher = CapabilityMatcher()
        self.discovery_callbacks: List[Callable] = []
        self.is_running = False
        
        # Network components
        self.multicast_socket: Optional[socket.socket] = None
        self.discovery_thread: Optional[threading.Thread] = None
        
        # Gossip protocol state
        self.gossip_peers: Set[str] = set()
        self.gossip_messages: Dict[str, float] = {}  # message_id -> timestamp
        
        # Performance tracking
        self.discovery_stats = {
            'nodes_discovered': 0,
            'advertisements_sent': 0,
            'advertisements_received': 0,
            'discovery_rounds': 0,
            'average_discovery_time': 0.0
        }
    
    def set_local_node(self, node_advertisement: NodeAdvertisement):
        """Set the local node advertisement"""
        self.local_node = node_advertisement
        logger.info(f"Set local node: {node_advertisement.node_id}")
    
    def add_discovery_callback(self, callback: Callable[[NodeAdvertisement], None]):
        """Add callback for node discovery events"""
        self.discovery_callbacks.append(callback)
    
    def setup_capability_weights(self, weights: Dict[str, float]):
        """Configure capability weights for matching"""
        for capability, weight in weights.items():
            self.capability_matcher.add_capability_weight(capability, weight)
    
    def setup_capability_dependencies(self, dependencies: Dict[str, Set[str]]):
        """Configure capability dependencies"""
        for capability, deps in dependencies.items():
            self.capability_matcher.add_capability_dependency(capability, deps)
    
    async def start_discovery(self):
        """Start the discovery service"""
        if self.is_running:
            return
        
        self.is_running = True
        logger.info(f"Starting mesh discovery with protocol: {self.config.protocol.value}")
        
        if self.config.protocol == DiscoveryProtocol.MULTICAST:
            await self._start_multicast_discovery()
        elif self.config.protocol == DiscoveryProtocol.GOSSIP:
            await self._start_gossip_discovery()
        
        # Start periodic cleanup and statistics
        asyncio.create_task(self._periodic_cleanup())
        asyncio.create_task(self._periodic_advertisement())
    
    async def stop_discovery(self):
        """Stop the discovery service"""
        self.is_running = False
        
        if self.multicast_socket:
            self.multicast_socket.close()
            self.multicast_socket = None
        
        logger.info("Mesh discovery service stopped")
    
    async def _start_multicast_discovery(self):
        """Start multicast-based discovery"""
        try:
            # Create multicast socket
            self.multicast_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            self.multicast_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.multicast_socket.bind((self.config.interface_ip, self.config.multicast_port))
            
            # Join multicast group
            mreq = socket.inet_aton(self.config.multicast_group) + socket.inet_aton('0.0.0.0')
            self.multicast_socket.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)
            
            # Start listener thread
            self.discovery_thread = threading.Thread(target=self._multicast_listener)
            self.discovery_thread.daemon = True
            self.discovery_thread.start()
            
            logger.info(f"Multicast discovery started on {self.config.multicast_group}:{self.config.multicast_port}")
            
        except Exception as e:
            logger.error(f"Failed to start multicast discovery: {e}")
            self.is_running = False
    
    def _multicast_listener(self):
        """Listen for multicast discovery messages"""
        while self.is_running:
            try:
                data, addr = self.multicast_socket.recvfrom(1024)
                message = json.loads(data.decode('utf-8'))
                
                if message.get('type') == 'node_advertisement':
                    advertisement = NodeAdvertisement.from_dict(message['data'])
                    
                    # Don't process our own advertisements
                    if self.local_node and advertisement.node_id == self.local_node.node_id:
                        continue
                    
                    self._process_node_advertisement(advertisement)
                    self.discovery_stats['advertisements_received'] += 1
                
            except Exception as e:
                if self.is_running:  # Only log if we're supposed to be running
                    logger.error(f"Error in multicast listener: {e}")
                time.sleep(0.1)
    
    async def _start_gossip_discovery(self):
        """Start gossip-based discovery"""
        # Initialize with known peers if any
        logger.info("Gossip discovery started")
        
        # Start gossip loop
        asyncio.create_task(self._gossip_loop())
    
    async def _gossip_loop(self):
        """Main gossip protocol loop"""
        while self.is_running:
            try:
                if self.local_node and self.gossip_peers:
                    # Select random peers to gossip with
                    peers_to_contact = random.sample(
                        list(self.gossip_peers), 
                        min(self.config.gossip_fanout, len(self.gossip_peers))
                    )
                    
                    for peer_id in peers_to_contact:
                        await self._send_gossip_message(peer_id)
                
                await asyncio.sleep(self.config.discovery_interval)
                
            except Exception as e:
                logger.error(f"Error in gossip loop: {e}")
                await asyncio.sleep(1.0)
    
    async def _send_gossip_message(self, peer_id: str):
        """Send gossip message to a peer"""
        if not self.local_node:
            return
        
        message = {
            'type': 'gossip',
            'sender': self.local_node.node_id,
            'timestamp': time.time(),
            'nodes': [ad.to_dict() for ad in self.discovered_nodes.values()],
            'local_node': self.local_node.to_dict()
        }
        
        # In a real implementation, this would send over network
        # For testing, we'll simulate the message processing
        logger.debug(f"Sending gossip message to {peer_id}")
    
    def _process_node_advertisement(self, advertisement: NodeAdvertisement):
        """Process received node advertisement"""
        if advertisement.is_expired():
            return
        
        node_id = advertisement.node_id
        is_new_node = node_id not in self.discovered_nodes
        
        self.discovered_nodes[node_id] = advertisement
        
        if is_new_node:
            self.discovery_stats['nodes_discovered'] += 1
            logger.info(f"Discovered new node: {node_id} ({advertisement.node_type})")
            
            # Notify callbacks
            for callback in self.discovery_callbacks:
                try:
                    callback(advertisement)
                except Exception as e:
                    logger.error(f"Error in discovery callback: {e}")
    
    async def _periodic_cleanup(self):
        """Periodically clean up expired node advertisements"""
        while self.is_running:
            try:
                current_time = time.time()
                expired_nodes = []
                
                for node_id, advertisement in self.discovered_nodes.items():
                    if advertisement.is_expired():
                        expired_nodes.append(node_id)
                
                for node_id in expired_nodes:
                    del self.discovered_nodes[node_id]
                    logger.info(f"Removed expired node: {node_id}")
                
                # Clean up old gossip messages
                old_messages = []
                for msg_id, timestamp in self.gossip_messages.items():
                    if current_time - timestamp > 300:  # 5 minutes
                        old_messages.append(msg_id)
                
                for msg_id in old_messages:
                    del self.gossip_messages[msg_id]
                
                await asyncio.sleep(30)  # Clean up every 30 seconds
                
            except Exception as e:
                logger.error(f"Error in periodic cleanup: {e}")
                await asyncio.sleep(30)
    
    async def _periodic_advertisement(self):
        """Periodically broadcast local node advertisement"""
        while self.is_running:
            try:
                if self.local_node:
                    await self.advertise_local_node()
                
                await asyncio.sleep(self.config.discovery_interval)
                
            except Exception as e:
                logger.error(f"Error in periodic advertisement: {e}")
                await asyncio.sleep(self.config.discovery_interval)
    
    async def advertise_local_node(self):
        """Advertise local node to the network"""
        if not self.local_node or not self.is_running:
            return
        
        message = {
            'type': 'node_advertisement',
            'data': self.local_node.to_dict(),
            'timestamp': time.time()
        }
        
        if self.config.protocol == DiscoveryProtocol.MULTICAST and self.multicast_socket:
            try:
                data = json.dumps(message).encode('utf-8')
                self.multicast_socket.sendto(
                    data, 
                    (self.config.multicast_group, self.config.multicast_port)
                )
                self.discovery_stats['advertisements_sent'] += 1
                
            except Exception as e:
                logger.error(f"Failed to send multicast advertisement: {e}")
    
    def find_nodes_for_capabilities(self, required_capabilities: Set[str],
                                  task_priority: float = 1.0,
                                  max_nodes: int = 3) -> List[NodeAdvertisement]:
        """Find best nodes for required capabilities"""
        active_nodes = [
            ad for ad in self.discovered_nodes.values()
            if not ad.is_expired()
        ]
        
        if not active_nodes:
            return []
        
        # Use capability matcher to rank nodes
        ranked_nodes = self.capability_matcher.rank_nodes(
            active_nodes, required_capabilities, task_priority
        )
        
        # Return top nodes
        result = []
        for node, score, details in ranked_nodes[:max_nodes]:
            if score > 0.3:  # Minimum capability threshold
                result.append(node)
                logger.debug(f"Selected node {node.node_id} with score {score:.3f}")
        
        return result
    
    def get_discovery_statistics(self) -> Dict[str, Any]:
        """Get discovery service statistics"""
        active_nodes = len([ad for ad in self.discovered_nodes.values() if not ad.is_expired()])
        
        return {
            'discovery_config': {
                'protocol': self.config.protocol.value,
                'discovery_interval': self.config.discovery_interval,
                'advertisement_ttl': self.config.advertisement_ttl
            },
            'statistics': {
                **self.discovery_stats,
                'active_nodes': active_nodes,
                'total_known_nodes': len(self.discovered_nodes),
                'gossip_peers': len(self.gossip_peers)
            },
            'capability_matcher': {
                'known_capabilities': len(self.capability_matcher.capability_weights),
                'capability_dependencies': len(self.capability_matcher.capability_dependencies),
                'performance_history_nodes': len(self.capability_matcher.performance_history)
            }
        }
    
    def update_node_performance(self, node_id: str, performance: float):
        """Update performance metric for a node"""
        self.capability_matcher.record_performance(node_id, performance)


# Global discovery service instance
discovery_service = MeshDiscoveryService()

# Setup default capability weights and dependencies
discovery_service.setup_capability_weights({
    'text_processing': 1.0,
    'neural_inference': 1.2,
    'reasoning': 1.1,
    'dialogue': 0.9,
    'attention_allocation': 1.3,
    'memory_management': 1.0,
    'cognitive_modeling': 1.4
})

discovery_service.setup_capability_dependencies({
    'neural_inference': {'text_processing'},
    'reasoning': {'text_processing', 'memory_management'},
    'dialogue': {'text_processing', 'reasoning'},
    'cognitive_modeling': {'reasoning', 'attention_allocation', 'memory_management'}
})