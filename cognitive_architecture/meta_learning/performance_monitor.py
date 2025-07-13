"""
Performance Monitoring System

Implements self-monitoring cognitive performance metrics to track system
efficiency, accuracy, and effectiveness across different cognitive tasks.
"""

import time
import logging
import asyncio
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass, field
from enum import Enum
import numpy as np
from collections import defaultdict, deque
import json

logger = logging.getLogger(__name__)


class MetricType(Enum):
    """Types of performance metrics"""
    PROCESSING_TIME = "processing_time"
    ACCURACY = "accuracy"
    EFFICIENCY = "efficiency"
    MEMORY_USAGE = "memory_usage"
    ATTENTION_FOCUS = "attention_focus"
    REASONING_QUALITY = "reasoning_quality"
    ADAPTATION_SPEED = "adaptation_speed"
    CONVERGENCE_RATE = "convergence_rate"


@dataclass
class PerformanceMetric:
    """Performance metric data structure"""
    metric_type: MetricType
    value: float
    timestamp: float = field(default_factory=time.time)
    context: Dict[str, Any] = field(default_factory=dict)
    component: str = "general"
    confidence: float = 1.0
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary representation"""
        return {
            'metric_type': self.metric_type.value,
            'value': self.value,
            'timestamp': self.timestamp,
            'context': self.context,
            'component': self.component,
            'confidence': self.confidence
        }


class PerformanceMonitor:
    """Self-monitoring cognitive performance metrics system"""
    
    def __init__(self, history_size: int = 10000):
        self.history_size = history_size
        self.metrics_history: Dict[MetricType, deque] = defaultdict(lambda: deque(maxlen=history_size))
        self.component_metrics: Dict[str, Dict[MetricType, deque]] = defaultdict(
            lambda: defaultdict(lambda: deque(maxlen=history_size))
        )
        self.aggregated_stats: Dict[MetricType, Dict[str, float]] = defaultdict(dict)
        self.baseline_metrics: Dict[MetricType, float] = {}
        self.alert_thresholds: Dict[MetricType, Tuple[float, float]] = {
            MetricType.PROCESSING_TIME: (0.01, 10.0),  # Min 10ms, Max 10s
            MetricType.ACCURACY: (0.0, 1.0),  # 0-100%
            MetricType.EFFICIENCY: (0.0, 1.0),  # 0-100%
            MetricType.MEMORY_USAGE: (0.0, 1000.0),  # MB
            MetricType.ATTENTION_FOCUS: (0.0, 1.0),  # 0-100%
            MetricType.REASONING_QUALITY: (0.0, 1.0),  # 0-100%
            MetricType.ADAPTATION_SPEED: (0.0, 10.0),  # Adaptations per second
            MetricType.CONVERGENCE_RATE: (0.0, 1.0)  # 0-100%
        }
        self.monitoring_active = True
        self.listeners: List[callable] = []
        
        # Performance trend tracking
        self.trend_window = 100  # Last N metrics for trend analysis
        self.performance_trends: Dict[MetricType, str] = {}  # "improving", "stable", "degrading"
        
        logger.info("Performance monitoring system initialized")
    
    def record_metric(self, metric: PerformanceMetric) -> bool:
        """Record a performance metric"""
        try:
            if not self.monitoring_active:
                return False
            
            # Validate metric value
            if not self._validate_metric(metric):
                logger.warning(f"Invalid metric value: {metric.metric_type.value} = {metric.value}")
                return False
            
            # Store in global history
            self.metrics_history[metric.metric_type].append(metric)
            
            # Store in component-specific history
            self.component_metrics[metric.component][metric.metric_type].append(metric)
            
            # Update aggregated statistics
            self._update_aggregated_stats(metric)
            
            # Update performance trends
            self._update_performance_trends(metric.metric_type)
            
            # Check for alerts
            self._check_alerts(metric)
            
            # Notify listeners
            self._notify_listeners(metric)
            
            return True
            
        except Exception as e:
            logger.error(f"Error recording metric: {e}")
            return False
    
    def record_processing_time(self, operation: str, duration: float, component: str = "general") -> bool:
        """Record processing time metric"""
        metric = PerformanceMetric(
            metric_type=MetricType.PROCESSING_TIME,
            value=duration,
            context={'operation': operation},
            component=component
        )
        return self.record_metric(metric)
    
    def record_accuracy(self, accuracy: float, task: str, component: str = "general") -> bool:
        """Record accuracy metric"""
        metric = PerformanceMetric(
            metric_type=MetricType.ACCURACY,
            value=accuracy,
            context={'task': task},
            component=component
        )
        return self.record_metric(metric)
    
    def record_efficiency(self, efficiency: float, resource_type: str, component: str = "general") -> bool:
        """Record efficiency metric"""
        metric = PerformanceMetric(
            metric_type=MetricType.EFFICIENCY,
            value=efficiency,
            context={'resource_type': resource_type},
            component=component
        )
        return self.record_metric(metric)
    
    def get_performance_summary(self, component: Optional[str] = None, 
                              time_window: Optional[float] = None) -> Dict[str, Any]:
        """Get comprehensive performance summary"""
        try:
            current_time = time.time()
            summary = {
                'timestamp': current_time,
                'component': component or 'all',
                'time_window': time_window,
                'metrics': {},
                'trends': {},
                'alerts': [],
                'overall_health': 'unknown'
            }
            
            # Select appropriate metrics source
            if component:
                metrics_source = self.component_metrics.get(component, {})
            else:
                metrics_source = self.metrics_history
            
            # Calculate statistics for each metric type
            for metric_type in MetricType:
                if metric_type in metrics_source:
                    metrics = list(metrics_source[metric_type])
                    
                    # Filter by time window if specified
                    if time_window:
                        cutoff_time = current_time - time_window
                        metrics = [m for m in metrics if m.timestamp >= cutoff_time]
                    
                    if metrics:
                        values = [m.value for m in metrics]
                        summary['metrics'][metric_type.value] = {
                            'count': len(values),
                            'mean': float(np.mean(values)),
                            'std': float(np.std(values)),
                            'min': float(np.min(values)),
                            'max': float(np.max(values)),
                            'latest': values[-1] if values else 0,
                            'trend': self.performance_trends.get(metric_type, 'stable')
                        }
                        
                        # Add percentiles
                        if len(values) >= 2:
                            summary['metrics'][metric_type.value]['p50'] = float(np.percentile(values, 50))
                            summary['metrics'][metric_type.value]['p95'] = float(np.percentile(values, 95))
                            summary['metrics'][metric_type.value]['p99'] = float(np.percentile(values, 99))
            
            # Calculate overall health score
            summary['overall_health'] = self._calculate_health_score(summary['metrics'])
            
            # Add trend information
            summary['trends'] = dict(self.performance_trends)
            
            # Add any recent alerts
            summary['alerts'] = self._get_recent_alerts()
            
            return summary
            
        except Exception as e:
            logger.error(f"Error generating performance summary: {e}")
            return {'error': str(e)}
    
    def get_performance_comparison(self, component1: str, component2: str, 
                                 metric_type: MetricType) -> Dict[str, Any]:
        """Compare performance between two components"""
        try:
            metrics1 = list(self.component_metrics.get(component1, {}).get(metric_type, []))
            metrics2 = list(self.component_metrics.get(component2, {}).get(metric_type, []))
            
            if not metrics1 or not metrics2:
                return {'error': 'Insufficient data for comparison'}
            
            values1 = [m.value for m in metrics1[-100:]]  # Last 100 metrics
            values2 = [m.value for m in metrics2[-100:]]
            
            mean1, mean2 = np.mean(values1), np.mean(values2)
            std1, std2 = np.std(values1), np.std(values2)
            
            # Statistical significance test (simplified)
            t_stat = (mean1 - mean2) / np.sqrt((std1**2 / len(values1)) + (std2**2 / len(values2)))
            
            comparison = {
                'component1': {
                    'name': component1,
                    'mean': float(mean1),
                    'std': float(std1),
                    'count': len(values1)
                },
                'component2': {
                    'name': component2,
                    'mean': float(mean2),
                    'std': float(std2),
                    'count': len(values2)
                },
                'difference': {
                    'absolute': float(mean1 - mean2),
                    'relative': float((mean1 - mean2) / mean2) if mean2 != 0 else 0,
                    't_statistic': float(t_stat),
                    'better_component': component1 if mean1 > mean2 else component2
                },
                'metric_type': metric_type.value
            }
            
            return comparison
            
        except Exception as e:
            logger.error(f"Error comparing performance: {e}")
            return {'error': str(e)}
    
    def set_baseline(self, metric_type: MetricType, baseline_value: float):
        """Set baseline value for a metric type"""
        self.baseline_metrics[metric_type] = baseline_value
        logger.info(f"Set baseline for {metric_type.value}: {baseline_value}")
    
    def get_performance_improvement(self, metric_type: MetricType, 
                                  time_window: float = 3600) -> Dict[str, Any]:
        """Calculate performance improvement over time window"""
        try:
            current_time = time.time()
            cutoff_time = current_time - time_window
            
            metrics = [m for m in self.metrics_history[metric_type] 
                      if m.timestamp >= cutoff_time]
            
            if len(metrics) < 2:
                return {'error': 'Insufficient data for improvement calculation'}
            
            # Split into early and recent periods
            split_point = len(metrics) // 2
            early_metrics = metrics[:split_point]
            recent_metrics = metrics[split_point:]
            
            early_mean = np.mean([m.value for m in early_metrics])
            recent_mean = np.mean([m.value for m in recent_metrics])
            
            improvement = {
                'metric_type': metric_type.value,
                'time_window': time_window,
                'early_period': {
                    'mean': float(early_mean),
                    'count': len(early_metrics)
                },
                'recent_period': {
                    'mean': float(recent_mean),
                    'count': len(recent_metrics)
                },
                'improvement': {
                    'absolute': float(recent_mean - early_mean),
                    'relative': float((recent_mean - early_mean) / early_mean) if early_mean != 0 else 0,
                    'direction': 'improvement' if recent_mean > early_mean else 'degradation'
                }
            }
            
            # Compare with baseline if available
            if metric_type in self.baseline_metrics:
                baseline = self.baseline_metrics[metric_type]
                improvement['vs_baseline'] = {
                    'absolute': float(recent_mean - baseline),
                    'relative': float((recent_mean - baseline) / baseline) if baseline != 0 else 0
                }
            
            return improvement
            
        except Exception as e:
            logger.error(f"Error calculating performance improvement: {e}")
            return {'error': str(e)}
    
    def add_performance_listener(self, callback: callable):
        """Add listener for performance metric updates"""
        if callback not in self.listeners:
            self.listeners.append(callback)
            logger.info(f"Added performance listener: {callback.__name__}")
    
    def remove_performance_listener(self, callback: callable):
        """Remove performance listener"""
        if callback in self.listeners:
            self.listeners.remove(callback)
            logger.info(f"Removed performance listener: {callback.__name__}")
    
    def export_metrics(self, filepath: str, component: Optional[str] = None, 
                      time_window: Optional[float] = None) -> bool:
        """Export metrics to JSON file"""
        try:
            current_time = time.time()
            export_data = {
                'export_timestamp': current_time,
                'component': component,
                'time_window': time_window,
                'metrics': {}
            }
            
            # Select metrics source
            if component:
                metrics_source = self.component_metrics.get(component, {})
            else:
                metrics_source = self.metrics_history
            
            # Export metrics
            for metric_type, metrics in metrics_source.items():
                metric_list = list(metrics)
                
                # Filter by time window if specified
                if time_window:
                    cutoff_time = current_time - time_window
                    metric_list = [m for m in metric_list if m.timestamp >= cutoff_time]
                
                export_data['metrics'][metric_type.value] = [m.to_dict() for m in metric_list]
            
            # Save to file
            with open(filepath, 'w') as f:
                json.dump(export_data, f, indent=2)
            
            logger.info(f"Exported metrics to {filepath}")
            return True
            
        except Exception as e:
            logger.error(f"Error exporting metrics: {e}")
            return False
    
    def _validate_metric(self, metric: PerformanceMetric) -> bool:
        """Validate metric value against thresholds"""
        if metric.metric_type not in self.alert_thresholds:
            return True
        
        min_val, max_val = self.alert_thresholds[metric.metric_type]
        return min_val <= metric.value <= max_val
    
    def _update_aggregated_stats(self, metric: PerformanceMetric):
        """Update aggregated statistics for metric type"""
        metrics = list(self.metrics_history[metric.metric_type])
        if metrics:
            values = [m.value for m in metrics]
            self.aggregated_stats[metric.metric_type] = {
                'mean': float(np.mean(values)),
                'std': float(np.std(values)),
                'count': len(values),
                'latest': values[-1]
            }
    
    def _update_performance_trends(self, metric_type: MetricType):
        """Update performance trend for metric type"""
        metrics = list(self.metrics_history[metric_type])
        if len(metrics) < self.trend_window:
            return
        
        recent_metrics = metrics[-self.trend_window:]
        values = [m.value for m in recent_metrics]
        
        # Simple trend detection using linear regression
        x = np.arange(len(values))
        coefficients = np.polyfit(x, values, 1)
        slope = coefficients[0]
        
        # Determine trend based on slope
        if abs(slope) < 0.001:  # Threshold for "stable"
            trend = "stable"
        elif slope > 0:
            trend = "improving"
        else:
            trend = "degrading"
        
        self.performance_trends[metric_type] = trend
    
    def _check_alerts(self, metric: PerformanceMetric):
        """Check for performance alerts"""
        # Simple alert checking (can be enhanced with more sophisticated rules)
        if metric.metric_type in self.alert_thresholds:
            min_val, max_val = self.alert_thresholds[metric.metric_type]
            if metric.value < min_val or metric.value > max_val:
                logger.warning(f"Performance alert: {metric.metric_type.value} = {metric.value} "
                             f"(threshold: {min_val}-{max_val})")
    
    def _notify_listeners(self, metric: PerformanceMetric):
        """Notify all registered listeners"""
        for listener in self.listeners:
            try:
                listener(metric)
            except Exception as e:
                logger.error(f"Error in performance listener {listener.__name__}: {e}")
    
    def _calculate_health_score(self, metrics: Dict[str, Any]) -> str:
        """Calculate overall system health score"""
        if not metrics:
            return "unknown"
        
        # Simple health calculation based on trends
        improving_count = sum(1 for m in metrics.values() if m.get('trend') == 'improving')
        degrading_count = sum(1 for m in metrics.values() if m.get('trend') == 'degrading')
        total_count = len(metrics)
        
        if degrading_count > total_count * 0.5:
            return "poor"
        elif improving_count > total_count * 0.5:
            return "excellent"
        else:
            return "good"
    
    def _get_recent_alerts(self) -> List[str]:
        """Get recent performance alerts"""
        # Placeholder for alert history (would be implemented with proper alert storage)
        return []
    
    def start_monitoring(self):
        """Start performance monitoring"""
        self.monitoring_active = True
        logger.info("Performance monitoring started")
    
    def stop_monitoring(self):
        """Stop performance monitoring"""
        self.monitoring_active = False
        logger.info("Performance monitoring stopped")
    
    def clear_history(self, metric_type: Optional[MetricType] = None):
        """Clear metrics history"""
        if metric_type:
            self.metrics_history[metric_type].clear()
            for component_metrics in self.component_metrics.values():
                component_metrics[metric_type].clear()
            logger.info(f"Cleared history for {metric_type.value}")
        else:
            self.metrics_history.clear()
            self.component_metrics.clear()
            logger.info("Cleared all metrics history")
    
    def get_monitoring_status(self) -> Dict[str, Any]:
        """Get monitoring system status"""
        return {
            'monitoring_active': self.monitoring_active,
            'total_metrics': sum(len(metrics) for metrics in self.metrics_history.values()),
            'metric_types': list(self.metrics_history.keys()),
            'components': list(self.component_metrics.keys()),
            'listeners_count': len(self.listeners),
            'history_size': self.history_size,
            'trend_window': self.trend_window
        }