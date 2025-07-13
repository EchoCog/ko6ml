# KO6ML Installation Guide

## Overview

This guide provides step-by-step instructions for installing and setting up the KO6ML Cognitive Architecture system. KO6ML enhances KoboldAI with sophisticated cognitive processing capabilities across 6 integrated phases.

## System Requirements

### Minimum Requirements
- **Operating System**: Windows 10/11, macOS 10.14+, or Linux (Ubuntu 18.04+)
- **Python**: 3.8 or higher
- **RAM**: 4GB (8GB+ recommended for optimal performance)
- **CPU**: Multi-core processor (4+ cores recommended)
- **Storage**: 10GB free space (50GB+ recommended for full installation)
- **Network**: Internet connection for initial setup

### Recommended Requirements
- **RAM**: 16GB+ for best performance
- **CPU**: 8+ cores for distributed processing
- **GPU**: CUDA-capable GPU for enhanced performance (optional)
- **Storage**: SSD with 50GB+ free space
- **Network**: 100Mbps+ for distributed mesh features

## Installation Methods

### Method 1: Quick Start (Recommended)

This method gets you up and running quickly with the core cognitive architecture.

#### Step 1: Clone Repository
```bash
# Clone the repository
git clone https://github.com/OzCog/ko6ml.git
cd ko6ml
```

#### Step 2: Install Core Dependencies
```bash
# Install essential dependencies
pip install numpy websockets aiohttp networkx pytest

# Verify installation
python --version  # Should be 3.8+
```

#### Step 3: Validate Installation
```bash
# Run phase validation tests
python test_phase1_requirements.py
python test_phase2_ecan_requirements.py
python test_phase3_requirements.py
python test_phase4_integration.py
python test_phase5_requirements.py
python test_phase6_requirements.py
```

#### Step 4: Start KO6ML
```bash
# Start the cognitive-enhanced KoboldAI server
python aiserver.py
```

### Method 2: Complete Installation

This method provides the full KoboldAI experience with all features.

#### Step 1: System Preparation

##### Windows
```cmd
# Install Python 3.8+ from python.org
# Install Git from git-scm.com

# Optional: Install Visual Studio Build Tools for compilation
# Download from: https://visualstudio.microsoft.com/visual-cpp-build-tools/
```

##### macOS
```bash
# Install Homebrew if not already installed
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"

# Install Python and Git
brew install python@3.9 git
```

##### Linux (Ubuntu/Debian)
```bash
# Update package list
sudo apt update

# Install Python, pip, and Git
sudo apt install python3 python3-pip git build-essential

# Install additional dependencies
sudo apt install python3-dev python3-venv
```

#### Step 2: Create Virtual Environment
```bash
# Navigate to installation directory
cd ko6ml

# Create virtual environment
python -m venv venv

# Activate virtual environment
# On Windows:
venv\Scripts\activate
# On macOS/Linux:
source venv/bin/activate
```

#### Step 3: Install Dependencies
```bash
# Install core cognitive architecture dependencies
pip install numpy websockets aiohttp networkx pytest

# Install additional packages for development
pip install black flake8 mypy pytest-cov

# Optional: Install visualization dependencies
pip install matplotlib seaborn plotly
```

#### Step 4: KoboldAI Dependencies (Partial)
```bash
# Note: Some KoboldAI dependencies may need manual installation
# due to version conflicts. Install what's available:

pip install Flask Flask-SocketIO Werkzeug
pip install transformers huggingface_hub
pip install requests markdown bleach

# Note: PyTorch installation may require specific commands
# See: https://pytorch.org/get-started/locally/
```

#### Step 5: Validate Complete Installation
```bash
# Run all validation tests
python -m pytest test_*.py -v

# Run demonstration scripts
python phase3_demonstration.py
python phase4_demonstration.py
python phase5_demonstration.py
python phase6_demonstration.py
```

### Method 3: Docker Installation (Future)

Docker installation is planned for future releases to simplify deployment.

## Configuration

### Basic Configuration

#### Initialize Cognitive Architecture
```python
# Basic initialization script
from cognitive_architecture.integration import kobold_cognitive_integrator

# Initialize with default settings
success = kobold_cognitive_integrator.initialize()
if success:
    print("✅ Cognitive architecture initialized successfully!")
else:
    print("❌ Cognitive architecture initialization failed")
```

#### Configuration File
Create `cognitive_config.json`:
```json
{
    "ecan": {
        "sti_budget": 1000,
        "lti_budget": 1000,
        "decay_rate": 0.1,
        "spreading_threshold": 0.1
    },
    "mesh": {
        "max_nodes": 10,
        "discovery_interval": 30.0,
        "health_check_interval": 10.0,
        "timeout_seconds": 60.0
    },
    "reasoning": {
        "logical_confidence_threshold": 0.7,
        "temporal_consistency_threshold": 0.8,
        "causal_influence_threshold": 0.6,
        "multimodal_pattern_threshold": 0.5
    },
    "meta_learning": {
        "performance_history_size": 1000,
        "learning_rate": 0.01,
        "adaptation_threshold": 0.1
    }
}
```

### Advanced Configuration

#### Environment Variables
```bash
# Set environment variables for advanced configuration
export KO6ML_CONFIG_PATH="/path/to/cognitive_config.json"
export KO6ML_LOG_LEVEL="INFO"
export KO6ML_CACHE_DIR="/path/to/cache"
export KO6ML_MAX_WORKERS="8"
```

#### Custom Component Configuration
```python
# Advanced configuration script
from cognitive_architecture.integration import kobold_cognitive_integrator
from cognitive_architecture.config import CognitiveConfig

# Create custom configuration
config = CognitiveConfig(
    ecan_sti_budget=1500,
    ecan_lti_budget=1500,
    mesh_max_nodes=20,
    reasoning_timeout=60.0,
    meta_learning_enabled=True,
    performance_monitoring=True,
    debug_mode=False
)

# Apply configuration
kobold_cognitive_integrator.configure(config)

# Initialize with custom settings
success = kobold_cognitive_integrator.initialize()
```

## Verification and Testing

### Phase-by-Phase Verification

#### Phase 1: Cognitive Primitives & Hypergraph Encoding
```bash
python test_phase1_requirements.py
```

Expected output:
```
✅ PASSED Scheme Adapters for Agentic Grammar
✅ PASSED Round-Trip Translation Tests
✅ PASSED Tensor Shape Encoding
✅ PASSED Prime Factorization Mapping
✅ PASSED Primitive Transformations
✅ PASSED Hypergraph Visualization

📊 Overall Results: 6/6 tests passed
```

#### Phase 2: ECAN Attention Allocation
```bash
python test_phase2_ecan_requirements.py
```

Expected output:
```
✅ PASSED ECAN-AtomSpace Integration
✅ PASSED ECAN-Task Scheduling Integration
✅ PASSED Attention Allocation Benchmarking
✅ PASSED Real Task Scheduling Flow
✅ PASSED Mesh Topology Documentation
✅ PASSED Integrated System Performance

📊 Overall Results: 6/6 tests passed
```

#### Phase 3: Distributed Mesh Topology
```bash
python test_phase3_requirements.py
```

#### Phase 4: KoboldAI Integration
```bash
python test_phase4_integration.py
```

#### Phase 5: Advanced Reasoning
```bash
python test_phase5_requirements.py
```

#### Phase 6: Meta-Cognitive Learning
```bash
python test_phase6_requirements.py
```

### Performance Verification

#### Benchmark Tests
```python
# Run performance benchmarks
from cognitive_architecture.testing import run_performance_benchmarks

results = run_performance_benchmarks()
print(f"AtomSpace Operations: {results['atomspace_ops_per_sec']} ops/sec")
print(f"ECAN Cycles: {results['ecan_cycles_per_sec']} cycles/sec")
print(f"Reasoning Analysis: {results['reasoning_analyses_per_sec']} analyses/sec")
```

#### Memory Usage Check
```python
# Check memory usage
import psutil
import os

process = psutil.Process(os.getpid())
memory_mb = process.memory_info().rss / 1024 / 1024
print(f"Memory usage: {memory_mb:.1f} MB")

# Should be under 200MB for basic operation
assert memory_mb < 500, f"Memory usage too high: {memory_mb:.1f} MB"
```

## Troubleshooting

### Common Installation Issues

#### Issue 1: Python Version Incompatibility
```
Error: Python 3.7 is not supported
```

**Solution**:
```bash
# Check Python version
python --version

# Install Python 3.8+ from python.org
# Update PATH to use correct Python version
```

#### Issue 2: Missing Dependencies
```
ModuleNotFoundError: No module named 'numpy'
```

**Solution**:
```bash
# Install missing dependencies
pip install numpy websockets aiohttp networkx pytest

# If using conda:
conda install numpy
pip install websockets aiohttp networkx pytest
```

#### Issue 3: Permission Errors (Linux/macOS)
```
PermissionError: [Errno 13] Permission denied
```

**Solution**:
```bash
# Use user installation
pip install --user numpy websockets aiohttp networkx pytest

# Or use virtual environment (recommended)
python -m venv venv
source venv/bin/activate
pip install numpy websockets aiohttp networkx pytest
```

#### Issue 4: Import Errors
```
ImportError: cannot import name 'kobold_cognitive_integrator'
```

**Solution**:
```bash
# Ensure you're in the correct directory
cd ko6ml

# Check if cognitive_architecture directory exists
ls -la cognitive_architecture/

# Verify Python path
python -c "import sys; print(sys.path)"
```

#### Issue 5: Test Failures
```
❌ FAILED Phase X requirements
```

**Solution**:
```bash
# Run tests with verbose output
python test_phaseX_requirements.py -v

# Check logs for specific errors
# Install missing dependencies as needed
# Verify system requirements are met
```

### Performance Issues

#### Issue 1: Slow Processing
**Symptoms**: Processing takes longer than expected

**Solutions**:
- Increase available RAM
- Use SSD storage
- Enable CPU multi-threading
- Check system resource usage

```python
# Monitor performance
from cognitive_architecture.meta_learning import meta_cognitive_engine
status = meta_cognitive_engine.get_meta_cognitive_status()
print(f"Processing time: {status['average_processing_time']:.3f}s")
```

#### Issue 2: High Memory Usage
**Symptoms**: System runs out of memory

**Solutions**:
- Reduce history size in configuration
- Enable memory optimization
- Close other applications

```python
# Configure memory optimization
config = {
    'performance_history_size': 500,  # Reduce from 1000
    'cache_max_size': 50,            # Reduce cache size
    'enable_memory_optimization': True
}
```

#### Issue 3: Network Issues (Distributed Mesh)
**Symptoms**: Mesh nodes can't connect

**Solutions**:
- Check firewall settings
- Verify network connectivity
- Configure proper ports

```python
# Check mesh status
from cognitive_architecture.distributed_mesh import mesh_orchestrator
status = mesh_orchestrator.get_enhanced_mesh_status()
print(f"Active nodes: {status['active_nodes']}")
```

### Getting Help

#### Debug Mode
```python
# Enable debug mode for detailed logging
from cognitive_architecture.integration import kobold_cognitive_integrator
kobold_cognitive_integrator.enable_debug_mode()

# Check system status
status = kobold_cognitive_integrator.get_integration_status()
print(status)
```

#### Log Analysis
```bash
# Check system logs
tail -f ko6ml.log

# Or enable Python logging
python -c "
import logging
logging.basicConfig(level=logging.DEBUG)
# Run your code here
"
```

#### Community Support
- **GitHub Issues**: Report installation problems
- **Documentation**: Check [TECHNICAL_DOCUMENTATION.md](TECHNICAL_DOCUMENTATION.md)
- **Developer Guide**: See [DEVELOPER_GUIDE.md](DEVELOPER_GUIDE.md)

## Post-Installation

### First Steps

#### 1. Verify Installation
```python
# Test basic functionality
from cognitive_architecture.integration import kobold_cognitive_integrator

# Initialize system
success = kobold_cognitive_integrator.initialize()
print(f"Initialization: {'✅ Success' if success else '❌ Failed'}")

# Test input processing
result = kobold_cognitive_integrator.process_input("Hello, cognitive world!")
print(f"Processing result: {len(result)} characters")

# Check system status
status = kobold_cognitive_integrator.get_integration_status()
print(f"All phases active: {all(status[f'phase{i}_active'] for i in range(1, 7))}")
```

#### 2. Run Demonstration
```bash
# Run comprehensive demonstration
python phase6_demonstration.py
```

#### 3. Explore API
```python
# Explore advanced reasoning
from cognitive_architecture.reasoning import advanced_reasoning_engine

story_data = {
    'text': 'The brave knight embarked on a perilous quest to save the kingdom.',
    'characters': [{'name': 'Knight', 'role': 'protagonist'}],
    'events': [{'description': 'Quest begins', 'participants': ['Knight']}]
}

result = advanced_reasoning_engine.reason_about_story(story_data)
print(f"Reasoning confidence: {result.overall_confidence:.2f}")
```

### Next Steps

1. **Read Documentation**: Explore [TECHNICAL_DOCUMENTATION.md](TECHNICAL_DOCUMENTATION.md)
2. **API Reference**: Study [API_DOCUMENTATION.md](API_DOCUMENTATION.md)
3. **Development**: Follow [DEVELOPER_GUIDE.md](DEVELOPER_GUIDE.md)
4. **Configuration**: Customize settings for your use case
5. **Integration**: Integrate with your existing KoboldAI workflows

### Maintenance

#### Updates
```bash
# Update KO6ML
git pull origin main

# Update dependencies
pip install --upgrade numpy websockets aiohttp networkx

# Re-run tests after updates
python -m pytest test_*.py -v
```

#### Monitoring
```python
# Set up monitoring
from cognitive_architecture.meta_learning import meta_cognitive_engine

# Check system health regularly
status = meta_cognitive_engine.get_meta_cognitive_status()
if status['meta_cognitive_active']:
    print("✅ System operating normally")
else:
    print("⚠️ System needs attention")
```

#### Backup
```bash
# Backup configuration and data
cp cognitive_config.json cognitive_config.backup
tar -czf ko6ml_backup.tar.gz cognitive_architecture/ *.py *.md
```

## Conclusion

You now have a fully functional KO6ML Cognitive Architecture installation! The system provides:

- ✅ **Hypergraph Knowledge Representation** (Phase 1)
- ✅ **Economic Attention Allocation** (Phase 2)
- ✅ **Distributed Processing** (Phase 3)
- ✅ **KoboldAI Integration** (Phase 4)
- ✅ **Advanced Reasoning** (Phase 5)
- ✅ **Meta-Cognitive Learning** (Phase 6)

For questions, issues, or contributions, please refer to the documentation or create an issue on GitHub.

**Welcome to the future of AI-assisted writing with cognitive architecture!**