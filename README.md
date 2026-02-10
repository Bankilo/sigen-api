# sigen

Async Python client for the Sigenergy cloud API (internal endpoints).

Replaces the unmaintained PyPI `sigen` package (v3.0.2) with a properly structured package, better error handling, and the same public interface.

## Installation

```bash
pip install -e /path/to/sigen-api
```

## Usage

```python
from sigen import Sigen

api = Sigen("user@example.com", "password", region="eu")
await api.async_initialize()

# Energy flow
flow = await api.get_energy_flow()

# Operational modes
mode = await api.get_operational_mode()
modes = await api.get_operational_modes()
await api.set_operational_mode(mode=1, profile_id=-1)

# Dynamic mode methods (created during init)
await api.set_operational_mode_tou()

# Smart loads
loads = await api.get_smart_loads()
await api.set_smart_load_state(load_path=123, state=1)
```

## Regions

`eu`, `cn`, `apac`, `us`
