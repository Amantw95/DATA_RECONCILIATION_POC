import yaml
import os

def load_config(config_name):
    """Loads YAML configuration file dynamically."""
    config_path = os.path.join("configs", f"{config_name}.yaml")
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"❌ Config file {config_path} not found!")
    
    with open(config_path, "r") as file:
        config = yaml.safe_load(file)
    
    return config
