from pathlib import Path
import yaml

def find_project_root() -> Path:
    """Find the project root directory by looking for src/hcp_pipeline/config/settings.yaml"""
    current_dir = Path.cwd()
    
    # Check if we're in the project root
    if (current_dir / "src" / "hcp_pipeline" / "config" / "settings.yaml").exists():
        return current_dir
    
    # Check if we're in the src directory
    if (current_dir / "hcp_pipeline" / "config" / "settings.yaml").exists():
        return current_dir.parent
    
    
    # Check if we're in the hcp_pipeline directory
    if (current_dir / "config" / "settings.yaml").exists():
        return current_dir.parents[1]
    
    # Fallback: try to find by going up directories
    for parent in current_dir.parents:
        if (parent / "src" / "hcp_pipeline" / "config" / "settings.yaml").exists():
            return parent
    
    # If all else fails, return current directory
    return current_dir

def get_settings_path() -> Path:
    """Get the correct path to settings.yaml regardless of where the script is run from"""
    project_root = find_project_root()
    return project_root / "src" / "hcp_pipeline" / "config" / "settings.yaml"

def load_settings(path: str|Path) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def load_project_settings() -> dict:
    """Load settings from the project's settings.yaml file"""
    return load_settings(get_settings_path())
