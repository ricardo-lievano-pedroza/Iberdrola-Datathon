import sys
import os
import tomllib
import importlib.util

# Add scripts directory to path
SCRIPTS_DIR = os.path.join(os.path.dirname(__file__), 'scripts')
sys.path.append(SCRIPTS_DIR)

def load_config(config_path="config.toml"):
    """Loads the central configuration."""
    if not os.path.exists(config_path):
        print(f"Error: Configuration '{config_path}' not found.")
        sys.exit(1)
    with open(config_path, "rb") as f:
        return tomllib.load(f)

def run_standardization_step(step_name, module_name):
    """Dynamically imports and runs a standardization script's main function."""
    print(f"\n>>> RUNNING STANDARDIZATION: {step_name} ({module_name}.py)")
    
    script_path = os.path.join(SCRIPTS_DIR, f"{module_name}.py")
    if not os.path.exists(script_path):
        print(f"Error: Script {script_path} not found.")
        return False

    spec = importlib.util.spec_from_file_location(module_name, script_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    if hasattr(module, 'main'):
        try:
            module.main()
            return True
        except Exception as e:
            print(f"FAILED step '{step_name}': {e}")
            return False
    else:
        print(f"Error: Module '{module_name}' has no main() function.")
        return False

def main():
    print("=== Iberdrola Datathon: Data Standardization Orchestrator ===\n")
    
    # Required order: roads, traffic, chargers, electric_capacity, vehicle_registrations, gas_stations
    standardization_steps = [
        ("Roads", "standardize_roads"),
        ("Traffic", "standardize_traffic"),
        ("EV Chargers", "standardize_chargers"),
        ("Electric Capacity", "standardize_electric_capacity"),
        ("Vehicle Registrations", "standardize_vehicle_registrations"),
        ("Gas Stations", "standardize_gas_stations")
    ]
    
    for step_label, script_name in standardization_steps:
        if not run_standardization_step(step_label, script_name):
            print(f"\nStandardization ABORTED at step: {step_label}")
            sys.exit(1)

    print("\n✅ All standardization steps completed successfully.")
    print("Files available in: data/standardized/")

if __name__ == "__main__":
    main()
