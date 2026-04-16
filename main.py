import sys
import os
import tomllib  # Built-in in Python 3.11+
from pathlib import Path

# Add scripts directory to path to allow direct imports
sys.path.append(os.path.join(os.path.dirname(__file__), 'scripts'))

try:
    import process_chargers
    import process_vehicle_registrations
    import process_gas_stations
    import process_electric_capacity
    import merge_traffic_data
    import process_road_segments
    import analyze_charging_sites_proximity
    import analyze_gas_stations_proximity
except ImportError as e:
    print(f"Error importing scripts: {e}")
    sys.exit(1)

def load_config(config_path="config.toml"):
    """Loads and returns the TOML configuration."""
    if not os.path.exists(config_path):
        print(f"Error: Configuration file '{config_path}' not found at {os.getcwd()}")
        sys.exit(1)
    
    with open(config_path, "rb") as f:
        return tomllib.load(f)

def run_step(step_name, config, force=False):
    """
    Executes a single processing step with configuration injection.
    Includes smart skipping and dependency checking.
    """
    step_config = config['steps'].get(step_name)
    if not step_config:
        print(f"Error: Configuration for step '{step_name}' not found under [steps].")
        return False

    # 1. Dependency Check
    dependencies = step_config.get('depends_on', [])
    for dep in dependencies:
        if not os.path.exists(dep):
            print(f"ERROR: Prerequisite missing for '{step_name}'. Please ensure raw data is downloaded and prior steps are run. Missing: {dep}")
            return False

    # 2. Smart Skipping Logic
    output_path = step_config.get('output_path')
    if output_path and os.path.exists(output_path) and not force:
        print(f"Skipping '{step_name}': Output already exists at {output_path}")
        return True

    print(f"\n>>> Executing Step: {step_name}")
    
    try:
        if step_name == "chargers":
            process_chargers.main(
                raw_xml_path=step_config['raw_path'],
                parquet_output_path=step_config['output_path']
            )
        elif step_name == "vehicle_registrations":
            process_vehicle_registrations.main(
                dir_zip=step_config['raw_dir'],
                output_parquet=step_config['output_path']
            )
        elif step_name == "gas_stations":
            process_gas_stations.main(
                raw_path=step_config['raw_path'],
                output_path=step_config['output_path']
            )
        elif step_name == "electric_capacity":
            process_electric_capacity.main(
                raw_dir=step_config['raw_dir'],
                output_path=step_config['output_path'],
                files=step_config['files']
            )
        elif step_name == "traffic":
            merge_traffic_data.main(
                input_dir=step_config['raw_dir'],
                output_path=step_config['output_path']
            )
        elif step_name == "segments":
            process_road_segments.main(
                shp_path=step_config['shp_path'],
                traffic_path=step_config['traffic_path'],
                kmz_path=step_config['kmz_path'],
                output_path=step_config['output_path'],
                backbone_output_path=step_config['backbone_output_path'],
                small_segment_length_m=step_config['small_segment_length_m']
            )
        elif step_name == "proximity":
            analyze_charging_sites_proximity.main(
                charging_points_path=step_config['charging_points_path'],
                road_network_path=step_config['road_network_path'],
                backbone_roads_path=step_config['backbone_roads_path'],
                output_path=step_config['output_path'],
                max_distance=step_config['max_distance']
            )
        elif step_name == "gas_stations_proximity":
            analyze_gas_stations_proximity.main(
                raw_path=step_config['raw_path'],
                road_network_path=step_config['road_network_path'],
                backbone_roads_path=step_config['backbone_roads_path'],
                output_path=step_config['output_path'],
                max_distance=step_config['max_distance']
            )
        else:
            print(f"Error: Manual glue-code for step '{step_name}' is missing in main.py.")
            return False
            
        return True
    except Exception as e:
        print(f"CRITICAL FAILURE in '{step_name}': {e}")
        return False

def main():
    """Main orchestrator entry point for processing."""
    print("=== Iberdrola Datathon: Data Processing Orchestrator ===\n")
    
    config = load_config()
    
    # Read execution settings
    execution = config.get('process_execution', config.get('execution', {}))
    steps_requested = execution.get('steps', ["all"])
    force_run = execution.get('force', False)

    # Definitive order of processing steps
    canonical_order = [
        "chargers", 
        "vehicle_registrations", 
        "gas_stations",
        "electric_capacity", 
        "traffic", 
        "segments", 
        "proximity", 
        "gas_stations_proximity"
    ]
    
    if "all" in steps_requested:
        steps_to_run = canonical_order
    else:
        steps_to_run = [s for s in canonical_order if s in steps_requested]
        
        # Check for user typos
        invalid = [s for s in steps_requested if s not in canonical_order and s != "all"]
        if invalid:
            print(f"Warning: Disregarding unknown step names found in config: {invalid}")

    if not steps_to_run:
        print("No valid processing steps selected for execution.")
        return

    print(f"Sequence: {', '.join(steps_to_run)}")
    print(f"Force Flag: {force_run}")

    for step in steps_to_run:
        if not run_step(step, config, force=force_run):
            print(f"\nPipeline ABORTED at step: {step}")
            sys.exit(1)

    print("\n=== All processing tasks finished successfully ===")

if __name__ == "__main__":
    main()
