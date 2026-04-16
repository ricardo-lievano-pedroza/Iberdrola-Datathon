import sys
import os
import tomllib
from pathlib import Path

# Add scripts directory to path
sys.path.append(os.path.join(os.path.dirname(__file__), 'scripts'))

try:
    import data_acquisition
except ImportError as e:
    print(f"Error importing scripts: {e}")
    sys.exit(1)

# ==========================================
# FIXED ACQUISITION CONFIGURATION (Code-Driven)
# ==========================================

RAW_BASE_DIR = "data/raw"

CONFIG_MAPPING = {
    "roads": {
        "url": "https://mapas.fomento.gob.es/arcgis2/rest/services/Hermes/0_CARRETERAS/MapServer/19/query?where=GEOM%20is%20not%20null&outFields=*&f=kmz",
        "output_path": os.path.join(RAW_BASE_DIR, "roads", "roads.kmz")
    },
    "traffic": {
        "geom_url_base": "https://movilidad-opendata.mitma.es/estudios_rutas/geometria/Geometria_tramos_2023_2024/Geometria_tramos",
        "info_url_base": "https://movilidad-opendata.mitma.es/estudios_rutas/informacion_tramo/",
        "info_files": [
            "20240331_Tramos_info_odmatrix.csv.gz",
            "20240824_Tramos_info_odmatrix.csv.gz",
            "20240827_Tramos_info_odmatrix.csv.gz",
            "20241016_Tramos_info_odmatrix.csv.gz",
            "20241019_Tramos_info_odmatrix.csv.gz"
        ],
        "base_dir": os.path.join(RAW_BASE_DIR, "traffic")
    },
    "vehicle_registrations": {
        "raw_dir": os.path.join(RAW_BASE_DIR, "vehicle_registrations")
    },
    "chargers": {
        "url": "https://infocar.dgt.es/datex2/v3/miterd/EnergyInfrastructureTablePublication/electrolineras.xml",
        "raw_path": os.path.join(RAW_BASE_DIR, "chargers", "chargers.xml")
    },
    "electric_capacity": {
        "datasets": [
            { "label": "Iberdrola", "url": "https://www.i-de.es/documents/d/guest/2026_04_01_r1-001_demanda-1-", "filename": "Iberdrola_2026_04_01.xlsx" },
            { "label": "Endesa", "url": "https://www.edistribucion.com/content/dam/edistribucion/conexion-a-la-red/descargables/nodos/generacion/202604/2026_04_01_R1299_generación.xlsx", "filename": "Endesa_2026_04_01.xlsx" },
            { "label": "Viesgo", "url": "https://storage.googleapis.com/apdes-prd-interactivemap-resources/network-capacity-map/v2/viesgo/doc_generation/2026_04_01_R1005_generacion.xlsx", "filename": "Viesgo_2026_04_01.xlsx" }
        ],
        "raw_dir": os.path.join(RAW_BASE_DIR, "electric_capacity")
    },
    "gas_stations": {
        "url": "https://sedeaplicaciones.minetur.gob.es/ServiciosRESTCarburantes/PreciosCarburantes/EstacionesTerrestres/",
        "raw_path": os.path.join(RAW_BASE_DIR, "gas_stations", "gas_stations.json")
    }
}

def load_config(config_path="config.toml"):
    """Loads and returns the TOML configuration."""
    if not os.path.exists(config_path):
        print(f"Error: Configuration file '{config_path}' not found.")
        sys.exit(1)
    with open(config_path, "rb") as f:
        return tomllib.load(f)

def run_step(step_name, config, force=False):
    """Executes a single download step using hardcoded paths and URLs."""
    acq_config = CONFIG_MAPPING.get(step_name)
    if not acq_config:
        print(f"Error: No acquisition details for step '{step_name}'.")
        return False

    # Smart Skipping Logic
    check_path = acq_config.get('raw_path') or acq_config.get('raw_dir') or acq_config.get('output_path')
    if not check_path:
         # Fallback for complex steps like 'traffic'
        if step_name == 'traffic':
            check_path = os.path.join(acq_config['base_dir'], "geometria", "Geometria_tramos.shp")

    if check_path and os.path.exists(check_path) and not force:
        if os.path.isdir(check_path) and os.listdir(check_path):
            print(f"Skipping '{step_name}': Raw data already exists in {check_path}")
            return True
        elif os.path.isfile(check_path):
            print(f"Skipping '{step_name}': Raw file already exists at {check_path}")
            return True

    print(f"\n>>> Executing Download: {step_name}")
    
    try:
        if step_name == "roads":
            return data_acquisition.fetch_roads(
                url=acq_config['url'],
                output_path=acq_config['output_path']
            )
        elif step_name == "traffic":
            return data_acquisition.fetch_traffic(
                geom_url_base=acq_config['geom_url_base'],
                info_url_base=acq_config['info_url_base'],
                info_files=acq_config['info_files'],
                base_dir=acq_config['base_dir']
            )
        elif step_name == "vehicle_registrations":
            # Get range from config for flexibility
            reg_config = config['steps'].get('vehicle_registrations', {})
            return data_acquisition.fetch_vehicle_registrations(
                ano_inicio=reg_config.get('ano_inicio', 2015),
                mes_inicio=reg_config.get('mes_inicio', 1),
                ano_fin=reg_config.get('ano_fin'),
                mes_fin=reg_config.get('mes_fin'),
                output_dir=acq_config['raw_dir']
            )
        elif step_name == "chargers":
            return data_acquisition.fetch_chargers(
                url=acq_config['url'],
                output_path=acq_config['raw_path']
            )
        elif step_name == "electric_capacity":
            return data_acquisition.fetch_electric_capacity(
                datasets=acq_config['datasets'],
                base_dir=acq_config['raw_dir']
            )
        elif step_name == "gas_stations":
            return data_acquisition.fetch_gas_stations(
                url=acq_config['url'],
                output_path=acq_config['raw_path']
            )
        else:
            return False
    except Exception as e:
        print(f"CRITICAL FAILURE in download step '{step_name}': {e}")
        return False

def main():
    print("=== Iberdrola Datathon: Data Acquisition Orchestrator ===\n")
    config = load_config()
    
    exec_config = config.get('download_execution', {})
    steps_requested = exec_config.get('steps', ["all"])
    force_run = exec_config.get('force', False)

    canonical_order = ["roads", "traffic", "vehicle_registrations", "chargers", "electric_capacity", "gas_stations"]
    
    if "all" in steps_requested:
        steps_to_run = canonical_order
    else:
        steps_to_run = [s for s in canonical_order if s in steps_requested]

    if not steps_to_run:
        print("No valid download steps selected for execution.")
        return

    print(f"Sequence: {', '.join(steps_to_run)}")
    print(f"Force Flag: {force_run}")

    for step in steps_to_run:
        if not run_step(step, config, force=force_run):
            print(f"\nDownload ABORTED at step: {step}")
            sys.exit(1)

    print("\n=== All downloads finished successfully ===")

if __name__ == "__main__":
    main()
