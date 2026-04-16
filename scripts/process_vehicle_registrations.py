import os
import requests
import zipfile
import polars as pl
from datetime import datetime

# DGT Metadata: [index, field_name, length]
DGT_FIELDS = [
    [1, 'FEC_MATRICULA', 8], [2, 'COD_CLASE_MAT', 1], [3, 'FEC_TRAMITACION', 8], 
    [4, 'MARCA_ITV', 30], [5, 'MODELO_ITV', 22], [6, 'COD_PROCEDENCIA_ITV', 1], 
    [7, 'BASTIDOR_ITV', 21], [8, 'COD_TIPO', 2], [9, 'COD_PROPULSION_ITV', 1], 
    [10, 'CILINDRADA_ITV', 5], [11, 'POTENCIA_ITV', 6], [12, 'TARA', 6], 
    [13, 'PESO_MAX', 6], [14, 'NUM_PLAZAS', 3], [15, 'IND_PRECINTO', 2], 
    [16, 'IND_EMBARGO', 2], [17, 'NUM_TRANSMISIONES', 2], [18, 'NUM_TITULARES', 2], 
    [19, 'LOCALIDAD_VEHICULO', 24], [20, 'COD_PROVINCIA_VEH', 2], [21, 'COD_PROVINCIA_MAT', 2], 
    [22, 'CLAVE_TRAMITE', 1], [23, 'FEC_TRAMITE', 8], [24, 'CODIGO_POSTAL', 5], 
    [25, 'FEC_PRIM_MATRICULACION', 8], [26, 'IND_NUEVO_USADO', 1], [27, 'PERSONA_FISICA_JURIDICA', 1], 
    [28, 'CODIGO_ITV', 9], [29, 'SERVICIO', 3], [30, 'COD_MUNICIPIO_INE_VEH', 5], 
    [31, 'MUNICIPIO', 30], [32, 'KW_ITV', 7], [33, 'NUM_PLAZAS_MAX', 3], 
    [34, 'CO2_ITV', 5], [35, 'RENTING', 1], [36, 'COD_TUTELA', 1], 
    [37, 'COD_POSESION', 1], [38, 'IND_BAJA_DEF', 1], [39, 'IND_BAJA_TEMP', 1], 
    [40, 'IND_SUSTRACCION', 1], [41, 'BAJA_TELEMATICA', 11], [42, 'TIPO_ITV', 25], 
    [43, 'VARIANTE_ITV', 25], [44, 'VERSION_ITV', 35], [45, 'FABRICANTE_ITV', 70], 
    [46, 'MASA_ORDEN_MARCHA_ITV', 6], [47, 'MASA_MÁXIMA_TECNICA_ADMISIBLE_ITV', 6], 
    [48, 'CATEGORÍA_HOMOLOGACIÓN_EUROPEA_ITV', 4], [49, 'CARROCERIA', 4], [50, 'PLAZAS_PIE', 3], 
    [51, 'NIVEL_EMISIONES_EURO_ITV', 8], [52, 'CONSUMO', 4], [53, 'CLASIFICACIÓN_REGLAMENTO_VEHICULOS_IT', 4], 
    [54, 'CATEGORÍA_VEHÍCULO_ELÉCTRICO', 4], [55, 'AUTONOMÍA_VEHÍCULO_ELÉCTRICO', 6], 
    [56, 'MARCA_VEHÍCULO_BASE', 30], [57, 'FABRICANTE_VEHÍCULO_BASE', 50], [58, 'TIPO_VEHÍCULO_BASE', 35], 
    [59, 'VARIANTE_VEHÍCULO_BASE', 25], [60, 'VERSIÓN_VEHÍCULO_BASE', 35], [61, 'DISTANCIA_EJES_12_ITV', 4], 
    [62, 'VIA_ANTERIOR_ITV', 4], [63, 'VIA_POSTERIOR_ITV', 4], [64, 'TIPO_ALIMENTACION_ITV', 1], 
    [65, 'CONTRASEÑA_HOMOLOGACION_ITV', 25], [66, 'ECO_INNOVACION_ITV', 1], [67, 'REDUCCION_ECO_ITV', 4], 
    [68, 'CODIGO_ECO_ITV', 25], [69, 'FEC_PROCESO', 8]
]

def process_zip_to_consolidated_parquet(dir_zip, campos, output_file, columns=None):
    """
    Combines decompression and fixed-width split into a single Polars pipeline.
    Optimized to filter columns early to save memory.
    """
    if columns is None:
        columns = ["FEC_MATRICULA", "MARCA_ITV", "MODELO_ITV", "COD_TIPO", 
                   "COD_PROPULSION_ITV", "CATEGORÍA_VEHÍCULO_ELÉCTRICO", "CLAVE_TRAMITE"]

    all_dataframes = []
    col_names = [campo[1] for campo in campos]
    col_specs = []
    
    current_pos = 0
    for campo in campos:
        length = campo[2]
        col_specs.append((current_pos, current_pos + length))
        current_pos += length

    if not os.path.exists(dir_zip):
        print(f"Error: Directory {dir_zip} does not exist.")
        return None

    zip_files = sorted([f for f in os.listdir(dir_zip) if f.endswith('.zip')])
    if not zip_files:
        print(f"No ZIP files found in {dir_zip}")
        return None

    print(f"Procesando {len(zip_files)} ficheros ZIP...")
    
    for zip_name in zip_files:
        zip_path = os.path.join(dir_zip, zip_name)
        
        try:
            with zipfile.ZipFile(zip_path, 'r') as z:
                internal_file = z.namelist()[0]
                with z.open(internal_file) as f:
                    try:
                        lines = f.read().decode('latin-1').splitlines()[1:]
                    except Exception as e:
                        print(f"  - Error decodificando {zip_name}: {e}")
                        continue
                    
                    rows = []
                    for line in lines:
                        if not line.strip(): continue
                        row = [line[start:end].strip() for start, end in col_specs]
                        rows.append(row)
                    
                    if not rows: continue
                    
                    df = pl.DataFrame(rows, schema=col_names, orient="row")
                    df = df.select(columns)
                    all_dataframes.append(df)
            print(f"  - Procesado {zip_name}")
        except Exception as e:
            print(f"  - Error procesando {zip_name}: {e}")

    if all_dataframes:
        print("Concatenando dataframes and guardando...")
        final_df = pl.concat(all_dataframes)
        
        # Ensure output directory exists
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        
        final_df.write_parquet(output_file)
        print(f"Guardado exitoso en {output_file}")
        return final_df
    else:
        print("No se pudieron generar dataframes.")
        return None

def main(
    dir_zip="data/raw/vehicle_registrations", 
    output_parquet="data/processed/ev_registrations.parquet"
):
    # Process
    process_zip_to_consolidated_parquet(dir_zip, DGT_FIELDS, output_parquet)

if __name__ == "__main__":
    main()
