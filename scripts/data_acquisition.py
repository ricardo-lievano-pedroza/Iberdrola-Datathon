import os
import requests
import gzip
import shutil
import zipfile
import urllib3
import ssl
from datetime import datetime

# Suppress insecure request warnings for specific government servers
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

class TLSAdapter(requests.adapters.HTTPAdapter):
    """
    Custom adapter to force a lower security level for SSL/TLS connections.
    Required for certain Spanish government servers (minetur.gob.es).
    """
    def init_poolmanager(self, connections, maxsize, block=False):
        ctx = ssl.create_default_context()
        ctx.set_ciphers('DEFAULT@SECLEVEL=1')
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        self.poolmanager = urllib3.poolmanager.PoolManager(
            num_pools=connections,
            maxsize=maxsize,
            block=block,
            ssl_context=ctx
        )

def download_file(url, output_path, label=None, decompress=False, use_tls_adapter=False):
    """
    Unified utility to download a file from a URL.
    Supports decompression of .gz files and custom TLS adapters.
    """
    lbl = f"[{label}] " if label else ""
    print(f"{lbl}Downloading from: {url}")
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    # If decompressing, download to a temporary .gz file first
    download_path = output_path + ".gz" if decompress else output_path
    
    try:
        session = requests.Session()
        if use_tls_adapter:
            session.mount("https://", TLSAdapter())
            
        with session.get(url, stream=True, timeout=300, headers=headers, verify=not use_tls_adapter) as r:
            r.raise_for_status()
            with open(download_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
        
        if decompress:
            print(f"{lbl}Decompressing {download_path}...")
            with gzip.open(download_path, 'rb') as f_in:
                with open(output_path, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
            os.remove(download_path)
            
        print(f"{lbl}Successfully saved to {output_path}")
        return True
    except Exception as e:
        print(f"{lbl}Error: {e}")
        if decompress and os.path.exists(download_path):
            os.remove(download_path)
        return False

def fetch_roads(url, output_path):
    """Downloads the road network KMZ file."""
    return download_file(url, output_path, label="Roads")

def fetch_traffic(geom_url_base, info_url_base, info_files, base_dir):
    """Downloads road routes geometry and traffic info CSVs."""
    success = True
    
    # 1. Geometry Files
    geom_extensions = [".cpg", ".dbf", ".prj", ".shp", ".shx"]
    geom_dir = os.path.join(base_dir, "geometria")
    for ext in geom_extensions:
        url = f"{geom_url_base}{ext}"
        filename = f"Geometria_tramos{ext}"
        if not download_file(url, os.path.join(geom_dir, filename), label=f"Geometry {ext}"):
            success = False
            
    # 2. Traffic Info Files (GZipped)
    info_dir = os.path.join(base_dir, "informacion_tramo")
    for filename in info_files:
        url = f"{info_url_base}{filename}"
        date_prefix = filename.split('_')[0]
        final_filename = f"{date_prefix}_info_tramo.csv"
        if not download_file(url, os.path.join(info_dir, final_filename), label="Traffic Info", decompress=True):
            success = False
            
    return success

def fetch_vehicle_registrations(ano_inicio, mes_inicio, ano_fin, mes_fin, output_dir):
    """Downloads monthly ZIP files from DGT site."""
    if ano_fin is None: ano_fin = datetime.now().year
    if mes_fin is None: mes_fin = datetime.now().month
    
    os.makedirs(output_dir, exist_ok=True)
    success = True
    
    for ano in range(ano_inicio, ano_fin + 1):
        for mes in range(1, 13):
            if (ano == ano_inicio and mes >= mes_inicio) or (ano == ano_fin and mes <= mes_fin) or (ano_inicio < ano < ano_fin):
                mes_pad = str(mes).zfill(2)
                url = f"https://www.dgt.es/microdatos/salida/{ano}/{mes}/vehiculos/matriculaciones/export_mensual_mat_{ano}{mes_pad}.zip"
                filename = f"{ano}_{mes_pad}.zip"
                path = os.path.join(output_dir, filename)
                
                # Check for existing to support smart skipping within the loop
                if os.path.exists(path):
                    print(f"[Registrations] Skipping {filename} (exists)")
                    continue
                    
                if not download_file(url, path, label=f"Regs {ano}-{mes_pad}"):
                    success = False
    return success

def fetch_chargers(url, output_path):
    """Downloads the EV charging points XML."""
    return download_file(url, output_path, label="Chargers")

def fetch_electric_capacity(datasets, base_dir):
    """Downloads capacity datasets for various providers."""
    success = True
    for ds in datasets:
        output_path = os.path.join(base_dir, ds["filename"])
        if not download_file(ds["url"], output_path, label=f"Capacity {ds['label']}"):
            success = False
    return success

def fetch_gas_stations(url, output_path):
    """Downloads the gas stations data using special TLS adapter."""
    # Note: We save the raw JSON/Parquet directly. 
    # Current implementation in original script saves as parquet immediately.
    # We'll just fetch the content for now.
    return download_file(url, output_path, label="Gas Stations", use_tls_adapter=True)
