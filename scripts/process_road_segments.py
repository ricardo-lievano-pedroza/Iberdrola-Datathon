import geopandas as gpd
import pandas as pd
import fiona
import numpy as np
from shapely.geometry import Point
from shapely.ops import substring
import shapely
import os
import time
from joblib import Parallel, delayed

# Enable KML driver
fiona.drvsupport.supported_drivers['KML'] = 'rw'

def process_backbone_group(bib_id, group, backbone_geom, traffic_cols, 
                           fusion_small_segment_m=1000, fusion_gap_threshold_m=0):
    """
    Core logic to simplify segments on a single backbone using Sequential Greedy Fusion.
    Includes a second pass for Contiguity Fusion (joining short neighbors).
    """
    if group.empty:
        return []
        
    # 1. SORT: Order by start mile-marker
    group = group.sort_values('start_m').reset_index(drop=True)
    
    # 2. PASS 1: OVERLAP FUSION (Islands)
    islands = []
    current_island_indices = [0]
    current_max_end = group.loc[0, 'end_m']
    
    for i in range(1, len(group)):
        if group.loc[i, 'start_m'] < current_max_end:
            current_island_indices.append(i)
            current_max_end = max(current_max_end, group.loc[i, 'end_m'])
        else:
            islands.append(current_island_indices)
            current_island_indices = [i]
            current_max_end = group.loc[i, 'end_m']
    islands.append(current_island_indices)
    
    initial_results = []
    for idx_list in islands:
        subset = group.iloc[idx_list]
        island_start = subset['start_m'].min()
        island_end = subset['end_m'].max()
        island_len = island_end - island_start
        
        if island_len <= 1e-3: continue
            
        final_traffic = {}
        for col in traffic_cols:
            total_vehicle_demand = (subset[col] * subset['interval_len']).sum()
            final_traffic[col] = total_vehicle_demand / island_len
            
        try:
            final_geom = substring(backbone_geom, island_start, island_end)
        except Exception:
            final_geom = subset.geometry.iloc[0]
            
        entry = {
            'backbone_id': bib_id,
            'geometry': final_geom,
            'length_m': final_geom.length if final_geom else 0,
            'master_start_m': island_start,
            'master_end_m': island_end,
            'original_segment_count': len(subset)
        }
        entry.update(final_traffic)
        initial_results.append(entry)

    # 3. PASS 2: CONTIGUITY FUSION (Join small continuous segments)
    if len(initial_results) <= 1:
        return initial_results
        
    consolidated = []
    curr = initial_results[0]
    
    for i in range(1, len(initial_results)):
        nxt = initial_results[i]
        
        gap = nxt['master_start_m'] - curr['master_end_m']
        is_small = (curr['length_m'] < fusion_small_segment_m) or (nxt['length_m'] < fusion_small_segment_m)
        
        if gap <= fusion_gap_threshold_m and is_small:
            # MERGE: Recalculate weighted traffic
            new_start = curr['master_start_m']
            new_end = nxt['master_end_m']
            total_new_len = curr['length_m'] + nxt['length_m']
            
            if total_new_len > 0:
                for col in traffic_cols:
                    total_demand = (curr[col] * curr['length_m']) + (nxt[col] * nxt['length_m'])
                    curr[col] = total_demand / total_new_len
            
            try:
                curr['geometry'] = substring(backbone_geom, new_start, new_end)
            except:
                pass
                
            curr['length_m'] = curr['geometry'].length
            curr['master_end_m'] = new_end
            curr['original_segment_count'] += nxt['original_segment_count']
        else:
            consolidated.append(curr)
            curr = nxt
    
    consolidated.append(curr)
    return consolidated

def main(
    shp_path="data/raw/traffic/geometria/Geometria_tramos.shp",
    traffic_path="data/processed/road_routes_traffic.parquet",
    kmz_path="data/raw/roads/roads.kmz",
    output_path="data/processed/integrated_road_network.parquet",
    backbone_output_path="./data/processed/backbone_roads.parquet",
    small_segment_length_m=2000,
    fusion_small_segment_m=1000,
    fusion_gap_threshold_m=0,
    target_traffic_column="total_max"
):
    start_time = time.time()
    print(f"🚀 Starting Road network processing (Target: {target_traffic_column})...")
    
    # 1. LOAD DATA
    print("Loading datasets...")
    gdf_segments = gpd.read_file(shp_path)
    if gdf_segments.crs is None:
        gdf_segments.set_crs(3042, inplace=True)
    
    df_info = pd.read_parquet(traffic_path)
    
    # Selection of traffic columns for processing
    if target_traffic_column not in df_info.columns:
        print(f"Warning: {target_traffic_column} not found. Defaulting to 'total_max'.")
        target_traffic_column = "total_max"
        
    # Pick up target total and its corresponding not_short column
    base_name = target_traffic_column.replace("total_", "")
    not_short_col = f"not_short_{base_name}"
    
    traffic_cols = [target_traffic_column]
    if not_short_col in df_info.columns:
        traffic_cols.append(not_short_col)
    
    print(f" - Processing traffic columns: {traffic_cols}")
    
    gdf_segments['id_tramo'] = gdf_segments['id_tramo'].astype(str)
    df_info['tramo'] = df_info['tramo'].astype(str)
    
    # 2. BACKBONE PROCESSING
    print("Processing KMZ backbones...")
    gdf_backbone = gpd.read_file(kmz_path, driver='KML')
    
    # Extract attributes from HTML description
    gdf_backbone["route_segment"] = gdf_backbone["description"].str.extract(
        r"<td>Carretera</td>\s*<td>([^<]+)</td>", expand=False
    )
    gdf_backbone["tipo_via"] = gdf_backbone["description"].str.extract(
        r"<td>Tipo_de_via</td>\s*<td>([^<]+)</td>", expand=False
    )
    gdf_backbone["pk_inicio"] = pd.to_numeric(
        gdf_backbone["description"].str.extract(r"<td>PK_inicio</td>\s*<td>([^<]+)</td>", expand=False), 
        errors="coerce"
    )
    gdf_backbone["pk_fin"] = pd.to_numeric(
        gdf_backbone["description"].str.extract(r"<td>PK_fin</td>\s*<td>([^<]+)</td>", expand=False), 
        errors="coerce"
    )

    gdf_backbone = gdf_backbone.to_crs(gdf_segments.crs)
    gdf_backbone['length_m'] = gdf_backbone.geometry.length
    gdf_backbone = gdf_backbone[gdf_backbone['length_m'] >= small_segment_length_m].copy()
    gdf_backbone = gdf_backbone.rename(columns={'id': 'backbone_id'})
    
    # Keep extracted columns in the final selection
    gdf_backbone = gdf_backbone[['backbone_id', 'geometry', 'length_m', 'route_segment', 'tipo_via', 'pk_inicio', 'pk_fin']]
    
    os.makedirs(os.path.dirname(backbone_output_path), exist_ok=True)
    gdf_backbone.to_parquet(backbone_output_path)

    # 3. MERGE & ASSIGN
    print("Joining traffic data and assigning to backbones...")
    gdf_merged = gdf_segments.merge(df_info, left_on='id_tramo', right_on='tramo')
    gdf_centroids = gdf_merged.copy()
    gdf_centroids['geometry'] = gdf_centroids.geometry.centroid
    
    gdf_assigned = gpd.sjoin_nearest(
        gdf_centroids, 
        gdf_backbone[['backbone_id', 'geometry']], 
        max_distance=500, 
        distance_col="dist_to_backbone"
    )
    
    gdf_merged = gdf_merged.merge(
        gdf_assigned[['id_tramo', 'backbone_id', 'dist_to_backbone']], 
        on='id_tramo', 
        how='left'
    ).dropna(subset=['backbone_id'])

    # 4. VECTORIZED INTERVAL CALCULATION
    print(f"Calculating linear references for {len(gdf_merged)} segments...")
    interval_data = []
    
    for bib_id, group in gdf_merged.groupby('backbone_id'):
        backbone_geom = gdf_backbone[gdf_backbone['backbone_id'] == bib_id].geometry.iloc[0]
        shapely.prepare(backbone_geom)
        
        segment_indices = group.index.tolist()
        geoms = group.geometry.values
        
        start_pts = [Point(g.coords[0]) for g in geoms]
        end_pts = [Point(g.coords[-1]) for g in geoms]
        
        start_dists = shapely.line_locate_point(backbone_geom, start_pts)
        end_dists = shapely.line_locate_point(backbone_geom, end_pts)
        
        for i, idx in enumerate(segment_indices):
            s, e = start_dists[i], end_dists[i]
            s_min, s_max = (s, e) if s < e else (e, s)
            interval_data.append({
                'original_index': idx,
                'start_m': s_min,
                'end_m': s_max,
                'interval_len': s_max - s_min
            })
            
    df_intervals = pd.DataFrame(interval_data).set_index('original_index')
    gdf_merged = gdf_merged.join(df_intervals[['start_m', 'end_m', 'interval_len']])

    # 5. PARALLEL FUSION (Sequential + Contiguity)
    print(f"Fusing and consolidating segments (Threshold={fusion_small_segment_m}m)...")
    backbone_groups = [
        (bid, grp, gdf_backbone[gdf_backbone['backbone_id'] == bid].geometry.iloc[0], traffic_cols, 
         fusion_small_segment_m, fusion_gap_threshold_m)
        for bid, grp in gdf_merged.groupby('backbone_id')
    ]
    
    results_nested = Parallel(n_jobs=-1)(
        delayed(process_backbone_group)(*args) for args in backbone_groups
    )
    processed_groups = [item for sublist in results_nested for item in sublist]

    # 6. SAVE RESULTS
    print("Consolidating and saving final network...")
    gdf_final = gpd.GeoDataFrame(processed_groups, crs=gdf_segments.crs)
    if not gdf_final.empty:
        gdf_final.sort_values(['backbone_id', 'master_start_m'], inplace=True)
        gdf_final.insert(0, 'segment_id', range(len(gdf_final)))
    
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    # Calculate percentage for fusion segments
    if not gdf_final.empty and not_short_col in gdf_final.columns:
        final_pct_col = f"pct_not_short_{base_name}"
        gdf_final[final_pct_col] = (gdf_final[not_short_col] / gdf_final[target_traffic_column]).fillna(0)
    
    gdf_final.to_parquet(output_path)
    
    print(f"✨ Process Complete!")
    print(f"   Output: {len(gdf_final)} super-segments.")
    print(f"   Total Time: {time.time()-start_time:.1f}s")

if __name__ == "__main__":
    main()
