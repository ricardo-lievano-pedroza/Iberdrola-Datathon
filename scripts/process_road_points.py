import geopandas as gpd
import pandas as pd
import fiona
import numpy as np
import shapely
import os
import time

# Enable KML driver
fiona.drvsupport.supported_drivers['KML'] = 'rw'

def discretize_line_to_points(gdf, id_col, interval=200):
    """
    Converts LineStrings into a series of Points along their actual path.
    Each point stores the distance from the line's start (m_ref).
    """
    points_data = []
    for _, row in gdf.iterrows():
        geom = row.geometry
        if geom is None or geom.is_empty:
            continue
        
        # Calculate distances at intervals
        length = geom.length
        if length <= 0:
            distances = [0.0]
        else:
            distances = np.arange(0, length, interval)
            # Ensure we include the end of the line
            if len(distances) == 0 or distances[-1] < length:
                distances = np.append(distances, length)
            
        for d in distances:
            pt = geom.interpolate(d)
            # Carry over all backbone attributes
            entry = row.to_dict()
            entry['geometry'] = pt
            entry['m_ref'] = d
            points_data.append(entry)
            
    return gpd.GeoDataFrame(points_data, crs=gdf.crs)

def main(
    shp_path="./data/raw/road_routes/geometria/Geometria_tramos.shp",
    traffic_path="./data/processed/road_routes_traffic.parquet",
    kmz_path="./data/raw/roads/query.kmz",
    output_path="./data/processed/road_backbone_points.parquet",
    target_traffic_column="total_max",
    sampling_interval_m=200,
    buffer_radius_m=50
):
    start_time = time.time()
    print(f"🚀 Starting Road Point Processing (Buffer={buffer_radius_m}m, Interval={sampling_interval_m}m)...")
    
    # Validation: sampling interval must be larger than buffer to avoid overlapping influence zones skipping points
    assert sampling_interval_m > buffer_radius_m, "sampling_interval_m must be greater than buffer_radius_m"
    
    # 1. LOAD DATA
    print("Loading datasets...")
    gdf_segments = gpd.read_file(shp_path)
    if gdf_segments.crs is None:
        gdf_segments.set_crs(3042, inplace=True)
    
    df_info = pd.read_parquet(traffic_path)
    
    # Determine traffic columns to aggregate
    if target_traffic_column not in df_info.columns:
        print(f"Warning: {target_traffic_column} not found. Defaulting to 'total_max'.")
        target_traffic_column = "total_max"
        
    base_name = target_traffic_column.replace("total_", "")
    not_short_col = f"not_short_{base_name}"
    
    traffic_cols = [target_traffic_column]
    if not_short_col in df_info.columns:
        traffic_cols.append(not_short_col)
    
    print(f" - Aggregating traffic columns: {traffic_cols}")
    
    gdf_segments['id_tramo'] = gdf_segments['id_tramo'].astype(str)
    df_info['tramo'] = df_info['tramo'].astype(str)
    
    # Merge traffic info with segments early
    gdf_merged = gdf_segments.merge(df_info, left_on='id_tramo', right_on='tramo')
    gdf_merged['seg_length'] = gdf_merged.geometry.length
    
    # 2. BACKBONE PROCESSING
    print("Processing KMZ backbones...")
    gdf_backbone = gpd.read_file(kmz_path, driver='KML')
    
    # Extract attributes from HTML description (preserving backbone metadata)
    gdf_backbone["route_name"] = gdf_backbone["description"].str.extract(
        r"<td>Carretera</td>\s*<td>([^<]+)</td>", expand=False
    )

    gdf_backbone["tipo_via"] = gdf_backbone["description"].str.extract(
        r"<td>Tipo_de_via</td>\s*<td>([^<]+)</td>", expand=False
    )
    
    gdf_backbone = gdf_backbone.rename(columns={'id': 'backbone_id'})
    
    gdf_backbone = gdf_backbone.to_crs(gdf_segments.crs)
    
    # Discretize Backbone to Points
    print(f"Discretizing backbones into points (Interval={sampling_interval_m}m)...")
    gdf_backbone_pts = discretize_line_to_points(gdf_backbone, 'backbone_id', interval=sampling_interval_m)
    
    # Create concatenated point_id: backbone_id + _ + index
    gdf_backbone_pts['point_idx'] = gdf_backbone_pts.groupby('backbone_id').cumcount()
    gdf_backbone_pts['point_id'] = (
        gdf_backbone_pts['backbone_id'].astype(str) + 
        "_" + 
        gdf_backbone_pts['point_idx'].astype(str)
    )
    
    # 3. SPATIAL BUFFER & JOIN
    print(f"Buffering backbone points and joining with segment lines...")
    # Buffer each point to create the "influence zone"
    gdf_pts_buffered = gdf_backbone_pts.copy()
    gdf_pts_buffered['geometry'] = gdf_pts_buffered.geometry.buffer(buffer_radius_m)
    
    # Join with segments (intersects)
    # Include backbone info and segment length for filtering
    joined = gpd.sjoin(
        gdf_pts_buffered[['point_id', 'backbone_id', 'point_idx', 'geometry']], 
        gdf_merged[['id_tramo', 'geometry', 'seg_length'] + traffic_cols], 
        how='inner', 
        predicate='intersects'
    )
    
    # FILTER: Only keep segments that:
    # 1. Match at least one neighbor point on the same backbone (consecutive)
    # 2. OR are shorter than the sampling interval (bypass for very short segments)
    if not joined.empty:
        joined['has_neighbor'] = joined.groupby(['id_tramo', 'backbone_id'])['point_idx'].transform(
            lambda x: x.isin(x + 1) | x.isin(x - 1)
        )
        joined = joined[joined['has_neighbor']].copy()
        joined = joined.drop(columns=['has_neighbor', 'seg_length'])
    
    if joined.empty:
        print("❌ No segments were matched. Check CRS and buffer radius.")
        return

    # 4. SUM TRAFFIC PER POINT
    print(f"Summing traffic from matched segments ({len(joined)} total matches)...")
    traffic_summary = joined.groupby('point_id')[traffic_cols].sum().reset_index()
    
    # Merge summed traffic back to the original points
    gdf_final = gdf_backbone_pts.merge(traffic_summary, on='point_id', how='left')
    # Fill NaN traffic with 0 for points that had no matches
    gdf_final[traffic_cols] = gdf_final[traffic_cols].fillna(0)
    
    # Add count of segments if useful
    segment_counts = joined.groupby('point_id')['id_tramo'].nunique().rename('segment_count').reset_index()
    gdf_final = gdf_final.merge(segment_counts, on='point_id', how='left').fillna({'segment_count': 0})

    # 5. GAP FILLING (Interpolation for single-point gaps)
    print("Interpolating single-point gaps...")
    gdf_final['is_interpolated'] = False
    gdf_final = gdf_final.sort_values(['backbone_id', 'point_idx'])
    
    cols_to_interpolate = traffic_cols + ['segment_count']
    # Use a combined mask to track any row that gets interpolated
    interpolated_mask = pd.Series(False, index=gdf_final.index)
    
    for col in cols_to_interpolate:
        if col not in gdf_final.columns:
            continue
            
        prev_val = gdf_final.groupby('backbone_id')[col].shift(1)
        next_val = gdf_final.groupby('backbone_id')[col].shift(-1)
        
        # Condition: Current is 0, but both immediate neighbors have data
        mask = (gdf_final[col] == 0) & (prev_val > 0) & (next_val > 0)
        gdf_final.loc[mask, col] = (prev_val + next_val) / 2
        interpolated_mask |= mask
        
    gdf_final.loc[interpolated_mask, 'is_interpolated'] = True

    # 6. SAVE RESULTS
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    gdf_final.to_parquet(output_path)
    
    print(f"✨ Process Complete!")
    print(f"   Output: {len(gdf_final)} backbone points.")
    print(f"   Saved to: {output_path}")
    print(f"   Total Time: {time.time()-start_time:.1f}s")

if __name__ == "__main__":
    main()
