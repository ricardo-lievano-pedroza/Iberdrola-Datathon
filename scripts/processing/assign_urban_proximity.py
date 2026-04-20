import geopandas as gpd
import pandas as pd
from shapely.geometry import Point
import io
import zipfile
import urllib.request


GEONAMES_ES_URL = "https://download.geonames.org/export/dump/ES.zip"

GEONAMES_COLS = [
    "geonameid", "name", "asciiname", "alternatenames",
    "latitude", "longitude", "feature_class", "feature_code",
    "country_code", "cc2", "admin1", "admin2", "admin3", "admin4",
    "population", "elevation", "dem", "timezone", "modification_date",
]


def load_spain_municipalities(pop_threshold: int = 50_000,
                              target_crs: str = "EPSG:25830") -> gpd.GeoDataFrame:
    """
    Download GeoNames Spain populated places and return a GeoDataFrame of
    municipalities with population >= pop_threshold, projected to target_crs.
    """
    print(f" - Downloading GeoNames ES.zip and filtering pop >= {pop_threshold:,}...")
    with urllib.request.urlopen(GEONAMES_ES_URL) as resp:
        buf = io.BytesIO(resp.read())
    with zipfile.ZipFile(buf) as z:
        with z.open("ES.txt") as f:
            df = pd.read_csv(f, sep="\t", header=None, names=GEONAMES_COLS,
                             dtype={"admin1": str, "admin2": str},
                             low_memory=False)

    # feature_class 'P' = populated place; keep PPL* feature_codes
    df = df[(df["feature_class"] == "P") & (df["population"] >= pop_threshold)].copy()
    geom = [Point(xy) for xy in zip(df["longitude"], df["latitude"])]
    gdf = gpd.GeoDataFrame(df[["geonameid", "name", "population"]],
                           geometry=geom, crs="EPSG:4326")
    gdf = gdf.to_crs(target_crs)
    print(f"   Kept {len(gdf)} municipalities at/above threshold.")
    return gdf


def assign_urban_proximity(gdf_points: gpd.GeoDataFrame,
                           gdf_munis: gpd.GeoDataFrame,
                           radius_m: float = 10_000,
                           big_city_pop: int = 200_000,
                           big_city_radius_m: float = 25_000) -> gpd.GeoDataFrame:
    """
    Tag backbone points with distance to the nearest urban center and
    a boolean `near_urban` flag for hard exclusion.

    Two tiers:
      - Any city in gdf_munis within radius_m -> near_urban = True
      - Any city with population >= big_city_pop within big_city_radius_m
        -> near_big_city = True (for soft-filtering downstream)
    """
    if gdf_munis.crs != gdf_points.crs:
        gdf_munis = gdf_munis.to_crs(gdf_points.crs)

    print(f" - Assigning urban proximity (r={radius_m} m, big-city r={big_city_radius_m} m)...")

    nearest = gpd.sjoin_nearest(
        gdf_points[["point_id", "geometry"]],
        gdf_munis[["name", "population", "geometry"]].rename(
            columns={"name": "nearest_city", "population": "nearest_city_pop"}
        ),
        how="left",
        distance_col="dist_urban_m",
    ).drop_duplicates(subset=["point_id"]).drop(columns=["index_right"], errors="ignore")

    big = gdf_munis[gdf_munis["population"] >= big_city_pop]
    if len(big):
        nearest_big = gpd.sjoin_nearest(
            gdf_points[["point_id", "geometry"]],
            big[["geometry"]],
            how="left",
            distance_col="dist_big_city_m",
        ).drop_duplicates(subset=["point_id"]).drop(columns=["index_right"], errors="ignore")
        nearest = nearest.merge(
            nearest_big[["point_id", "dist_big_city_m"]], on="point_id", how="left"
        )
    else:
        nearest["dist_big_city_m"] = float("inf")

    nearest["near_urban"] = nearest["dist_urban_m"] <= radius_m
    nearest["near_big_city"] = nearest["dist_big_city_m"] <= big_city_radius_m

    out_cols = ["point_id", "nearest_city", "nearest_city_pop",
                "dist_urban_m", "dist_big_city_m", "near_urban", "near_big_city"]
    return gdf_points.merge(nearest[out_cols], on="point_id", how="left")
