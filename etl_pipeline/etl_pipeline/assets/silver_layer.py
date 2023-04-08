from dagster import asset, Output, AssetIn
import pandas as pd

@asset(
    io_manager_key="minio_io_manager",
    required_resource_keys={"minio_io_manager"},
    ins={
        "teamstats": AssetIn(
            key_prefix=["football", "bronze"]
        ),
        "games": AssetIn(
            key_prefix=["football", "bronze"]
        ),
        "leagues": AssetIn(
            key_prefix=["football", "bronze"]
        )
    },
    key_prefix=["football", "silver"],
    group_name="Silver_layer",
    compute_kind="Minio"
)
def silver_statsPerLeagueSeason(teamstats: pd.DataFrame, games: pd.DataFrame, leagues: pd.DataFrame) -> Output[pd.DataFrame]:
    ts = teamstats.copy()
    gs = games.copy()
    lgs = leagues.copy()

    #Drop unsusable columns in games
    gs.drop(columns=gs.columns.to_list()[13:], inplace=True)
    
    #Megre
    result = pd.merge(ts, gs, on="gameID")
    result = result.merge(lgs, on="leagueID", how="left")
    result.drop(columns=['season_y', 'date_y'],inplace=True)
    result = result.rename(columns={'season_x': 'season', 'date_x': 'date'})
    return Output(
        result,
        metadata={
            "table": "statsPerLeagueSeason",
            "records": len(result)
        }
    )
    