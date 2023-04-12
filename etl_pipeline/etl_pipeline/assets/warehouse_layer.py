from dagster import multi_asset, Output, AssetIn, AssetOut, asset
import pandas as pd

@multi_asset(
    ins={
        "gold_statsPerLeagueSeason": AssetIn(
            key_prefix=["football", "gold"]
            #input_manager_key="minio_io_manger"
        )
    },
    outs={
        "statsperleagueseason": AssetOut(
            io_manager_key="psql_io_manager",
            key_prefix=["statsPerLeagueSeason", 'football'],
            metadata={
                "columns": [
                    "name",
                    "season",
                    "goals",
                    "xGoals",
                    "shots",
                    "shotsOnTarget",
                    "fouls",
                    "yellowCards",
                    "redCards",
                    "corners",
                    "games",
                    "goalPerGame"
                ]
            }
        ),
    },
    compute_kind="PostgreSQL",
    group_name="Warehouse_layer"
)
def statsPerLeagueSeason(gold_statsPerLeagueSeason: pd.DataFrame):# -> Output[pd.DataFrame]:
    return Output(
        gold_statsPerLeagueSeason,
        metadata={
            "schema": "football",
            "table": "statsPerLeagueSeason",
            "records": len(gold_statsPerLeagueSeason)
        }
    )


@multi_asset(
    ins={
        "gold_statsPerPlayerSeason": AssetIn(
            key_prefix=['football', 'gold']
        )
    },
    outs={
        "statsperplayerseason": AssetOut(
            io_manager_key="psql_io_manager",
            key_prefix=["statsPerPlayerSeason", 'football'],
            metadata={
                "columns": [
                    "playerID",
                    "name",
                    "season",
                    "goals",
                    "shots",
                    "xGoals",
                    "xGoalsChain",
                    "xGoalsBuildup",
                    "assists",
                    "keyPasses",
                    "xAssists",
                    "gDiff",
                    "gDiffRatio"
                ]
            }
        )
    },
    compute_kind="PostgreSQL",
    group_name="Warehouse_layer"
)
def statsPerPlayerSeason(gold_statsPerPlayerSeason: pd.DataFrame):# -> Output[pd.DataFrame]:
    return Output(
        gold_statsPerPlayerSeason,
        metadata={
            "schema": "football",
            "table": "statsPerPlayerSeason",
            "records": len(gold_statsPerPlayerSeason)
        }
    )
# , Output(
#         gold_statsPerPLayerSeason,
#         output_name='statsperplayerseason'
#     )
