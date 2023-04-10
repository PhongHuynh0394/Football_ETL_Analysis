from dagster import asset, Output, AssetIn
import pandas as pd

@asset(
    io_manager_key="minio_io_manager",
    ins={
        "silver_statsTeamOnGames": AssetIn(
            key_prefix=["football", "silver"]
        )
    },
    group_name="Gold_layer",
    key_prefix=["football", "gold"],
    description='Statistic of all league in each season',
    compute_kind="Minio"
)
def gold_statPerLeagueSeason(silver_statsTeamOnGames: pd.DataFrame) -> Output[pd.DataFrame]: 
    st = silver_statsTeamOnGame.copy()

    result = (
        st.groupby(['name', 'season'])
        .agg({"goals": "sum", "xGoals": "sum", "shots": "sum", "shotsOnTarget": "sum", "fouls": "sum", "yellowCards": "sum", "redCards": "sum",'corners': 'sum', "gameID": 'count'})
        .reset_index()
    )

    result = result.rename(columns={'gameID':"games"})
    return Output(
        result,
        metadata={
            'table': 'statPerLeagueSeason',
            'records': len(result)
        }
    )
