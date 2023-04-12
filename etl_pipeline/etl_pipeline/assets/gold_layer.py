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
def gold_statsPerLeagueSeason(silver_statsTeamOnGames: pd.DataFrame) -> Output[pd.DataFrame]: 
    st = silver_statsTeamOnGames.copy()

    result = (
        st.groupby(['name', 'season'])
        .agg({"goals": "sum", "xGoals": "sum", "shots": "sum", "shotsOnTarget": "sum", "fouls": "sum", "yellowCards": "sum", "redCards": "sum",'corners': 'sum', "gameID": 'count'})
        .reset_index()
    )

    result = result.rename(columns={'gameID':"games"})
    result['goalPerGame']= result.goals/result.games
    result['season'] = result['season'].astype('string')
    return Output(
        result,
        metadata={
            'table': 'statPerLeagueSeason',
            'records': len(result)
        }
    )


@asset(
    io_manager_key="minio_io_manager",
    ins={
        "silver_playerAppearances": AssetIn(
            key_prefix=["football", "silver"]
        )
    },
    group_name="Gold_layer",
    key_prefix=["football", "gold"],
    description='Statistic of all player in each season',
    compute_kind="Minio"
)
def gold_statsPerPlayerSeason(silver_playerAppearances: pd.DataFrame) -> Output[pd.DataFrame]: 
    st = silver_playerAppearances.copy()

    statsPerPlayerSeason = (
       st.groupby(['playerID','name','season'])
        .agg({'goals': 'sum','shots': 'sum','xGoals':'sum','xGoalsChain':'sum','xGoalsBuildup':'sum','assists':'sum','keyPasses':'sum','xAssists':'sum'})
        .reset_index()
    )
    statsPerPlayerSeason['gDiff'] = statsPerPlayerSeason['goals'] - statsPerPlayerSeason['xGoals']
    statsPerPlayerSeason['gDiffRatio'] = statsPerPlayerSeason['goals'] / statsPerPlayerSeason['xGoals']
    statsPerPlayerSeason['gDiffRatio'] = statsPerPlayerSeason['gDiffRatio'].fillna(0)

    return Output(
        statsPerPlayerSeason,
        metadata={
            'table': 'statsPerPlayerSeason',
            'records': len(statsPerPlayerSeason)
        }
    )
    