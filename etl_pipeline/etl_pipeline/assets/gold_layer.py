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
    compute_kind="Pandas"
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
    compute_kind="Pandas"
)
def gold_statsPerPlayerSeason(silver_playerAppearances: pd.DataFrame) -> Output[pd.DataFrame]: 
    st = silver_playerAppearances.copy()

    statsPerPlayerSeason = (
       st.groupby(['playerID','name','season'])
        .agg({'goals': 'sum','shots': 'sum','xGoals':'sum','xGoalsChain':'sum','xGoalsBuildup':'sum','assists':'sum','keyPasses':'sum','xAssists':'sum','time': 'sum'})
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
    

@asset(
    io_manager_key="minio_io_manager",
    ins={
        "gold_statsPerPlayerSeason": AssetIn(
            key_prefix=["football", "gold"]
        )
    },
    group_name="Gold_layer",
    key_prefix=["football", "gold"],
    description='Statistic of all player in 90 min',
    compute_kind="Pandas"
)
def gold_statsPlayerPer90(gold_statsPerPlayerSeason: pd.DataFrame) -> Output[pd.DataFrame]: 
    statsPerPLayerSeason = gold_statsPerPlayerSeason.copy()
    
    statsPlayerPer90 = statsPerPLayerSeason[statsPerPLayerSeason['season'].isin(['2018','2019','2020'])]
    statsPlayerPer90 = (
        statsPlayerPer90.groupby(['playerID', 'name'])
        .agg(total_goals=('goals','sum'),total_assists=('assists','sum'),total_time=('time','sum'))
        .reset_index()
    )
    statsPlayerPer90['goalsPer90'] = statsPlayerPer90['total_goals'] / statsPlayerPer90['total_time'] * 90
    statsPlayerPer90['assistsPer90'] = statsPlayerPer90['total_assists'] / statsPlayerPer90['total_time'] * 90
    statsPlayerPer90['scorers'] = statsPlayerPer90['total_goals'] + statsPlayerPer90['total_assists']
    statsPlayerPer90 = statsPlayerPer90[statsPlayerPer90['scorers'] > 30]
    return Output(
        statsPlayerPer90,
        metadata={
            'table': 'statsPerPlayerSeason',
            'records': len(statsPlayerPer90)
        }
    )