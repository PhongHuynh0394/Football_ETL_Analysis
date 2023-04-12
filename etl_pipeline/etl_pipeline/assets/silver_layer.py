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
    description='Statistic of teams in games',
    group_name="Silver_layer",
    compute_kind="Pandas"
)
def silver_statsTeamOnGames(teamstats: pd.DataFrame, games: pd.DataFrame, leagues: pd.DataFrame) -> Output[pd.DataFrame]:
    ts = teamstats.copy()
    gs = games.copy()
    lgs = leagues.copy()

    #Drop unsusable columns in games
    gs.drop(columns=gs.columns.to_list()[13:], inplace=True)
    
    #create StatperLeagueSeason
    result = pd.merge(ts, gs, on="gameID")
    result = result.merge(lgs, on="leagueID", how="left")
    result.drop(columns=['season_y', 'date_y'],inplace=True)
    result = result.rename(columns={'season_x': 'season', 'date_x': 'date'})
    return Output(
        result,
        metadata={
            "table": "statsTeamOnGames",
            "records": len(result)
        }
    )
    

@asset(
    io_manager_key="minio_io_manager",
    required_resource_keys={"minio_io_manager"},
    ins={
        "appearances": AssetIn(
            key_prefix=["football", "bronze"]
        ),
        "games": AssetIn(
            key_prefix=["football", "bronze"]
        ),
        "players": AssetIn(
            key_prefix=["football", "bronze"]
        )
    },
    key_prefix=["football", "silver"],
    group_name="Silver_layer",
    description='statistic of players in games',
    compute_kind="Pandas"
)
def silver_playerAppearances(appearances: pd.DataFrame, games: pd.DataFrame, players: pd.DataFrame) -> Output[pd.DataFrame]:
    app = appearances.copy()
    ga = games.copy()
    pla = players.copy()

    #Drop unusable column
    ga.drop(columns=ga.columns.to_list()[13:], inplace=True)

    #Merge 
    player_appearances = pd.merge(app, pla, on="playerID", how="left")
    player_appearances = pd.merge(player_appearances, ga, on="gameID", how="left")

    #drop unecessary columns and rename
    player_appearances.drop(columns=['leagueID_y'],inplace=True)
    player_appearances.rename(columns={'leagueID_x': 'leagueID'}, inplace=True)
    return Output(
        player_appearances,
        metadata={
            "table": "playerAppearances",
            "records": len(player_appearances)
        }
    )

@asset(
    io_manager_key="minio_io_manager",
    required_resource_keys={"minio_io_manager"},
    ins={
        "teams": AssetIn(
            key_prefix=["football", "bronze"]
        )
    },
    key_prefix=["football", "silver"],
    group_name="Silver_layer",
    description='Teams',
    compute_kind="Pandas"
)
def silver_teams(teams: pd.DataFrame) -> Output[pd.DataFrame]:
    return Output(
        teams,
        metadata={
            "table": 'teams',
            'records': len(teams)
        }
    )