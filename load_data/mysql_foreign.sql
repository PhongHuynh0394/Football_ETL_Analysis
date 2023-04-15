ALTER TABLE appearances ADD FOREIGN KEY (gameID) REFERENCES games(gameID);
ALTER TABLE appearances ADD FOREIGN KEY (playerID) REFERENCES players(playerID);
ALTER TABLE appearances ADD FOREIGN KEY (leagueID) REFERENCES leagues(leagueID);

ALTER TABLE games ADD FOREIGN KEY (leagueID) REFERENCES leagues(leagueID);
ALTER TABLE games ADD FOREIGN KEY (homeTeamID) REFERENCES teams(teamID);
ALTER TABLE games ADD FOREIGN KEY (awayTeamID) REFERENCES teams(teamID);

ALTER TABLE teamstats ADD FOREIGN KEY (gameID) REFERENCES games(gameID);
ALTER TABLE teamstats ADD FOREIGN KEY (teamID) REFERENCES teams(teamID);
