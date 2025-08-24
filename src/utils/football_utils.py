from itertools import product
from typing import Dict
from pyspark.sql.functions import *
from pyspark.sql import Column
from pyspark.sql.types import StringType, BooleanType
from src.schemas.fields import FactMatchStatisticFields


class FootballUtils:
    def __init__(self, leagues: list[int], seasons: list[int]):
        self.leagues = leagues
        self.seasons = seasons
        pass

    def combineLeagueAndSeason(self):
        return list(product(self.leagues, self.seasons))
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        # Cleanup code if needed (optional)
        pass

class FootballStadiumCategory:
    SMALL = "small"
    MEDIUM = "medium"
    LARGE = "large"

class DataFrameFootballUtils:
    @staticmethod
    def get_stadium_category(capacity_col: Column) -> Column:
        """
        Categorizes football stadiums based on their capacity.
        :param capacity_col: Column representing the stadium capacity.
        :return: Column with the stadium category.
        """
        return (when(capacity_col > 100000, FootballStadiumCategory.LARGE)
                .when(capacity_col > 40000 , FootballStadiumCategory.MEDIUM)
                .otherwise(FootballStadiumCategory.SMALL))
    
    @staticmethod
    def transfrom_score_str(score_home: Column, score_away: Column) -> StringType:
        """
        Transforms score columns into a structured string format.
        :param score_home: Column representing the home team's score.
        :param score_away: Column representing the away team's score.
        :return: Column with the formatted score string.
        """
        return when(score_home.isNull() | score_away.isNull(), None) \
            .otherwise(concat(score_home.cast(StringType()),
                              lit("-"), 
                              score_away.cast(StringType())))
    
    @staticmethod
    def get_match_result(score_home: Column, score_away: Column) -> StringType:
        """
        Determines the match result based on home and away scores.
        :param score_home: Column representing the home team's score.
        :param score_away: Column representing the away team's score.
        :return: Column with the match result as a string.
        """
        return when(score_home.isNull() | score_away.isNull(), None) \
            .otherwise(when(score_home > score_away, "home_win")
                       .when(score_home < score_away, "away_win")
                       .otherwise("draw"))
    
    @staticmethod
    def get_goals_summary(fixture_id: Column, team_id: Column, fixture_df: DataFrame) -> Dict:
        # Filter matches where the team played (either home or away)
        team_matches = fixture_df.filter(
            col('fixture_id') == fixture_id
        )
        
        # Calculate goals scored and conceded
        goals_data = team_matches.select(
            # Goals scored: home_goals when playing at home, away_goals when playing away
            when(col('home_team_id') == team_id, col('goals_home'))
            .otherwise(col('goals_away')).alias('goals_scored'),
            
            # Goals conceded: away_goals when playing at home, home_goals when playing away  
            when(col('home_team_id') == team_id, col('goals_away'))
            .otherwise(col('goals_home')).alias('goals_conceded'),

            # Goals scored in the first half: home_score_halftime when playing at home, away_score_halftime when playing away
            when(col('home_team_id') == team_id, col('score_halftime_home'))
            .otherwise(col('score_halftime_away')).alias('goals_scored_first_half'),

            # Goals conceded in the first half: away_score_halftime when playing at home, home_score_halftime when playing away
            when(col('home_team_id') == team_id, col('score_halftime_away'))
            .otherwise(col('score_halftime_home')).alias('goals_conceded_first_half'),

            # Goals scored in the second half: home_score_fulltime when playing at home, away_score_fulltime when playing away
            when(col('home_team_id') == team_id, col('score_fulltime_home') - col('score_halftime_home'))
            .otherwise(col('score_fulltime_away') - col('score_halftime_away')).alias('goals_scored_second_half'),

            # Goals conceded in the second half: away_score_fulltime when playing at home, home_score_fulltime when playing away
            when(col('home_team_id') == team_id, col('score_fulltime_away') - col('score_halftime_away'))
            .otherwise(col('score_fulltime_home') - col('score_halftime_home')).alias('goals_conceded_second_half'),

            # Goals scored in extra time: home_score_extratime when playing at home, away_score_extratime when playing away
            when(col('home_team_id') == team_id, col('score_extratime_home'))
            .otherwise(col('score_extratime_away')).alias('goals_scored_extra_time'),
            
            # Goals conceded in extra time: away_score_extratime when playing at home, home_score_extratime when playing away
            when(col('home_team_id') == team_id, col('score_extratime_away'))
            .otherwise(col('score_extratime_home')).alias('goals_conceded_extra_time'),
            
            # Goals scored in penalties: home_score_penalty when playing at home, away_score_penalty when playing away
            when(col('home_team_id') == team_id, col('score_penalty_home'))
            .otherwise(col('score_penalty_away')).alias('goals_scored_penalty'),
            
            # Goals conceded in penalties: away_score_penalty when playing at home, home_score_penalty when playing away
            when(col('home_team_id') == team_id, col('score_penalty_away'))
            .otherwise(col('score_penalty_home')).alias('goals_conceded_penalty'),

            when(col('home_team_id') == team_id, lit(True)).otherwise(lit(False)).alias('is_home')
        ).collect()[0]
        
        return {
            'total_goals_scored': goals_data['goals_scored'] or 0,
            'total_goals_conceded': goals_data['goals_conceded'] or 0,
            'total_goals_scored_first_half': goals_data['goals_scored_first_half'] or 0,
            'total_goals_conceded_first_half': goals_data['goals_conceded_first_half'] or 0,
            'total_goals_scored_second_half': goals_data['goals_scored_second_half'] or 0,
            'total_goals_conceded_second_half': goals_data['goals_conceded_second_half'] or 0,
            'total_goals_scored_extra_time': goals_data['goals_scored_extra_time'] or 0,
            'total_goals_conceded_extra_time': goals_data['goals_conceded_extra_time'] or 0,
            'total_goals_scored_penalty': goals_data['goals_scored_penalty'] or 0,
            'total_goals_conceded_penalty': goals_data['goals_conceded_penalty'] or 0  
        }
    

    @staticmethod
    def add_goals_columns(fact_df: DataFrame, fixture_df: DataFrame) -> DataFrame:
        # Prepare fixture data for home teams
        home_team_fixture = fixture_df.select(
            col('fixture_id'),
            col('home_team_id').alias('team_id'),
            col('goals_home').alias('goals_scored'),
            col('goals_away').alias('goals_conceded'),
            col('score_halftime_home').alias('goals_scored_first_half'),
            col('score_halftime_away').alias('goals_conceded_first_half'),
            (col('score_fulltime_home') - col('score_halftime_home')).alias('goals_scored_second_half'),
            (col('score_fulltime_away') - col('score_halftime_away')).alias('goals_conceded_second_half'),
            col('score_extratime_home').alias('goals_scored_extra_time'),
            col('score_extratime_away').alias('goals_conceded_extra_time'),
            col('score_penalty_home').alias('goals_scored_penalty'),
            col('score_penalty_away').alias('goals_conceded_penalty')
        )
        
        # Prepare fixture data for away teams
        away_team_fixture = fixture_df.select(
            col('fixture_id'),
            col('away_team_id').alias('team_id'),
            col('goals_away').alias('goals_scored'),
            col('goals_home').alias('goals_conceded'),
            col('score_halftime_away').alias('goals_scored_first_half'),
            col('score_halftime_home').alias('goals_conceded_first_half'),
            (col('score_fulltime_away') - col('score_halftime_away')).alias('goals_scored_second_half'),
            (col('score_fulltime_home') - col('score_halftime_home')).alias('goals_conceded_second_half'),
            col('score_extratime_away').alias('goals_scored_extra_time'),
            col('score_extratime_home').alias('goals_conceded_extra_time'),
            col('score_penalty_away').alias('goals_scored_penalty'),
            col('score_penalty_home').alias('goals_conceded_penalty')
        )
        
        # Union both datasets
        team_fixture_goals = home_team_fixture.union(away_team_fixture)
        
        # Join with fact table
        result_df = fact_df.join(
            team_fixture_goals,
            (fact_df[FactMatchStatisticFields.FIXTURE_ID] == team_fixture_goals['fixture_id']) &
            (fact_df[FactMatchStatisticFields.TEAM_ID] == team_fixture_goals['team_id']),
            'left'
        )
        
        # Select final columns
        final_df = result_df.select(
            fact_df['*'],
            team_fixture_goals['goals_scored'].alias(FactMatchStatisticFields.GOALS_SCORED),
            team_fixture_goals['goals_conceded'].alias(FactMatchStatisticFields.GOALS_CONCEDED),
            team_fixture_goals['goals_scored_first_half'].alias(FactMatchStatisticFields.GOALS_SCORED_FIRST_HALF),
            team_fixture_goals['goals_conceded_first_half'].alias(FactMatchStatisticFields.GOALS_CONCEDED_FIRST_HALF),
            team_fixture_goals['goals_scored_second_half'].alias(FactMatchStatisticFields.GOALS_SCORED_SECOND_HALF),
            team_fixture_goals['goals_conceded_second_half'].alias(FactMatchStatisticFields.GOALS_CONCEDED_SECOND_HALF),
            team_fixture_goals['goals_scored_extra_time'].alias(FactMatchStatisticFields.GOALS_SCORED_EXTRATIME),
            team_fixture_goals['goals_conceded_extra_time'].alias(FactMatchStatisticFields.GOALS_CONCEDED_EXTRATIME),
            team_fixture_goals['goals_scored_penalty'].alias(FactMatchStatisticFields.GOALS_SCORED_PENALTY),
            team_fixture_goals['goals_conceded_penalty'].alias(FactMatchStatisticFields.GOALS_CONCEDED_PENALTY),
            when(team_fixture_goals['goals_scored'] > team_fixture_goals['goals_conceded'], 'win').otherwise(
                when(team_fixture_goals['goals_scored'] < team_fixture_goals['goals_conceded'], 'loss').otherwise('draw')
            ).alias(FactMatchStatisticFields.MATCH_RESULT),
        )
        return final_df