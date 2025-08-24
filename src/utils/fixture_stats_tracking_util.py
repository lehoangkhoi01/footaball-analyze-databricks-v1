import json
import os
from datetime import datetime
from delta import DeltaTable
from typing import Dict, List, Set
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, coalesce
from pyspark.sql.functions import round as spark_round
#from utils.data_configure.unified_data_utils import UnifiedDataUtils
from pyspark.sql import SparkSession, DataFrame
from src.schemas.fields import TableNames, OtherFields
from src.schemas.stats_tracking_schema import StatsTrackingSchema
from pyspark.sql.types import BooleanType, LongType, TimestampType, StructField, StructType


class LeagueProgressTrackingRecord:
    def __init__(
        self,
        league_id: int,
        total_fixtures: int,
        fetched_count: int,
        fetched_percentage: float,
        is_complete: bool,
        last_updated: datetime,
    ):
        self.league_id = league_id
        self.total_fixtures = total_fixtures
        self.fetched_count = fetched_count
        self.fetched_percentage = fetched_percentage
        self.is_complete = is_complete
        self.last_updated = last_updated

    def to_str(self) -> str:
        """
        Convert the LeagueProgressTrackingRecord to a readable string format.
        """
        return (
            f"LeagueProgressTrackingRecord(\n"
            f"  league_id: {self.league_id},\n"
            f"  total_fixtures: {self.total_fixtures},\n"
            f"  fetched_count: {self.fetched_count},\n"
            f"  fetched_percentage: {self.fetched_percentage},\n"
            f"  is_complete: {self.is_complete},\n"
            f"  last_updated: {self.last_updated}\n"
            f")"
        )


class FixtureStatsHandler:
    def __init__(
        self,
        fixture_stats_tracking_df: DataFrame, 
        league_progress_tracking_df: DataFrame,
        data_dir: str = "../../data", 
        league_file: str = "leagues.json",
        spark: SparkSession = None):
        """
        Initialize the Fixture Statistics Tracker
        
        Args:
            data_dir: Directory where data files are stored
            league_file: Name of the league file containing league information
            spark: Optional SparkSession instance for Spark operations
        """
        self.data_dir = data_dir
    
        self.spark = spark or SparkSession.builder.getOrCreate()

        if (league_progress_tracking_df.count() == 0):
            raise Exception("Empty league progress tracking dataframe")
        if (fixture_stats_tracking_df.count() == 0):
            raise Exception("Empty fixture stats tracking dataframe")
        self.fixture_stats_tracking_df = fixture_stats_tracking_df
        self.league_progress_tracking_df = league_progress_tracking_df
        
        # File paths
        self.fixture_tracking_file = os.path.join(data_dir, "fixture_stats_tracking.json")
        self.league_progress_file = os.path.join(data_dir, "league_progress_tracking.json")
        self.league_path = os.path.join(data_dir, league_file)
        self.fixture_stats_file_prefix = "fixtures_stats"
        
    def get_fixture_stats(self) -> DataFrame:
        """Return the fixture statistics DataFrame"""
        return self.fixture_stats_tracking_df
    
    def get_league_progress_tracking(self) -> DataFrame:
        """Return the league progress tracking DataFrame"""
        return self.league_progress_tracking_df


    #TODO: Ignore now
    def sync_league_progess_and_stats_tracking(self, league_id: str):
        fixture_stat_league_tracking = []
        for fixture_id, fixture_stat in self.fixture_tracking["fetched_fixtures"].items():
            if str(fixture_stat["league_id"]) == league_id:
                fixture_stat_league_tracking.append(int(fixture_id))
        print('Fixture stats tracking count: ', len(fixture_stat_league_tracking), type(fixture_stat_league_tracking[0]))
        fixture_stat_league = self.load_fixture_stats(league_id)
        fixture_ids = [fixture['fixture_id'] for fixture in fixture_stat_league]
        print('Fixture stats count: ', len(fixture_ids), type(fixture_ids[0]))
        
        should_remove_fixture = list(set(fixture_ids) - set(fixture_stat_league_tracking))
        if (len(should_remove_fixture) > 0):
            print('Removing fixtures to sync')
            self.remove_fixtures_fetched(league_id, should_remove_fixture)
        else:
            print('No fixtures to remove for syncing')

    
    # TODO: Ignore
    def load_fixture_stats(self, league_id: str) -> List[Dict]:
        """Load fixture statistics for a specific league"""
        fixtures_file = os.path.join(self.data_dir, f"{self.fixture_stats_file_prefix}_{league_id}.json")
        if os.path.exists(fixtures_file):
            with open(fixtures_file, 'r') as f:
                return json.load(f)
        return []
    
    
    def save_fixture_tracking(self, batch_update_df: DataFrame):
        """Save fixture statistics tracking data"""
        if (batch_update_df.count()):
            target_df = self.fixture_stats_tracking_df
        
            # Join to identify existing records based on fixture_id
            joined_df = target_df.alias("target").join(
                batch_update_df.alias("updates"),
                target_df.fixture_id == batch_update_df.fixture_id,
                "full_outer"
            )
            # Create the merged result with coalesce to handle updates
            merged_df = joined_df.select(
                coalesce(joined_df["updates.fixture_id"], joined_df["target.fixture_id"]).alias("fixture_id"),
                coalesce(joined_df["updates.league_id"], joined_df["target.league_id"]).alias("league_id"),
                coalesce(joined_df["updates.is_fetched_stat"], joined_df["target.is_fetched_stat"]).alias("is_fetched_stat"),
                coalesce(joined_df["updates.fetch_stat_time"], joined_df["target.fetch_stat_time"]).alias("fetch_stat_time")
            )
            # Replace the target DataFrame
            self.fixture_stats_tracking_df = merged_df
        
    def save_league_progress(self, batch_update_df: DataFrame):
        """Save league progress tracking data"""     
        if batch_update_df is not None:
            target_df = self.league_progress_tracking_df
            # Use DataFrame API instead of DeltaTable API
            batch_update_df.createOrReplaceTempView("updates")
            
            # Join to identify existing records
            joined_df = target_df.alias("target").join(
                batch_update_df.alias("updates"),
                target_df.league_id == batch_update_df.league_id,
                "full_outer"
            )
            # Create the merged result
            merged_df = joined_df.select(
                coalesce(joined_df["updates.league_id"], joined_df["target.league_id"]).alias("league_id"),
                coalesce(joined_df["updates.total_fixtures"], joined_df["target.total_fixtures"]).alias("total_fixtures"),
                coalesce(joined_df["updates.fetched_count"], joined_df["target.fetched_count"]).alias("fetched_count"),
                coalesce(joined_df["updates.fetched_percentage"], joined_df["target.fetched_percentage"]).alias("fetched_percentage"),
                coalesce(joined_df["updates.is_complete"], joined_df["target.is_complete"]).alias("is_complete"),
                coalesce(joined_df["updates.last_updated"], joined_df["target.last_updated"]).alias("last_updated")
            )
            # Replace the target DataFrame
            self.league_progress_tracking_df = merged_df
            
    
    def get_all_league_ids(self) -> List[str]:
        league_ids = self.league_progress_tracking_df.select("league_id").toPandas()["league_id"].tolist()
        return league_ids
    
    def mark_fixtures_fetched(self, fixture_ids: List[str], league_id: str, status: str = "success"):
        """
        Mark a fixture as having its statistics fetched
        
        Args:
            fixture_ids: List of fixture's ID
            league_id: ID of the league the fixture belongs to
            status: Status of the fetch ('success', 'failed', 'partial')
        """
        league_id = str(league_id)
        batch_update_fixtures = []

        schema = StatsTrackingSchema.get_fixture_stat_tracking_schema()

        for fid in fixture_ids:
            batch_update_fixtures.append(
                {
                    "fixture_id": int(fid),
                    "league_id": int(league_id),
                    "fetch_stat_time": datetime.now(),
                    "is_fetched_stat": True
                }
            )
        batch_update_df = self.spark.createDataFrame(batch_update_fixtures)
        self.save_fixture_tracking(batch_update_df)
        self.update_league_progress(league_id)

   
    def remove_fixtures_fetched(self, league_id: str, fixture_ids: List[str]):
        """
        Remove fixtures from fetched tracking
        
        Args:
            fixture_ids: List of fixture's ID to remove
        """
        all_fixtures = self.load_fixture_stats(league_id)
        print('Fixture stats count before removed: ', len(all_fixtures))
        for fid in fixture_ids:
            fid = str(fid)
            if fid in self.fixture_tracking["fetched_fixtures"]:
                league_id = self.fixture_tracking["fetched_fixtures"][fid]["league_id"]
                del self.fixture_tracking["fetched_fixtures"][fid]
                self.update_league_progress(league_id)
            removed_fixture = next((f for f in all_fixtures if str(f['fixture_id']) == fid), None)
            print('Removing fixture: ', removed_fixture)
            if removed_fixture:
                all_fixtures.remove(removed_fixture)

        self.save_fixture_stats(league_id, all_fixtures)        
        self.save_fixture_tracking()
    
    def get_fixture_status(self, fixture_id: str) -> Dict:
        """Get the fetch status of a specific fixture"""
        return self.fixture_tracking["fetched_fixtures"].get(str(fixture_id), None)
    
    def get_unfetched_fixtures_for_league(self, league_id: str) -> List[Dict]:
        """Get list of fixtures in a league that haven't had their statistics fetched"""
        league_id = int(league_id)
        fixtures = self.get_league_fixtures(league_id)
        unfetched = []
        unfetched_fixtures = (self.fixture_stats_tracking_df
            .filter((col("league_id") == league_id) & (col("is_fetched_stat") == False))
            .toPandas().to_dict(orient="records"))
        return unfetched_fixtures
    
    def get_fetched_fixtures_for_league(self, league_id: str) -> List[str]:
        """Get list of fixture IDs that have been fetched for a specific league"""
        league_id = int(league_id)
        return (self.fixture_stats_tracking_df
            .filter((col("league_id") == league_id) & (col("is_fetched_stat") == True))
            .toPandas()["fixture_id"].tolist())
    
    def update_league_progress(self, league_id: str):
        league_id = int(league_id)
        """Update progress tracking for a league"""
        total_fixtures = len(self.get_league_fixtures(league_id))
        fetched_count = len(self.get_fetched_fixtures_for_league(league_id))
        fetched_percentage = round((fetched_count / total_fixtures) * 100, 2) if total_fixtures > 0 else 0
        
        update_batch_df = self.spark.createDataFrame([{
            "league_id": league_id,
            "total_fixtures": total_fixtures,
            "fetched_count": fetched_count,
            "fetched_percentage": fetched_percentage,
            "is_complete": fetched_count >= total_fixtures,
            "last_updated": datetime.now().isoformat()
        }])
        self.save_league_progress(update_batch_df)
    
    def get_invalid_fixture_stats(self, league_id: str) -> List[str]:
        """
        Get list of fixture IDs that have invalid statistics (e.g., missing data)
        Returns:
            List of fixture IDs with invalid statistics
        """
        invalid_fixtures = []
        all_current_fixtures = self.load_fixture_stats(league_id)
        for item in all_current_fixtures:
            if len(item['stats']) == 0:
                invalid_fixtures.append(item['fixture_id'])
        return invalid_fixtures
    
    def add_league_progress(self, league_id: str):
        self.league_progress[league_id] = {
            "total_fixtures": 0,
            "fetched_count": 0,
            "completion_percentage": 0.0,
            "is_complete": False,
            "last_updated": datetime.now().isoformat()
        }
        self.save_league_progress()
        return self.league_progress[league_id]
                  
    def get_league_progress(self, league_id: str) -> Dict:
        """
        Get progress information for a specific league
        Return {league_id, total_fixtures, fetched_count, fetched_percentage, is_complete, last_updated}
        """
        league_id = int(league_id)
        league = self.league_progress_tracking_df.filter(f"league_id = '{league_id}'").toPandas().to_dict(orient="records")[0] if self.league_progress_tracking_df.filter(f"league_id = '{league_id}'").count() > 0 else {}
        return league
    
    def get_league_fixtures(self, league_id: str) -> List[Dict]:
        """Load fixtures for a specific league"""
        """Return: [{fixture_id, league_id, is_fetched_stat, fetch_stat_time}]"""
        league_id = int(league_id)
        fixtures = self.fixture_stats_tracking_df.filter(f"league_id = '{league_id}'").toPandas().to_dict(orient="records")
        return fixtures
    
    def is_league_complete(self, league_id: str) -> bool:
        """Check if all fixtures in a league have been fetched"""
        league_id = int(league_id)
        league_progress = self.get_league_progress(league_id)
        return league_progress[OtherFields.IS_COMPLETE]
    
    def get_incomplete_leagues(self) -> List[str]:
        """Get list of league IDs that are not fully fetched"""
        incomplete = []
        for league_id in self.get_all_league_ids():
            if not self.is_league_complete(league_id):
                incomplete.append(league_id)
        return incomplete
    
    def get_next_fixtures_to_fetch(self, limit: int = 10) -> List[Dict]:
        """
        Get the next batch of fixtures to fetch statistics for
        
        Args:
            limit: Maximum number of fixtures to return
            
        Returns:
            List of fixture dictionaries with added 'league_id' field
        """
        fixtures_to_fetch = []
        
        # Prioritize incomplete leagues
        incomplete_leagues = self.get_incomplete_leagues()
        all_leagues = self.get_all_league_ids()
        
        # Process incomplete leagues first, then complete ones
        for league_id in incomplete_leagues + [lid for lid in all_leagues if lid not in incomplete_leagues]:
            if len(fixtures_to_fetch) >= limit:
                break
            
            unfetched = self.get_unfetched_fixtures_for_league(league_id)
            
            for fixture in unfetched:
                if len(fixtures_to_fetch) >= limit:
                    break
                
                # Add league_id to fixture data for easy reference
                # fixture_with_league = fixture.copy()
                # fixture_with_league['league_id'] = league_id
                fixtures_to_fetch.append(fixture)
        
        return fixtures_to_fetch
    
    def get_progress_summary(self) -> Dict:
        """Get a comprehensive progress summary"""
        all_leagues = self.get_all_league_ids()
        total_fixtures = 0
        total_fetched = 0
        
        league_details = {}
        
        for league_id in all_leagues:
            progress = self.get_league_progress(league_id)
            league_details[league_id] = progress
            total_fixtures += progress["total_fixtures"]
            total_fetched += progress["fetched_count"]
        
        return {
            "total_leagues": len(all_leagues),
            "total_fixtures": total_fixtures,
            "total_fetched": total_fetched,
            "overall_completion": round((total_fetched / total_fixtures) * 100, 2) if total_fixtures > 0 else 0,
            "incomplete_leagues": len(self.get_incomplete_leagues()),
            "league_details": league_details
        }
    
    # Ignore now
    def reset_fixture_tracking(self, fixture_id: str):
        """Remove a fixture from tracking (in case you need to re-fetch)"""
        fixture_id = str(fixture_id)
        if fixture_id in self.fixture_tracking["fetched_fixtures"]:
            league_id = self.fixture_tracking["fetched_fixtures"][fixture_id]["league_id"]
            del self.fixture_tracking["fetched_fixtures"][fixture_id]
            self.save_fixture_tracking()
            self.update_league_progress(league_id)
            return True
        return False
    
    # ignore now
    def reset_league_tracking(self, league_id: str):
        """Remove all fixtures for a league from tracking"""
        fixtures_to_remove = []
        league_id = str(league_id)
        for fixture_id, data in self.fixture_tracking["fetched_fixtures"].items():
            if data["league_id"] == league_id:
                fixtures_to_remove.append(fixture_id)
        
        for fixture_id in fixtures_to_remove:
            del self.fixture_tracking["fetched_fixtures"][fixture_id]
        
        if fixtures_to_remove:
            self.save_fixture_tracking()
            self.update_league_progress(league_id)
        
        return len(fixtures_to_remove)
