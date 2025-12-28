import time
import os
import datetime
import logging
from collections import Counter
from typing import Optional, Generator, Any

from dotenv import load_dotenv
import dropbox
from dropbox.exceptions import ApiError, RateLimitError
from dropbox.team_common import TimeRange

# Configure global logging settings
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Load environment variables from a .env file
load_dotenv(verbose=True)


class DropboxConfigError(Exception):
    """
    Custom exception raised when the Dropbox configuration is missing or invalid.
    """
    pass


class DropboxManager:
    """
    Manages interactions with the Dropbox Team API, specifically regarding event logging and statistical analysis.
    """

    def __init__(self, access_token: Optional[str] = None) -> None:
        """
        Initializes the Dropbox Team Client.
        Prioritizes the passed access_token; falls back to the 'ACCESS_TOKEN' environment variable if not provided.
        Args:
            access_token: The Dropbox API access token.
        Raises:
            DropboxConfigError: If the access token is missing or client initialization fails.
        """
        self.access_token: str = access_token or os.getenv('ACCESS_TOKEN')

        if not self.access_token:
            raise DropboxConfigError(
                "Missing ACCESS_TOKEN. Please set it in your .env file or environment."
            )

        try:
            self.client = dropbox.DropboxTeam(self.access_token)
        except Exception as e:
            logger.critical(f"Client initialization failed: {e}")
            raise DropboxConfigError("Failed to initialize DropboxTeam client.") from e

    def _fetch_team_events(self, lookback_minutes: int = 10) -> Generator[Any, None, None]:
        """
        Retrieves team events occurring within a specified time window.
        This method handles API pagination and respects rate limits by implementing a backoff strategy when necessary.
        Args:
            lookback_minutes: The number of minutes into the past to query for events.
        Yields:
            Event objects returned by the Dropbox Team Log API.
        Raises:
            ApiError: If an unrecoverable error occurs during the API call.
        """
        # Calculate the UTC start time for the query window
        start_time = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(minutes=lookback_minutes)

        cursor = None
        has_more = True

        while has_more:
            try:
                # If no cursor exists, start a new query; otherwise, continue the previous session
                if cursor is None:
                    result = self.client.team_log_get_events(time=TimeRange(start_time=start_time))
                else:
                    result = self.client.team_log_get_events_continue(cursor=cursor)

                yield from result.events

                # Update pagination control variables
                cursor = result.cursor
                has_more = result.has_more

            except RateLimitError as e:
                # Dropbox SDK provides a 'backoff' attribute suggesting wait time
                wait_time = e.backoff or 5
                logger.warning(f"Rate limit exceeded. Pausing for {wait_time} seconds.")
                time.sleep(wait_time)
                continue

            except ApiError as e:
                logger.error(f"Dropbox API error encountered: {e}")
                break

    def process_team_events(self, lookback_minutes: int = 1000) -> None:
        """
        Gathers and logs metadata for team events in a single pass.
        This method consumes the event generator, logs specific event details,
        and calculates the frequency of event types.
        Args:
            lookback_minutes: The lookback window in minutes for event retrieval.
        """
        count = 0
        counter = Counter()

        try:
            # Iterate through the generator to process events
            for event in self._fetch_team_events(lookback_minutes):
                count += 1

                # Extract and log relevant metadata to create an event log
                event_data = {
                    "timestamp": str(event.timestamp),
                    "event_type": str(event.event_type),
                    "actor": str(event.actor),
                    "origin": str(event.origin),
                    "assets": str(event.assets)
                }

                logger.info(f"Event Processed: {event_data}")

                # Extract the event tag name for statistical analysis
                if "'" in str(event.event_type):
                    event_tag = str(event.event_type).split("'")[1]
                    counter[event_tag] += 1

            # Output the analysis results
            if counter:
                most_common, frequency = counter.most_common(1)[0]
                logger.info(f"Most common event: {most_common} ({frequency} occurrences)")
            else:
                logger.info("No events found in the specified time range.")

        except Exception as e:
            logger.critical(f"Processing interrupted after {count} events: {e}")
            raise


if __name__ == '__main__':
    # Initialize the manager and execute the processing workflow
    manager = DropboxManager()
    manager.process_team_events(lookback_minutes=10)