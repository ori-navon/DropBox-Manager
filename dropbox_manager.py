import sys
from typing import Optional, Generator, Any

from dotenv import load_dotenv
import os
import dropbox
import datetime
from dropbox.team_common import TimeRange

load_dotenv(verbose=True)

class DropboxConfigError(Exception):
    """Raised when configuration is missing or invalid."""
    pass

class DropboxManager:
    def __init__(self, access_token: Optional[str] = None):
        """
        Initialize the DropBox Team Client.
        :param access_token: DropBox Access Token. If None, attempts to load from env vars.
        """
        self.access_token: str = access_token or os.getenv('ACCESS_TOKEN')

        if not self.access_token:
            raise DropboxConfigError("Missing ACCESS_TOKEN. Please set it in your .env file or environment.")

        try:
            self.client = dropbox.DropboxTeam(self.access_token)
        except Exception as e:
            raise Exception(f"SDK initialization failed: {e}")

    def _fetch_team_events(self, lookback_minutes: int = 400) -> Generator[Any, None, None]:
        """
        Yields team events within the specified lookback window.
        """
        start_time = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(minutes=lookback_minutes)

        result = self.client.team_log_get_events(time=TimeRange(start_time=start_time))

        # yield initial batch to process
        yield from result.events

        # pagination loop
        while result.has_more:
            result = self.client.team_log_get_events_continue(result.cursor)
            yield from result.events

    def report_team_events(self):
        """
        Iterates through events and logs their metadata.
        """
        count = 0

        try:
            # iterate over the generator
            for event in self._fetch_team_events():
                count += 1

                event_data = {
                    "timestamp": str(event.timestamp),
                    "event_type": str(event.event_type),
                    "actor": str(event.actor),
                    "origin": str(event.origin),
                    "assets": str(event.assets)
                }

                for key, value in event_data.items():
                    print(f"{key}: {value}")
                print("\n")

        except Exception as e:
            raise Exception(f"Unexpected error during event process: {e}")

if __name__ == '__main__':
    manager = DropboxManager()
    manager.report_team_events()