from dotenv import load_dotenv
import os
import dropbox
import datetime
from dropbox.team_common import TimeRange

load_dotenv(verbose=True)

class DropboxManager:
    def __init__(self):
        self.access_token: str = os.getenv('ACCESS_TOKEN')
        self.client: dropbox.dropbox_client.DropboxTeam = dropbox.DropboxTeam(self.access_token)

    def fetch_team_events(self, lookback_minutes: int = 10):
        """
        Retrieves all team events within the specified lookback window.
        :param lookback_minutes: The number of minutes to look back from the current UTC time.
        :return: A list of TeamEvent objects.
        """
        start_time = datetime.datetime.now(datetime.UTC) - datetime.timedelta(minutes=lookback_minutes)

        result = self.client.team_log_get_events(time=TimeRange(start_time=start_time))
        events = result.events

        while result.has_more:
            result = self.client.team_log_get_events_continue(result.cursor)
            events.extend(result.events)

        return events

if __name__ == '__main__':
    manager = DropboxManager()
    manager.fetch_team_events()