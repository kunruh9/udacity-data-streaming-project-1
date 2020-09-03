"""Contains functionality related to Weather"""
import logging

logger = logging.getLogger(__name__)


class Weather:
    """Defines the Weather model"""

    statuses = ["sunny", "partly_cloudy", "cloudy", "windy", "precipitation"]

    def __init__(self):
        """Creates the weather model"""
        self.temperature = 70.0
        self.status = "sunny"

    def process_message(self, message):
        """Handles incoming weather data"""
        weather = message.value()
        self.temperature = weather['temperature']
        self.status = self.statuses[weather['status']]
