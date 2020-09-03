"""Contains functionality related to Lines"""
import json
import logging
import re

from models import Line
import topic_names as TOPIC


logger = logging.getLogger(__name__)


class Lines:
    """Contains all train lines"""

    line_colors = ['blue', 'green', 'red']

    def __init__(self):
        """Creates the Lines object"""
        self.red_line = Line("red")
        self.green_line = Line("green")
        self.blue_line = Line("blue")

    def process_message(self, message):
        """Processes a station message"""
        if TOPIC.STATIONS in message.topic() or re.search('arrival', message.topic()):
            value = message.value()
            if message.topic() == TOPIC.TRANSFORMED_STATIONS:
                value = json.loads(value)

            line_color = self.decode_color(value['line'])

            if line_color == "green":
                self.green_line.process_message(message)
            elif line_color == "red":
                self.red_line.process_message(message)
            elif line_color == "blue":
                self.blue_line.process_message(message)
            else:
                logger.debug("discarding unknown line msg %s", value["line"])
        elif TOPIC.TURNSTILE_SUMMARY == message.topic():
            self.green_line.process_message(message)
            self.red_line.process_message(message)
            self.blue_line.process_message(message)
        else:
            logger.info("ignoring non-lines message %s", message.topic())

    def decode_color(self, color):
        if isinstance(color, int):
            return self.line_colors[color]
        else:
            return color
