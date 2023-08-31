"""Contains functionality related to Weather"""
import json
import logging


logger = logging.getLogger(__name__)


class Weather:
    """Defines the Weather model"""

    def __init__(self):
        """Creates the weather model"""
        self.temperature = 70.0
        self.status = "sunny"

    def process_message(self, message):
        """Handles incoming weather data"""
        value = json.loads(message.value())
        
        temperature = value.get("temperature")
        if not isinstance(temperature, float):
            logger.error(f"Unable to process 'temperature' value with type: {type(temperature)}")
        self.temperature = temperature
        
        status = value.get("status")
        if not isinstance(status, float):
            logger.error(f"Unable to process 'status' value with type: {type(status)}")
        self.status = status
