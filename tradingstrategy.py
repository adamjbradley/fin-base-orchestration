import logging

from datetime import datetime, timedelta

class Strategy:
    def __init__(self):

        logging.basicConfig(format='%(asctime)s  %(levelname)s %(message)s',
          level=logging.INFO)

        self.timeToClose = None

    # If current price is greater than open
    def SimpleStrategy(self, contracts):
        pass

#strategy = Strategy()
#strategy.SimpleStrategy(None)
