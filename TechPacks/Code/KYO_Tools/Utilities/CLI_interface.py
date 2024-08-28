import sys
from datetime import datetime


class CLI_interface(object):

    def __init__(self):
        self.PINK = '\033[95m'
        self.BLUE = '\033[36m'
        self.GREEN = '\033[92m'
        self.YELLOW = '\033[93m'
        self.RED = '\033[91m'
        self.ENDC = '\033[0m'
        self.BOLD = '\033[1m'
        self.UNDERLINE = '\033[4m'

    def Green(self, message, end=True):
        self._printMessage(self.GREEN + self.BOLD, message, end)

    def White(self, message, end=True):
        self._printMessage(self.BOLD, message, end)

    def Yellow(self, message, end=True):
        self._printMessage(self.YELLOW + self.BOLD, message, end)

    def Pink(self, message, end=True):
        self._printMessage(self.PINK + self.BOLD, message, end)

    def Blue(self, message, end=True):
        self._printMessage(self.BLUE + self.BOLD, message, end)

    def Red(self, message, end=True):
        self._printMessage(self.RED + self.BOLD, message, end)

    def _printMessage(self, format, message, end):
        # time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        line = format + message
        if end:
            line = line + self.ENDC
        print(line)

    def getCommandInput(self, message):
        response = input(self.YELLOW + message + ': ')
        if response.upper() == 'EXIT':
            raise Exception('Shutdown')

        print(self.ENDC)
        return response