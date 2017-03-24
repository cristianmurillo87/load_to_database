from time import time

class Cronometro:
    def __init__(self, start, end):
        self.start = start
        self.end = end

    def get_start(self):
        return self.start

    def get_end(self):
        return self.end

    def calculate_elapsed_time(self):
        delta = self.end - self.start
        horas = delta / 3600
        mins = (horas - int(horas)) * 60
        secs = (mins - int(mins)) * 60
        print ("Elapsed time {} hours {} mins {} secs ".format(int(horas), int(mins), int(secs)))