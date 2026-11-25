import argparse

parser = argparse.ArgumentParser(description="Process logs from the cc1101-afec custom library.")

parser.add_argument("-s", "--serial", type=str, default="", help="Parse logs from the specified serial connection.")
parser.add_argument("-f", "--file", type=str, default="", help="Parse logs from the specified file")

parser.parse_args()

class LogRow:
    def __init__(self, row: str):
        split = row.split(" ")

        self.__localTimestamp = split[0][1:-1]
        self.__node = split[1]
        self.__tag = split[2]

        self.__log = row
        self.__kwargs = {}

        for arg in split[3::]:
            if "=" in arg:
                localSplit = arg.split("=")
                self.__kwargs[localSplit[0]] = localSplit[1]

    def get_log(self) -> str:
        return self.__log

    def get_kwargs(self) -> dict[str,str]:
        return self.__kwargs

    def get_kwarg(self, key) -> str | None:
        return self.__kwargs.get(key, None)

    def get_kwarg_keys(self) -> list[str]:
        return self.__kwargs.keys()
