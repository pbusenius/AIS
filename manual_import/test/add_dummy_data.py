import os
import pandas as pd

from typing import List
from hdfs import InsecureClient


DUMMY_DATA_PATH = "data/"


def get_dummy_files() -> List[str]:
    return [os.path.join(DUMMY_DATA_PATH, i) for i in os.listdir(DUMMY_DATA_PATH)]


def main():
    client = InsecureClient("http://localhost:50070", "test")

    files = get_dummy_files()

    for file in files:
        df = pd.read_csv(file)
        with client.write(f"{os.path.basename(file)}", encoding = 'utf-8') as writer:
            df.to_csv(writer)
        


if __name__ == "__main__":
    main()
