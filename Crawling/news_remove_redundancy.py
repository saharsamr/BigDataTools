import pandas as pd
from sys import argv
import jdatetime


if __name__ == "__main__":

    time = jdatetime.datetime.now()
    df = pd.read_csv(argv[1])
    df = df.drop_duplicates()
    df.to_csv(argv[1])





