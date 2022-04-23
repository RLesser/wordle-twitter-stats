import csv
import os
import time
from datetime import datetime
import glob
import pandas as pd
import numpy as np

# returns the surface index for the given surface string
def get_surface_id(surface_string):
    if surface_string == "Twitter for iPhone":
        return 1
    if surface_string == "Twitter for Android":
        return 2
    if surface_string == "Twitter Web App":
        return 3
    if surface_string == "Twitter for iPad":
        return 4
    if surface_string == "Tweetbot for iΟS":
        return 5
    if surface_string == "TweetDeck":
        return 6
    return 7


# returns the given datetime string in timestamp form
def get_timestamp(time_string):
    return int(datetime.strptime(time_string, "%a %b %d %H:%M:%S +0000 %Y").timestamp())


# whether the given matrix contains a win not in the final row
# this signals that the matrix is invalid
def contains_interior_win(matrix):
    rows = [matrix[i : i + 5] for i in range(0, len(matrix), 5)]
    try:
        return rows.index("CCCCC") != len(rows) - 1
    except:
        return False


# counter class used to replace the user id with an increasing index
# newly seen user ids are given the next index
class PosterCounter:
    poster_count = 0
    poster_id_map = {}

    def get_poster_index(self, id):
        if id in self.poster_id_map:
            return self.poster_id_map[id]
        else:
            self.poster_count += 1
            self.poster_id_map[id] = self.poster_count
            return self.poster_count


# Opens data/all_wordle.csv, containing every wordle tweet in full form,
# and condenses it into data/condensed/all_wordle.csv
def condense_file():
    i = 0
    PC = PosterCounter()
    with open("data/condensed/all_wordle.csv", "w") as f:
        f.write("")
    for df in pd.read_csv("data/all_wordle.csv", chunksize=100000, dtype={12: str}):
        print(i)
        # map surface string to id
        df["surface"] = df["surface"].map(get_surface_id)
        # map time string to timestamp
        df["time"] = df["time"].map(get_timestamp)
        # map user id to user anon index
        df["user_id"] = df["user_id"].map(PC.get_poster_index)

        # add column for interior win matrices, then filter by it and drop it
        df["contains_interior_win"] = df["matrix"].map(contains_interior_win)
        df = df[df["contains_interior_win"] == False]
        df.drop("contains_interior_win", axis=1, inplace=True)

        # fix wins to include colorblind wins
        df.loc[(df["colorblind"] == 1) & df["matrix"].str.endswith("CCCCC"), "win"] = 1
        # fix lowercase x to uppercase
        df.loc[df["rounds"] == "x", "rounds"] = "X"
        # fix rounds for when rounds = 6 and win = 0
        df.loc[(df["rounds"] == "6") & (df["win"] == 0), "rounds"] = "X"

        # drop rows where round count != matrix length * 5
        df["matrix_size_mismatch"] = df["matrix"].str.len() / 5 != pd.to_numeric(
            np.where(df["rounds"] == "X", 6, df["rounds"])
        )
        df = df[df["matrix_size_mismatch"] == False]
        df.drop("matrix_size_mismatch", axis=1, inplace=True)

        # drop rows where round = X and win = 1, or round != X and win = 0
        df["matrix_win_mismatch"] = ((df["win"] == 1) & (df["rounds"] == "X")) | (
            (df["win"] == 0) & (df["rounds"] != "X")
        )
        df = df[df["matrix_win_mismatch"] == False]
        df.drop("matrix_win_mismatch", axis=1, inplace=True)

        # drop rows where round count
        # print(df)
        # print(df.info())
        # break

        # # drop tweet ids
        df.drop("tweet_id", axis=1, inplace=True)
        print(i)
        if i == 0:
            df.to_csv("data/condensed/all_wordle.csv", index=False, mode="w")
        else:
            df.to_csv(
                "data/condensed/all_wordle.csv",
                mode="a",
                header=False,
                index=False,
            )
        i += 1
    print(i)


def create_combined_file(filenames, wordle_num):
    ids = {}
    dupes = 0
    a = time.time()
    if wordle_num is None:
        write_file = "data/all_wordle.csv"
    else:
        write_file = "data/all_wordle." + str(wordle_num) + ".csv"

    with open(write_file, "w") as fout:
        for filename in filenames:
            print(filename)
            file_rows = 0
            file_dupes = 0
            with open(filename) as f:
                if filename != filenames[0]:
                    f.__next__()
                for line in f:
                    tweet_id = line.split(",")[1]
                    if tweet_id not in ids:
                        ids[tweet_id] = 1
                        file_rows += 1
                        fout.write(line)
                    else:
                        file_dupes += 1
                        dupes += 1
            print(file_rows, "saved | ", file_dupes, "dupes")
    b = time.time()
    print("time:", b - a)
    print("duplicates:", dupes)
    print("saved", len(ids))


def get_filenames_for_wordle_num(wordle_num):
    return sorted(glob.glob("data/wordle." + str(wordle_num) + "*.csv"))


def get_filenames_for_all_wordles():
    return sorted(glob.glob("data/wordle.*.csv"))


def compile_files(wordle_num=None):
    if wordle_num is not None:
        files = get_filenames_for_wordle_num(wordle_num)
    else:
        files = get_filenames_for_all_wordles()
    create_combined_file(files, wordle_num)


# splits the condensed file into individual wordle number files
def split_condensed_file():
    chunk_num = 0
    wordle_num = None
    continuation = False
    for df in pd.read_csv(
        "data/condensed/all_wordle.csv", chunksize=100000, dtype={12: str}
    ):
        print("getting chunk", chunk_num)
        if wordle_num == None:
            wordle_num = df.loc[0]["wordle_num"]
        while True:
            tweets_for_num = df[df["wordle_num"] == wordle_num]
            tweets_for_next_num = df[df["wordle_num"] == wordle_num + 1]
            # if there are tweets for the current num, save them
            if len(tweets_for_num) > 0:
                # if it is a continuation from the last chunk,
                # append to the existing file
                if continuation:
                    print(
                        "continuing to save wordle",
                        wordle_num,
                        "-",
                        str(len(tweets_for_num)),
                        "tweets",
                    )
                    tweets_for_num.to_csv(
                        "data/condensed/wordle." + str(wordle_num) + ".csv",
                        mode="a",
                        header=False,
                        index=False,
                    )
                    continuation = False
                else:
                    print(
                        "saving wordle",
                        wordle_num,
                        "-",
                        str(len(tweets_for_num)),
                        "tweets",
                    )
                    tweets_for_num.to_csv(
                        "data/condensed/wordle." + str(wordle_num) + ".csv",
                        index=False,
                        mode="w",
                    )
            # if there are no tweets for the next number,
            # set continuation to true, and break to get next chunk
            if len(tweets_for_next_num) == 0:
                continuation = True
                break

            # if there are tweets for the next number,
            # incriment the wordle_num
            wordle_num += 1
        chunk_num += 1


def main():
    compile_files()
    condense_file()
    split_condensed_file()


if __name__ == "__main__":
    main()
