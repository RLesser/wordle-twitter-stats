import os
import json
import pandas as pd
import numpy as np
from datetime import datetime
from google.cloud import storage
from google.cloud import bigquery


def get_wordle_num_from_filename(filename):
    # filename in the form of folder/wordle.NUM.api.csv
    # or folder/wordle.NUM.csv
    return filename.split("/")[1].split(".")[1]


def get_wordle_day_blobs(bucket):
    storage_client = storage.Client()
    blobs = storage_client.list_blobs(bucket, prefix="day_files/", delimiter="/")

    for blob in blobs:
        print(blob.name)

    return [b for b in blobs if b.endswith(".csv")]


class UserCounter:
    def __init__(self, bucket_name):
        self.bucket = storage.Client().get_bucket(bucket_name)
        blob = self.bucket.get_blob("metadata/user_id_map.csv")
        if not blob:
            print("UC - creating new file")
            self.df = pd.DataFrame({"user_id": [], "user_index": []})
        else:
            print("UC - getting from existing file")
            self.df = pd.read_csv(f"gs://{bucket_name}/metadata/user_id_map.csv")
        self.df.set_index("user_id", inplace=True)
        print(f"UC - {len(self.df)} rows loaded")
        print("index", self.df.index)

    def get_index(self, user_id):
        if len(self.df) > 50 and len(self.df) < 55:
            print(f"user id: {user_id} | next index: {len(self.df) + 1}")
            print("df:", self.df)
            print("df index:", self.df.index)
            print("user in index:", user_id in self.df.index)
            print("user in index values", user_id in self.df.index.values)
        if user_id not in self.df.index:
            count = len(self.df)
            new_index = count + 1
            self.df.loc[user_id] = [new_index]
            return new_index

        return self.df.loc[user_id]["user_index"]

    def save_data(self):
        bucket_name = self.bucket.name
        self.df.to_csv(f"gs://{bucket_name}/metadata/user_id_map.csv", mode="w")
        print(f"UC - {len(self.df)} rows saved")


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


def get_timestamp(time_string):
    return datetime.strptime(time_string, "%a %b %d %H:%M:%S +0000 %Y")


def contains_interior_win(matrix):
    rows = [matrix[i : i + 5] for i in range(0, len(matrix), 5)]
    try:
        return rows.index("CCCCC") != len(rows) - 1
    except:
        return False


# condenses individual day file
def condense_day_file(bucket, filename):
    print(f"condensing {filename}...")
    # get condensed filename
    condensed_filename = (
        f"condensed_days/wordle.{get_wordle_num_from_filename(filename)}.csv"
    )

    df = pd.read_csv(f"gs://{bucket}/{filename}", dtype={12: str})

    UC = UserCounter(bucket)

    # map surface string to id
    df["surface"] = df["surface"].map(get_surface_id)
    # map time string to timestamp
    df["time"] = df["time"].map(get_timestamp)
    # map user id to user anon index
    df["user_id"] = df["user_id"].map(UC.get_index)

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

    # drop tweet ids
    df.drop("tweet_id", axis=1, inplace=True)

    # # write data to new blob in condensed_days folder
    # df.to_csv(f"gs://{bucket}/{condensed_filename}", index=False, mode="w")

    # print(f"condensed {len(df)} rows into {condensed_filename}!")

    UC.save_data()

    return df


def load_to_bq_condensed_table(dataframe):
    print("loading to condensed data table...")
    client = bigquery.Client()
    project_id = os.environ.get("GCP_PROJECT")
    table_id = f"{project_id}.main.condensed_data"
    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("time", "TIMESTAMP"),
            bigquery.SchemaField("user_id", "INTEGER"),
            bigquery.SchemaField("surface", "INTEGER"),
            bigquery.SchemaField("is_reply", "BOOLEAN"),
            bigquery.SchemaField("is_quote", "BOOLEAN"),
            bigquery.SchemaField("retweets", "INTEGER"),
            bigquery.SchemaField("quotes", "INTEGER"),
            bigquery.SchemaField("favs", "INTEGER"),
            bigquery.SchemaField("replies", "INTEGER"),
            bigquery.SchemaField("language", "STRING"),
            bigquery.SchemaField("wordle_num", "INTEGER"),
            bigquery.SchemaField("rounds", "STRING"),
            bigquery.SchemaField("hard", "BOOLEAN"),
            bigquery.SchemaField("theme", "STRING"),
            bigquery.SchemaField("colorblind", "BOOLEAN"),
            bigquery.SchemaField("win", "BOOLEAN"),
            bigquery.SchemaField("matrix", "STRING"),
        ],
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
        range_partitioning=bigquery.RangePartitioning(
            field="wordle_num",
            range_=bigquery.PartitionRange(start=1, end=4000, interval=1),
        ),
    )
    job = client.load_table_from_dataframe(dataframe, table_id, job_config=job_config)
    print(job.result())


def append_to_bq_wordle_rounds_table(wordle_num):
    print("Appending to Wordle rounds agg table...")
    client = bigquery.Client()
    project_id = os.environ.get("GCP_PROJECT")
    query = f"""
        INSERT INTO {project_id}.main.wordle_rounds_count
        SELECT
            wordle_num,
            rounds,
            COUNT(1)
        FROM
            {project_id}.main.condensed_data
        WHERE
            wordle_num = {wordle_num}
        GROUP BY
            1,
            2
    """
    job = client.query(query)
    print(job.result())


# def moveViewTablesToStorage(bucket):
#     print("Moving views to bucket...")
#     client = bigquery.Client()
#     project_id = os.environ.get("GCP_PROJECT")
#     get_destination_file = lambda name: f"gs://{bucket}/views/{name}"
#     dataset_ref = bigquery.DatasetReference(project_id, "main")
#     extract_job = client.extract_table(
#         dataset_ref.table("wordle_rounds_count"),
#         get_destination_file("wordle_round_count.csv"),
#     )
#     print(extract_job.result())


def main(event, context):
    """Triggered by a change to a Cloud Storage bucket.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """

    bucket = event["bucket"]
    filename = event["name"]

    if not filename.startswith("day_files/"):
        print(f"triggered by {filename}, ending.")
        return

    print(f"triggered by {filename}, beginning process.")

    print(json.dumps(event))
    print(json.dumps(context.__dict__))

    # condense day data as a dataframe
    df = condense_day_file(bucket, filename)
    # write condensed data to bigquery main table
    load_to_bq_condensed_table(df)
    # append to wordle rounds aggregate table
    wordle_num = get_wordle_num_from_filename(filename)
    append_to_bq_wordle_rounds_table(wordle_num)
    # # extract views to GCS
    # moveViewTablesToStorage(bucket)
