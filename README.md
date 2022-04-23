# Wordle Twitter Stats
 
## Overview
This repository holds the data for the wordle twitter project, as well as the python code to automate the data collection and manipulation.

**This project is a follow-up to my Observable-based data exploration, [Wordle, 15 Million Tweets Later](https://observablehq.com/@rlesser/wordle-twitter-exploration)**.

## Motivation
Why do this? Fair question; three reasons:
1. After publishing that piece, I was curious to see if the trends and correlations would continue to hold true over time, and whether new words would emerge as the hardest or easiest. I didn't want to continually update the prose/structure of the article, so a dashboard made more sense.
2. Given the limited nature of Twitter's search API, it is very hard to fetch data from more than 7 days ago. So this dataset is not easily reproducible if data were to stop being collected.
3. This was a good opportunity to become more familar with github workflows, Google Cloud Platform, and data pipeline architecture more generally.

## Data Pipeline
The data pipeline currently is as follows:
1. The \<TODO DATA SCRAPE\> workflow is run on a daily basis.
    1. This workflow calls `WordleTwitterAPIScrape.py` which fetches the last day's full set of Wordle tweets.
    2. This data is compiled to a CSV and uploaded to Google Cloud Storage (GCS)
2. The upload to GCS triggers a Cloud Function, which runs `GCPCompileFiles.py`
    1. This script condenses and anonymizes the data.
    2. The data is then uploaded to a Google BigQuery (GBQ) dataset holding all the data from previous days
    3. The script then runs queries on the GBQ dataset to generate summary tables, such as round counts by day
    4. The script then triggers the `download_views.yml` workflow in the Github repo
3. The `download_views.yml` is triggered, which downloads the summary data.
    1. The queries run against GBQ are stored in `GHQueryForViewData.py`.
    2. The data is saved to the CSVs in `data_views/`

## Dashboard
Dashboard to come

## Data Privacy
Although all of the collected tweets are publicly available, steps have been taken to protect the user identity behind each tweet:
* Tweet id has been removed
* Tweet creator id has been replaced by an increasing index, still allowing for user-based analysis
* Only the Wordle matrix text is saved

Obviously, it is possible to recover the original tweet even with just this data, but not trivially.
