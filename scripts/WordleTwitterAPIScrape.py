import requests
import os
import json
import re
import csv
import time
import sys
from datetime import datetime, timedelta
import subprocess


# To set your environment variables in your terminal run the following line:
# export 'BEARER_TOKEN'='<your_bearer_token>'
bearer_token = os.environ.get("BEARER_TOKEN")

SEARCH_URL = "https://api.twitter.com/1.1/search/tweets.json"
WORDLE_DAY_ONE = datetime.fromisoformat("2021-06-18")

# Twitter API limits to 450 calls per 15 min
LIMIT = 450
# Data will be saved every 50 api calls
SAVE_INTERVAL = 50

# set environment
ENV = "PC"
if len(sys.argv) > 2 and sys.argv[2] == "github":
    ENV = "GITHUB"

# creates a native mac notification to alert the user to the progress of the program
def notify(title, text):
    if ENV == "GITHUB":
        print(title)
        print(text)
        return

    CMD = """
    on run argv
    display notification (item 2 of argv) with title (item 1 of argv)
    end run
    """
    subprocess.call(["osascript", "-e", CMD, title, text])


# Optional params: start_time,end_time,since_id,until_id,max_results,next_token,
# expansions,tweet.fields,media.fields,poll.fields,place.fields,user.fields


# sets the data file path based on the environment set from CLI args
# when using on github actions (as opposed to locally), "github" flag should be specified
def get_data_file_path(wordle_num):
    if ENV == "GITHUB":
        return "./wordle." + str(wordle_num) + ".api.csv"
    else:
        return "data/wordle." + str(wordle_num) + ".api.csv"


# checks whether it is too early to scrape the given wordle number's tweets
def is_too_early(wordle_num):
    start = WORDLE_DAY_ONE
    wordle_end = start + timedelta(days=3 + wordle_num)
    now = datetime.utcnow()
    return wordle_end > now


def wordle_num_from_current_datetime():
    now: datetime = datetime.utcnow()
    start = WORDLE_DAY_ONE
    latest_num = (now - start).days - 3
    print(f"using latest wordle, which is {latest_num}")
    return latest_num


# generates the twitter API search params for the given wordle number and max id, if applicable
def get_search_params(wordle_num, max_id=None):
    start = WORDLE_DAY_ONE
    wordle_start = start + timedelta(days=wordle_num)
    wordle_end = wordle_start + timedelta(days=3)
    return {
        "q": f'"wordle {wordle_num}" until:{wordle_end.strftime("%Y-%m-%d")} since:{wordle_start.strftime("%Y-%m-%d")} -filter:retweets',
        "result_type": "recent",
        "count": 100,
        "max_id": max_id,
    }


# sets the OAuth info for the API request
def bearer_oauth(r):
    r.headers["Authorization"] = f"Bearer {bearer_token}"
    r.headers["User-Agent"] = "v2RecentSearchPython"
    return r


# gets the API response for the given url and params
def get_response(url, params):
    pause_seconds = 60
    response = requests.get(url, auth=bearer_oauth, params=params)
    if response.status_code == 429:
        # request has been ratelimitted, will be unratelimited in 15 min,
        # but let's recheck every min just in case
        while response.status_code == 429:
            print("RATE LIMIT HIT - pausing for ", pause_seconds, "seconds")
            time.sleep(pause_seconds)
            response = requests.get(url, auth=bearer_oauth, params=params)
    elif response.status_code != 200:
        # if not a ratelimit or ok response, throw an exception
        print()
        raise Exception(response.status_code, response.text)
    return response.json()


# The main event
# Parses the API response, and returns an object containing the cleaned tweets
def process_response(res, wordle_num):
    tweets = res["statuses"]
    if len(tweets) == 0:
        print("END OF TWEET LIST")
        return []
    text_invalid = 0
    squares_invalid = 0
    clean_tweets = []

    for tweet in tweets:
        # easy fields to gather
        clean_tweet = {
            "time": tweet["created_at"],
            "tweet_id": tweet["id"],
            "user_id": tweet["user"]["id"],
            "surface": tweet["source"].split(">")[1].split("<")[0],
            "is_reply": 1 if tweet["in_reply_to_user_id"] != None else 0,
            "is_quote": 1 if tweet["is_quote_status"] else 0,
            "retweets": tweet["retweet_count"],
            "quotes": None,
            "favs": tweet["favorite_count"],
            "replies": None,
            "language": tweet["lang"],
        }
        text = tweet["text"]

        # gets the wordle text string, i.e. "Wordle 250 2/6*"
        wordleRegex = re.compile(
            "Wordle[()#!,\-.:\s]*(\d*)[()#!,\-.:\s]*([1-6X])\/6\*?", re.IGNORECASE
        )
        wordleText = wordleRegex.search(text)
        # parse wordle number, round count, and hard mode from wordle text
        if wordleText is not None:
            clean_tweet["wordle_num"] = wordleText.group(1)
            clean_tweet["rounds"] = (
                6 if wordleText.group(2) == "X" else wordleText.group(2)
            )
            clean_tweet["hard"] = 1 if "/6*" in wordleText.string else 0
        else:
            # must have valid text
            text_invalid += 1
            continue

        # must be current wordle num
        if clean_tweet["wordle_num"] != str(wordle_num):
            text_invalid += 1
            continue

        # clean squares
        valid_chars = ["⬛", "🟨", "🟩", "⬜", "🟧", "🟦", "\n"]
        raw_rows = "".join(c for c in text if c in valid_chars).split("\n")
        rows = [row for row in raw_rows if row != ""]

        # must have between 1 and 6 rows
        if len(rows) > 6 or len(rows) < 1:
            squares_invalid += 1
            continue

        # must have 5 squares in each row
        if any([len(row) != 5 for row in rows]):
            squares_invalid += 1
            continue

        squares = "".join(rows)

        if "⬛" in squares:
            clean_tweet["theme"] = "d"  # dark
        elif "⬜" in squares:
            clean_tweet["theme"] = "l"  # light
        else:
            clean_tweet["theme"] = "u"  # unknown

        clean_tweet["colorblind"] = 1 if "🟧" in squares or "🟦" in squares else 0
        squares = (
            squares.replace("⬛", "A")
            .replace("⬜", "A")
            .replace("🟨", "B")
            .replace("🟦", "B")
            .replace("🟧", "C")
            .replace("🟩", "C")
        )
        clean_tweet["win"] = 1 if squares[-5:] == "CCCCC" else 0
        clean_tweet["matrix"] = squares

        clean_tweets.append(clean_tweet)

    if len(clean_tweets) != 0:
        print(
            "processed",
            len(clean_tweets),
            "tweets. time:",
            clean_tweets[0]["time"],
            "-",
            clean_tweets[-1]["time"],
        )
    else:
        print("processed 0 tweets.")

    print(
        "text_invalid:",
        text_invalid,
        "| squares_invalid:",
        squares_invalid,
        "| saving",
        len(clean_tweets),
        "out of",
        len(tweets),
    )

    return clean_tweets


# save the cleaned tweets currently collected
def save_tweets(clean_tweets, wordle_num, mode):
    if len(clean_tweets) == 0:
        return 0
    print("Saving", len(clean_tweets), "tweets...")
    with open(get_data_file_path(wordle_num), mode) as csvfile:
        writer = csv.DictWriter(csvfile, clean_tweets[0].keys())
        if mode == "w":
            writer.writeheader()
        writer.writerows(clean_tweets)
    return len(clean_tweets)


# get the last saved id, to determine where to start the scraping
def get_last_saved_id(wordle_num):
    try:
        with open(get_data_file_path(wordle_num), "r") as f:
            for line in f:
                pass
            return int(line.split(",")[1])
    except:
        return None


# get responses for one full limit of the API, or 450 calls
def get_full_response_set(wordle_num, restart=False):
    max_id = None
    if is_too_early(wordle_num):
        print("Too early to gather this wordle!")
        return True  # Done

    # if not in restart mode, look for the last saved id
    if not restart:
        print("In continue mode, searching for existing data...")
        max_id = get_last_saved_id(wordle_num)
        # if max_id is None, no saved data, and should switch to restart mode
        if max_id is None:
            print("No existing data found, switching to restart mode")
            restart = True
        else:
            max_id -= 1
            print("Existing data found, continuing from last row")

    i = 0
    tweet_count = 0
    all_tweets = []
    while True:
        try:
            # get the API response
            res = get_response(SEARCH_URL, get_search_params(wordle_num, max_id))
        except Exception as e:
            # if an error, save the existing tweets and exit with the error
            print("ERROR - saving current data")
            save_tweets(all_tweets, wordle_num, "w" if restart else "a")
            raise e
        print("[" + str(i) + "]", end=" ")
        clean_tweets = process_response(res, wordle_num)
        i += 1
        all_tweets += clean_tweets
        if i % SAVE_INTERVAL == 0:
            tweet_count += save_tweets(all_tweets, wordle_num, "w" if restart else "a")
            restart = False
            all_tweets = []
        if i >= LIMIT or len(res["statuses"]) == 0:
            break
        # save the new max index to determine where the next call should start
        max_id = int(
            res["search_metadata"]["next_results"].split("max_id=")[1].split("&q=")[0]
        )
        max_id -= 1

    tweet_count += save_tweets(all_tweets, wordle_num, "w" if restart else "a")
    if len(res["statuses"]) == 0:
        notify(
            "[" + str(wordle_num) + "] Processed " + str(tweet_count) + " tweets",
            "END OF TWEETS!",
        )
        return True  # Done
    else:
        notify(
            "[" + str(wordle_num) + "] Processed " + str(tweet_count) + " tweets",
            "Continuing...",
        )
        return False  # Not Done


# runs for as many response sets as needed to get all the wordle tweets for the given number
def get_all_response_sets(wordle_num):
    is_done = False
    while not is_done:
        is_done = get_full_response_set(wordle_num)


# main function, gets wordle num as first CLI arg
def main():
    wordle_arg = sys.argv[1]
    if wordle_arg == "latest":
        wordle_num = wordle_num_from_current_datetime()
    else:
        wordle_num = int(wordle_arg)
    get_all_response_sets(wordle_num)


if __name__ == "__main__":
    main()
