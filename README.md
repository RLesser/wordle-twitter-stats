# Wordle Twitter Stats
 
## Overview
This repository holds the data for the wordle twitter project, as well as the python code to scrape the twitter API, and to combine the data files

## Data Pipeline
The data pipeline currently is as follows:
1. `WordleTwitterAPIScrape.py` is ran on a daily basis, fetching the last day's full set of Wordle tweets. These are saved in `data/wordle.<num>.api.csv`
2. `WordleComileFiles.py` is ran, which does three things:
  * `compile_files()` compiles the individual files into `data/all_wordle.csv`
  * `condense_file()` converts `data/all_wordle.csv` into `data/condensed/all_wordle.csv`, removing extraneous info and anonymizing the data.
  * `split_condensed_file()` splits `data/condensed/all_wordle.csv` into individual wordle number files, as `data/`
3. [TODO] Condensed and split data files are then pushed to Github

## Data Privacy
Although all of the collected tweets are publicly available, steps have been taken to protect the user identity behind each tweet:
* Tweet id has been removed
* Tweet creator id has been replaced by an increasing index, still allowing for user-based analysis
* Only the Wordle matrix text is saved
Obviously, it is possible to recover the original tweet even with just this data, but not trivially. I'd rather err on the side of privacy here.

## Data Size
So far, as of 3/14/22 there is a bit over 1GB of condensed data in the dataset. Github has a limit of 100MB per file, which is why I've split them into each wordle date. 

Additionally, Github has a limit of 5GB per repository. Whether it ever hits that amount depends on the trajectory of the game, but it will likely be a while until that happens. This limit (and the privacy goals mentioned above) are why the original csv files are not included.