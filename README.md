<p align="left">
  <img src="../master/assets/NGS-spider-logo.png" width="350" title="I'm Nextgenstats-Spider, nice to meet you!">
</p>

# nextgenstats-spider
A Scrapy spider using Selenium to scrape NFL's Next Gen Stats website, inspired by [Deryck97's work](https://github.com/Deryck97/nfl_nextgenstats_data). Nextgenstats-spider currently supports scraping of the passing, receiving, rushing and fastest ball carrier stats. Column names try to follow the standard used by [Deryck97](https://github.com/Deryck97/nfl_nextgenstats_data).

## Installation
Nextgenstats-spider is built using Python 3.7 and requires the following Python libraries:
* [scrapy](https://github.com/scrapy/scrapy)
* [scrapy-selenium](https://github.com/clemfromspace/scrapy-selenium)
* [pandas >0.25](https://github.com/pandas-dev/pandas)

If you want to include game and play IDs (for easier joining with nflscrapR data), you will also need to install the following packages:
* [requests](https://github.com/psf/requests). Used to pull json data from NFL.
* [tenacity](https://github.com/jd/tenacity). Used for retry logic, as pulling play descriptions is pretty flaky and can fail.

To install the respective libraries, please follow instructions provided by their maintainers.

To use Selenium with Scrapy, you also need to install a [supported browser](https://www.seleniumhq.org/about/platforms.jsp) as well as the corresponding driver. Nextgenstats-spider is currently written to run with Firefox using geckodriver, although you can modify it to run with your browser and driver of choice. See instructions on installing geckodriver on Mac OS X [here](https://www.kenst.com/2016/12/installing-marionette-firefoxdriver-on-mac-osx/) or Windows [here](https://www.softwaretestinghelp.com/geckodriver-selenium-tutorial/).

## Usage notes
To run nextgenstats-spider, open a terminal and navigate to the nextgenstat-spider folder.

To get all passing data in week 8 of the 2018 regular season, with game IDs:
<br>
```
scrapy crawl ngs_spider -a type=passing -a year=2018 -a week=8 -a ids=true
```

## Parameters
Nextgenstats-spider supports a few parameters, to allow for scraping of specific tables. Note that all parameters must be passed with the `-a` option preceding, lest an error be raised.

**Year**<br>
The `year` parameter is required, and can currently take values between `2016` and `2019`. The scraper will still run with years outside this interval, but will (obviously) only return data if it exists on the Next Gen Stats website.

**Type**<br>
The `type` parameter is required, and can take the following values:
* `passing`
* `rushing`
* `receiving`
* `fastest`

The `fastest` type will fetch the [Fastest Ball Carriers](https://nextgenstats.nfl.com/stats/top-plays/fastest-ball-carriers) data.

**Week**<br>
The `week` parameter is optional and can take the following values:
* `reg` for all weekly data from the regular season. This is the default value, and will be used if the `week` parameter is not provided.
* `post` for all weekly data from the postseason.
* `all` for the overall season totals (i.e. not broken down by week)
* A single week number, like `5` for data pertaining to a specific week.
* Multiple week numbers separated by comma, like `1,5,10`, to fetch data for a set of weeks. Do not include any spaces.
* A range of weeks expressed by the first and last weeks, separated by a colon, e.g. `5:10`. Do not include any spaces.

**IDs**<br>
To fetch NFL game IDs or play IDs (for the fastest ball carriers), you need to set this parameter to `true`. By default it will be set to `false`. Please note: getting play IDs for the `fastest` option can take a long time, as the scraper will need to fetch the play descriptions by clicking a button and dismissing a pop-up for each row of data. For a full 17 week season, this can take around 15-20 minutes to complete.

## Data
Once executed, Nextgenstats-spider will fetch your data and store it in an aptly named .csv file in the `data` folder.

Most of the available data is already stored in this repository as .csv files, and will be updated weekly.

## TODO
* Simplifying code to use pandas built-in html table parsing (how did I not know about this??)
* Fetching NFL's official short name data — currently names are shortened by a local function which can miss in cases where NFL adds prefixes to separate between identical names, for instance

## Known issues
* Fetching the play descriptions for the fastest ball carriers will fail at times, seemingly because the page doesn't load correctly. I've added up to 5 retries per page to combat this, but with poor luck you might still get an error.
* NFLs play-by-play descriptions will vary slightly between sources, so sometimes the join will fail. I've tried to clean up a range of known differences, but if you find play description that hasn't joined to a playId — please let me know!

## Feedback
For any comments, questions or suggestions, either submit an issue or feel free to contact me on [Twitter](https://twitter.com/larsjaakko).
