<p align="left">
  <img src="../master/assets/NGS-spider-logo.png" width="350" title="I'm Nextgenstats-Spider, nice to meet you!">
</p>

# nextgenstats-spider
A Scrapy spider using Selenium to scrape NFL's Next Gen Stats website, inspired by [Deryck97's work](https://github.com/Deryck97/nfl_nextgenstats_data). Nextgenstats-spider currently supports scraping of the passing, receiving and rushing stats. Column names try to follow the standard used by [Deryck97](https://github.com/Deryck97/nfl_nextgenstats_data).

## Installation
Nextgenstats-spider is built using Python 3.7 and requires the following Python libraries:
* [scrapy](https://github.com/scrapy/scrapy)
* [scrapy-selenium](https://github.com/clemfromspace/scrapy-selenium)
* [pandas](https://github.com/pandas-dev/pandas)
* [requests](https://github.com/psf/requests)

To install them to your system, please follow instructions provided by their maintainers.

To use Selenium with Scrapy, you also need to install a [supported browser](https://www.seleniumhq.org/about/platforms.jsp) as well as the corresponding driver. Nextgenstats-spider is currently written to run with Firefox using geckodriver, although you can modify it to run with your browser and driver of choice. See instructions on installing geckodriver on Mac OS X [here](https://www.kenst.com/2016/12/installing-marionette-firefoxdriver-on-mac-osx/) or Windows [here](https://www.softwaretestinghelp.com/geckodriver-selenium-tutorial/).

## Usage Notes
To run nextgenstats-spider, open a terminal and navigate to the nextgenstat-spider folder.

To get all passing data in week 8 of the 2018 regular season:
<br>
```scrapy crawl ngs_spider -a type=passing -a year=2018 -a week=8```

## Parameters
Nextgenstats-spider supports a few parameters, to allow for scraping of specific tables. Note that all parameters must be passed with the `-a` option preceding, lest an error be raised.

**Year**<br>
The `year` parameter is required, and can currently take values between `2016` and `2019`. The scraper will still run with years outside this interval, but will (obviously) only return data if it exists on the Next Gen Stats website.

**Type**<br>
The `type` parameter is requireed, and can take the following values:
* `passing`
* `rushing`
* `receiving`
* `fastest`

The `fastest` type will fetch the [Fastest Ball Carriers](https://nextgenstats.nfl.com/stats/top-plays/fastest-ball-carriers) data. Do note: to be able to join the plays back to nflscrapR gameIDs, nextgenstats-spider needs to open all the popups with the game descriptions — this can be a slow process, and can also consume a lot of memory, as a separate browser instance needs to be launched for each URL.

**Week**<br>
The 'week' parameter is optional and can take the following values:
* `reg` for all weekly data from the regular season. This is the default value, and will be used if the `week` parameter is not provided.
* `post` for all weekly data from the postseason.
* `all` for the overall season totals (i.e. not broken down by week)
* A single week number, like `5` for data pertaining to a specific week.
* Multiple week numbers separated by comma, like `1,5,10`, to fetch data for a set of weeks. Do not include any spaces.
* A range of weeks expressed by the first and last weeks, separated by a colon, e.g. `5:10`. Do not include any spaces.

## Data
Once executed, Nextgenstats-spider will fetch your data and store it in an aptly named .csv file in the `data` folder.

Most of the available data is already stored in this repository as .csv files, and will be updated weekly.

## TODO

## Feedback
For any comments, questions or suggestions, either submit an issue or feel free to contact me on [Twitter](https://twitter.com/larsjaakko).
