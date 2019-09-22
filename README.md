<p align="center">
  <img src="../master/assets/NGS-spider-logo.png" width="350" title="I'm Nextgenstats-Spider, nice to meet you!">
</p>

# nextgenstats-spider
A Scrapy spider using Selenium to scrape NFL's Next Gen Stats website, inspired by [Deryck97's work](https://github.com/Deryck97/nfl_nextgenstats_data). Nextgenstats-spider currently supports scraping of the passing, receiving and rushing stats. Column names try to follow the standard used by [Deryck97](https://github.com/Deryck97/nfl_nextgenstats_data).

(Please note! Some columns in Deryck97's data are not included here and some numbers don't have the same number of decimals — I suspect this is because not all the data from the JSON is used in the Next Gen Stats frontend.)

## Installation
Nextgenstats-spider is built using Python 3.7 and requires the following Python libraries:
* [scrapy](https://github.com/scrapy/scrapy)
* [scrapy-selenium](https://github.com/clemfromspace/scrapy-selenium)
* [pandas](https://github.com/pandas-dev/pandas)

To install them to your system, follow instructions provided by their maintainers.

To use Selenium with Scrapy, you also need to install a [supported browser](https://www.seleniumhq.org/about/platforms.jsp) as well as the corresponding driver. Nextgenstats-spider is currently written to run with Firefox using geckodriver, although you can modify it to run with your browser and driver of choice. See instructions on installing geckodriver on Mac OS X [here](https://www.kenst.com/2016/12/installing-marionette-firefoxdriver-on-mac-osx/) or Windows [here](https://www.softwaretestinghelp.com/geckodriver-selenium-tutorial/).

## Usage Notes
To run nextgenstats-spider, open a terminal and navigate to the nextgenstat-spider folder.

To get all weeks of passing data in week 8 of the 2018 regular season:
<br>
```scrapy crawl ngs_spider -a type=passing -a year=2018 -a week=8```

## Parameters
Nextgenstats-spider supports a few parameters, to allow for scraping of specific tables. Note that all parameters must be passed with the `-a` option preceding, lest an error be raised.

**Type**
The `type` parameter is mandatory, and can take the values `passing`, `rushing` or `receiving`.

**Year**
The `year` parameter is mandatory, and can currently take values between `2016` and `2019`. The scraper will still run with years outside this interval, but will (obviously) only return data if it exists on the Next Gen Stats website.

**Week**
The 'week' parameter is optional and can take the following values:
* `reg` for all weekly data from the regular season. This is the default value, and will be used if the `week` parameter is not provided.
* `post` for all weekly data from the postseason.
* `all` for the overall season totals (i.e. not broken down by week)
* A single week number, like `5` for data pertaining to a specific week.
* Multiple week numbers separated by comma, like `1, 5, 10`, to fetch data for a set of weeks.
* A range of weeks expressed by the first and last weeks, separated by a colon, e.g. `5:10`.

## Data
Once executed, Nextgenstats-spider will fetch your data and store it in an aptly named .csv file in the `data` folder.

Most of the available data is already stored in this repository as .csv files, and will be updated weekly.

## TODO
* Adding scraping of the Fastest Ball Carrier tables, along with parsing of the play descriptions

## Feedback
For any comments, questions or suggestions, either submit an issue or feel free to contact me on [Twitter](https://twitter.com/larsjaakko).
