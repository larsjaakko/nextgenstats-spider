#!/bin/bash

years='2018 2017 2016'
types='fastest'
weeks='all reg post'

for year in $years
do
  for type in $types
  do
    for week in $weeks
    do
      scrapy crawl ngs_spider -a year=$year -a type=$type -a week=$week -a ids=true
    done
  done
done
