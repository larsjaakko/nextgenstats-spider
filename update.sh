#!/bin/bash

years='2019'
types='passing rushing receiving fastest'
weeks='all reg'

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
