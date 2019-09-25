#!/bin/bash

years='2016 2017 2018 2019'
types='passing rushing receiving fastest'
weeks='all reg post'

for year in $years
do
  for type in $types
  do
    for week in $weeks
    do
      echo $year, $type, $week
    done
  done
done
