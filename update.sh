#!/bin/bash

years='2019'
types='passing rushing'
weeks='1'

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
