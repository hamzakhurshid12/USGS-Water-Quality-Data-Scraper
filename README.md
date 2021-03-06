# USGS-Water-Quality-Data-Scraper
A tool to scrap data of water quality of all data stations from USGS website. The data can be further used for analysis and Machine Learning.

## Overview
USGS has a huge database of collecting water quality from various locations in all US states. There are a total of near 2000 locations where these quality parameters are monitored.The data is available as back as before 1990 and is updated frequently. 
However, they do not allow an option for users to download that data, and it is only available for viewing purposes.
We have created this tool which scraps their whole database  and stores the output as JSON files for each location.
The result is an offline database of historical water quality data which can be used for research purposes.

The data can be viewed at https://nwis.waterdata.usgs.gov/nwis/current/?type=quality

## Requirements
Python3

A stable and fast internet connection

RAM greater or equal to 6 GB is preferred.

## Output
The output is in JSON, which is the standard web data convention now. You get a JSON file for each of the data collection sites available at USGS. The JSON file can parsed for further processing of data or visulazation of it.

### Note
This work is only for research purpose. We do not intend to promote scraping it too often as it is unethical to put this much load on a website's servers. We are working to upload the database we scrapped and make it available for download to avoid other people scraping it each time.
