#!/bin/bash

# Define the variables
YEAR_START=$1
YEAR_END=$2
MONTH_START=$3
MONTH_END=$4

# Function to download the data
download_data() {
    taxi_type=$1
    year=$2
    month=$3
    destination="data/raw/${taxi_type}/${year}/$(printf "%02d" $month)"
    url="https://github.com/DataTalksClub/nyc-tlc-data/releases/download/${taxi_type}/${taxi_type}_tripdata_${year}-$(printf "%02d" $month).csv.gz"
    
    # Use curl to check if the file exists
    if curl --output /dev/null --silent --head --fail "$url"; then
        echo "File exists at $url"
        mkdir -p "$destination"
        wget "$url" -P "$destination"
        gunzip "$destination/${taxi_type}_tripdata_${year}-$(printf "%02d" $month).csv.gz"  # Unzip the file
    else
        echo "File does not exist at $url"
        echo "Error: File not found for ${taxi_type} taxi type, year ${year}, month ${month}"
    fi


    # Check if the file exists
    # if [ ! -f "$destination/${taxi_type}_tripdata_${year}-$(printf "%02d" $month).csv" ]; then
    #     echo "Error: File not found for ${taxi_type} taxi type, year ${year}, month ${month}"
    #     return
}

# Yellow taxi data for the specified years
for ((year = YEAR_START; year <= YEAR_END; year++)); do
    for ((month = MONTH_START; month <= MONTH_END; month++)); do
        download_data "yellow" "$year" "$month"
    done
done

# Green taxi data for all months of the specified year
#for ((year = YEAR_START; year <= YEAR_END; year++)); do
#    for ((month = MONTH_START; month <= MONTH_END; month++)); do
#        download_data "green" "$year" "$month"
#    done
#done
