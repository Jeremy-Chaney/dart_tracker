import requests
import pandas as pd
import zipfile
import os
from datetime import datetime
import argparse

class CustomHelpFormatter(
    argparse.RawDescriptionHelpFormatter,
    argparse.ArgumentDefaultsHelpFormatter,
    argparse.MetavarTypeHelpFormatter):
    pass

# URL to the GTFS ZIP file
gtfs_url = "https://www.dart.org/transitdata/latest/google_transit.zip"
home_dir = os.path.expanduser("~")
save_path = os.path.normpath(os.path.join(home_dir, "Documents", "GTFS", "dart_gtfs.zip"))
extract_path = os.path.normpath(os.path.join(home_dir, "Documents", "GTFS"))

# Ensure the directory for save_path exists
os.makedirs(os.path.dirname(save_path), exist_ok=True)

def extract_gtfs_info(gtfs_url:str=gtfs_url, save_path:str=save_path, extract_path:str=extract_path):
    """
    Extract [GTFS](https://gtfs.org/) (General Transit Feed Specification) ZIP file and print basic information.

    :param gtfs_url: URL to the GTFS ZIP file.
    :param save_path: Local path where the GTFS ZIP file is saved.
    :param extract_path: Local path where the GTFS data will be extracted.
    
    *usage example:*
    ```python
    extract_gtfs_info(
        gtfs_url="https://www.dart.org/transitdata/latest/google_transit.zip",
        save_path="/path/to/save/dart_gtfs.zip",
        extract_path="/path/to/extract/GTFS"
    )
    ```
    """
    print("GTFS Data Information:")
    print(f"URL: {gtfs_url}")
    print(f"Save Path: {save_path}")

    # Extract GTFS ZIP file
    with zipfile.ZipFile(save_path, 'r') as zip_ref:
        zip_ref.extractall(extract_path)

def print_gtfs_info():
    """
    Print basic information from the extracted GTFS data.
    """
    # Load stops data
    stops = pd.read_csv(os.path.join(extract_path, "stops.txt"))
    print("Stops:")
    print(stops.head())

    # Load routes data
    routes = pd.read_csv(os.path.join(extract_path, "routes.txt"))
    print("Routes:")
    print(routes.head())

    # Load trips data
    trips = pd.read_csv(os.path.join(extract_path, "trips.txt"))
    print("Trips:")
    print(trips.head())

    # Load stop times data
    stop_times = pd.read_csv(os.path.join(extract_path, "stop_times.txt"))
    print("Stop Times:")
    print(stop_times.head())

def get_next_train(station_name: str, num_trains_per_route: int = 1, extract_path: str = extract_path):
    """
    Get the next train arrival times for a specified station from the GTFS data.
    Requires the [GTFS](https://gtfs.org/) data to be extracted first.

    :param station_name: Name of the station to get next train info.
    :param num_trains_per_route: Number of upcoming trains to display per route.
    :param extract_path: Local path where the GTFS data is extracted.

    *usage example:*
    ```python
    get_next_train(
        station_name="ADDISON STATION",
        num_trains_per_route=2,
        extract_path="/path/to/extract/GTFS"
    )
    ```
    """
    stops = pd.read_csv(os.path.join(extract_path, "stops.txt"))
    stop_times = pd.read_csv(os.path.join(extract_path, "stop_times.txt"))
    trips = pd.read_csv(os.path.join(extract_path, "trips.txt"))
    calendar = pd.read_csv(os.path.join(extract_path, "calendar.txt"))
    station_stop_ids = stops[stops['stop_name'] == station_name]['stop_id'].tolist()

    if not station_stop_ids:
        print(f"No station found with name: {station_name}")
        return
    else:
        # Filter stop times for the station
        filtered_stop_times = stop_times[stop_times['stop_id'].isin(station_stop_ids)]

        # Convert arrival_time to datetime for filtering
        current_time = datetime.now()
        filtered_stop_times['arrival_time'] = pd.to_datetime(filtered_stop_times['arrival_time'], format='%H:%M:%S', errors='coerce').dt.time

        # Filter for times after the current time
        upcoming_trains = filtered_stop_times[filtered_stop_times['arrival_time'] > current_time.time()]

        # Merge with trips to include trip_headsign and service_id
        upcoming_trains = pd.merge(upcoming_trains, trips, on='trip_id')

        # Check service_id against the current date
        current_day = current_time.strftime('%A').lower()
        calendar = calendar[calendar[current_day] == 1]  # Filter services running today
        upcoming_trains = upcoming_trains[upcoming_trains['service_id'].isin(calendar['service_id'])]

        # Sort by trip_headsign and arrival_time
        upcoming_trains = upcoming_trains.sort_values(by=['trip_headsign', 'arrival_time'])

        print(f"Station '{station_name}' found with stop IDs: {station_stop_ids}")

        # Get the next N trains for each unique trip_headsign
        next_trains_per_headsign = upcoming_trains.groupby('trip_headsign').apply(lambda x: x.head(num_trains_per_route)).reset_index(drop=True)

        # Sort the final list by arrival_time
        next_trains_per_headsign = next_trains_per_headsign.sort_values(by='arrival_time')

        print(f"Next trains headed to {station_name}:")
        print(next_trains_per_headsign[['stop_id', 'trip_id', 'arrival_time', 'departure_time', 'route_id', 'trip_headsign']])

def fetch_gtfs_data(url, path):
    """
    Fetch [GTFS](https://gtfs.org/) (General Transit Feed Specification) data from the specified URL and save it to the given path.

    :param url: URL to download the GTFS ZIP file.
    :param path: Local path to save the GTFS ZIP file.
    
    *usage example:*
    ```python
    fetch_gtfs_data(
        url="https://www.dart.org/transitdata/latest/google_transit.zip",
        path="/path/to/save/dart_gtfs.zip"
    )
    ```
    """
    # Download the file
    response = requests.get(url)
    if response.status_code == 200:
        with open(path, "wb") as file:
            file.write(response.content)
        print(f"GTFS data saved to {path}")
    else:
        print(f"Failed to download GTFS data: {response.status_code}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(    
        formatter_class = CustomHelpFormatter,
        description="Train Schedule Information"
    )
    parser.add_argument("--station", type=str, help="Name of the station to get next train info", default="ADDISON STATION")
    parser.add_argument("--num_print", type=int, help="Number of trains to print per route", default=2)
    parser.add_argument("--fetch", action="store_true", help="Fetch the latest GTFS data")
    args = parser.parse_args()
    station_name = args.station
    fetch_data = args.fetch
    num_trains_per_route = args.num_print

    if fetch_data:
        fetch_gtfs_data(gtfs_url, save_path)
        extract_gtfs_info()
        print_gtfs_info()
    else:
        get_next_train(station_name, num_trains_per_route)