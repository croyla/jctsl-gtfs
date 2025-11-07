#!/usr/bin/env python3
"""
GTFS Generator for JCTSL Bus Data
Fetches data from Omnificent APIs and generates GTFS feed
"""

import argparse
import json
import os
import shutil
import zipfile
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Set, Tuple
import requests
from math import radians, cos, sin, asin, sqrt
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# API Configuration
API_BASE_URL = "https://www.omnificent.co.in/OMB"
AUTH_TOKEN = os.getenv("AUTH_TOKEN")
USER_ID = os.getenv("USER_ID")
CITY_ID = os.getenv("CITY_ID", "1")

if not AUTH_TOKEN or not USER_ID:
    raise ValueError(
        "AUTH_TOKEN and USER_ID must be set in .env file. "
        "Please copy .env.example to .env and fill in your credentials."
    )

HEADERS = {
    "Auth-token": AUTH_TOKEN,
    "Content-Type": "application/json",
    "User-ID": USER_ID,
}


class GTFSGenerator:
    def __init__(self):
        self.raw_dir = Path("raw")
        self.output_dir = Path("gtfs_output")
        self.raw_dir.mkdir(exist_ok=True)
        self.output_dir.mkdir(exist_ok=True)

        # Data structures
        self.all_routes = []
        self.unique_stops = {}  # stop_id -> stop_info
        self.route_mappings = {}  # route_long_name -> {short_name, sequences}

    def haversine_distance(self, lon1: float, lat1: float, lon2: float, lat2: float) -> float:
        """
        Calculate the great circle distance between two points on earth (in kilometers)
        """
        lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
        dlon = lon2 - lon1
        dlat = lat2 - lat1
        a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
        c = 2 * asin(sqrt(a))
        km = 6371 * c
        return km

    # ==================== DATA COLLECTION PHASE ====================

    def fetch_all_routes(self):
        """Fetch all available routes and save to raw/all_routes.json"""
        print("Fetching all routes...")
        url = f"{API_BASE_URL}/Log/getAvailableroutes"
        data = {"city_id": CITY_ID}

        response = requests.post(url, headers=HEADERS, json=data)
        response.raise_for_status()

        routes_data = response.json()

        # Save raw data
        with open(self.raw_dir / "all_routes.json", "w") as f:
            json.dump(routes_data, f, indent=2)

        self.all_routes = routes_data.get("respData", [])
        print(f"Found {len(self.all_routes)} routes")
        return self.all_routes

    def fetch_route_stops(self, route_id: str):
        """Fetch stop sequence for a specific route"""
        print(f"Fetching stops for route {route_id}...")
        url = f"{API_BASE_URL}/rt/rvsearching"
        data = {"route_id": route_id}

        response = requests.post(url, headers=HEADERS, json=data)
        response.raise_for_status()

        stops_data = response.json()

        # Save to raw/{route_id}/stops.json
        route_dir = self.raw_dir / str(route_id)
        route_dir.mkdir(exist_ok=True)

        with open(route_dir / "stops.json", "w") as f:
            json.dump(stops_data, f, indent=2)

        return stops_data

    def fetch_route_schedules(self, route_long_name: str, route_id: str):
        """Fetch schedule information for a specific route"""
        print(f"Fetching schedules for route {route_long_name}...")
        url = f"{API_BASE_URL}/log/gettrps"
        data = {"route_id": route_long_name}

        response = requests.post(url, headers=HEADERS, json=data)
        response.raise_for_status()

        schedule_data = response.json()

        # Save to raw/{route_id}/schedules.json
        route_dir = self.raw_dir / str(route_id)
        route_dir.mkdir(exist_ok=True)

        with open(route_dir / "schedules.json", "w") as f:
            json.dump(schedule_data, f, indent=2)

        return schedule_data

    def load_existing_routes(self):
        """Load routes from existing raw/all_routes.json"""
        print("\n" + "="*60)
        print("PHASE 1: LOADING EXISTING DATA")
        print("="*60 + "\n")

        routes_file = self.raw_dir / "all_routes.json"

        if not routes_file.exists():
            raise FileNotFoundError(
                f"No existing routes data found at {routes_file}. "
                "Please run without --skip-api first to fetch data."
            )

        with open(routes_file) as f:
            routes_data = json.load(f)

        self.all_routes = routes_data.get("respData", [])
        print(f"Loaded {len(self.all_routes)} routes from existing data")
        return self.all_routes

    def collect_all_data(self):
        """Run all API requests and store raw data"""
        print("\n" + "="*60)
        print("PHASE 1: DATA COLLECTION")
        print("="*60 + "\n")

        # Fetch all routes
        self.fetch_all_routes()

        # Fetch stops and schedules for each route
        for route in self.all_routes:
            route_id = route.get("route_id")
            route_long_name = route.get("route_long_name", route.get("route_name", ""))

            if route_id:
                try:
                    self.fetch_route_stops(route_id)
                    self.fetch_route_schedules(route_long_name, route_id)
                except Exception as e:
                    print(f"Error fetching data for route {route_id}: {e}")

        print("\nData collection complete!")

    # ==================== DATA PROCESSING PHASE ====================

    def generate_stop_id(self, stop_name: str, lat: float, lon: float) -> str:
        """Generate a unique stop ID based on location"""
        # Use coordinates rounded to 5 decimal places for uniqueness
        return f"stop_{lat:.5f}_{lon:.5f}".replace(".", "_").replace("-", "n")

    def process_unique_stops(self):
        """Generate comprehensive unique list of all stops"""
        print("\n" + "="*60)
        print("PHASE 2: PROCESSING UNIQUE STOPS")
        print("="*60 + "\n")

        stop_locations = {}  # (lat, lon) -> stop_info for deduplication

        for route in self.all_routes:
            route_id = route.get("route_id")
            route_dir = self.raw_dir / str(route_id)
            stops_file = route_dir / "stops.json"

            if not stops_file.exists():
                continue

            with open(stops_file) as f:
                stops_data = json.load(f)

            # Extract stops from the response
            stops_list = stops_data.get("respData", {}).get("route", [])

            for stop in stops_list:
                stop_name = stop.get("stop_name", "").strip()
                stop_code = stop.get("stop_code", "").strip()
                lat = float(stop.get("latitude", 0))
                lon = float(stop.get("longitude", 0))

                if not stop_name or lat == 0 or lon == 0:
                    continue

                # Round coordinates for deduplication (within ~11 meters)
                loc_key = (round(lat, 4), round(lon, 4))

                if loc_key not in stop_locations:
                    stop_id = self.generate_stop_id(stop_name, lat, lon)
                    stop_locations[loc_key] = {
                        "stop_id": stop_id,
                        "stop_name": stop_name,
                        "stop_code": stop_code,
                        "stop_lat": lat,
                        "stop_lon": lon,
                    }
                    self.unique_stops[stop_id] = stop_locations[loc_key]

        print(f"Found {len(self.unique_stops)} unique stops")
        return self.unique_stops

    def get_stop_id_from_location(self, lat: float, lon: float) -> str:
        """Find stop ID from coordinates"""
        loc_key = (round(lat, 4), round(lon, 4))

        # Search for matching stop
        for stop_id, stop_info in self.unique_stops.items():
            stop_lat = round(stop_info["stop_lat"], 4)
            stop_lon = round(stop_info["stop_lon"], 4)

            if (stop_lat, stop_lon) == loc_key:
                return stop_id

        # If not found, generate new stop ID
        return self.generate_stop_id(f"Stop_{lat}_{lon}", lat, lon)

    def process_route_sequences(self):
        """Generate route mappings with stop sequences"""
        print("\n" + "="*60)
        print("PHASE 3: PROCESSING ROUTE SEQUENCES")
        print("="*60 + "\n")

        for route in self.all_routes:
            route_id = route.get("route_id")
            route_short_name = route.get("route_orgno", str(route_id)).strip()

            route_dir = self.raw_dir / str(route_id)
            stops_file = route_dir / "stops.json"

            if not stops_file.exists():
                continue

            with open(stops_file) as f:
                stops_data = json.load(f)

            stops_list = stops_data.get("respData", {}).get("route", [])

            if not stops_list:
                continue

            # Build stop sequence
            stop_sequence = []
            for stop in stops_list:
                stop_name = stop.get("stop_name", "").strip()
                lat = float(stop.get("latitude", 0))
                lon = float(stop.get("longitude", 0))

                if lat != 0 and lon != 0:
                    stop_id = self.get_stop_id_from_location(lat, lon)
                    stop_sequence.append({
                        "stop_id": stop_id,
                        "stop_name": stop_name,
                        "lat": lat,
                        "lon": lon,
                    })

            if len(stop_sequence) < 2:
                continue

            # Generate long name from first and last stop
            first_stop = stop_sequence[0]["stop_name"]
            last_stop = stop_sequence[-1]["stop_name"]
            route_long_name = f"{first_stop} to {last_stop}"

            # Store route mapping
            if route_short_name not in self.route_mappings:
                self.route_mappings[route_short_name] = {
                    "route_id": route_id,
                    "route_short_name": route_short_name,
                    "route_long_name": route_long_name,
                    "sequences": []
                }

            self.route_mappings[route_short_name]["sequences"].append({
                "direction_id": len(self.route_mappings[route_short_name]["sequences"]),
                "stops": stop_sequence,
                "original_route_id": route_id,
            })

        print(f"Processed {len(self.route_mappings)} unique routes")
        return self.route_mappings

    def merge_bidirectional_routes(self):
        """Merge routes with same short name into bidirectional routes"""
        print("\n" + "="*60)
        print("PHASE 4: MERGING BIDIRECTIONAL ROUTES")
        print("="*60 + "\n")

        merged_count = 0

        for short_name, route_info in self.route_mappings.items():
            sequences = route_info["sequences"]

            if len(sequences) >= 2:
                # Update long name to show bidirectional
                first_seq = sequences[0]["stops"]
                last_seq = sequences[-1]["stops"]

                stop_a = first_seq[0]["stop_name"]
                stop_b = first_seq[-1]["stop_name"]

                route_info["route_long_name"] = f"{stop_a} <-> {stop_b}"
                merged_count += 1
                print(f"Merged route {short_name}: {route_info['route_long_name']}")

        print(f"\nMerged {merged_count} bidirectional routes")

    def filter_outlier_stops(self, stop_sequence: List[Dict], max_distance_km: float = 10.0) -> List[Dict]:
        """Remove stops that are too far from adjacent stops (likely data errors)

        Smart detection: If A→B and B→C are both far, but A→C is close,
        then B is the outlier, not A or C.

        Args:
            stop_sequence: List of stops with coordinates
            max_distance_km: Maximum distance in km between consecutive stops

        Returns:
            Filtered stop sequence
        """
        if len(stop_sequence) <= 2:
            return stop_sequence

        # Build list of stops to keep (True) or remove (False)
        keep = [True] * len(stop_sequence)
        removed_count = 0

        # Check each middle stop
        for i in range(1, len(stop_sequence) - 1):
            prev_stop = stop_sequence[i - 1]
            curr_stop = stop_sequence[i]
            next_stop = stop_sequence[i + 1]

            # Calculate distances
            dist_prev_to_curr = self.haversine_distance(
                prev_stop["lon"], prev_stop["lat"],
                curr_stop["lon"], curr_stop["lat"]
            )

            dist_curr_to_next = self.haversine_distance(
                curr_stop["lon"], curr_stop["lat"],
                next_stop["lon"], next_stop["lat"]
            )

            dist_prev_to_next = self.haversine_distance(
                prev_stop["lon"], prev_stop["lat"],
                next_stop["lon"], next_stop["lat"]
            )

            # If both distances to current stop are large,
            # but distance skipping current stop is small,
            # then current stop is the outlier
            if (dist_prev_to_curr > max_distance_km and
                dist_curr_to_next > max_distance_km and
                dist_prev_to_next < max_distance_km):
                keep[i] = False
                removed_count += 1
                print(f"  Removed outlier stop '{curr_stop['stop_name']}' "
                      f"(prev→curr: {dist_prev_to_curr:.2f}km, curr→next: {dist_curr_to_next:.2f}km, "
                      f"prev→next: {dist_prev_to_next:.2f}km)")

        # Build filtered list
        filtered = [stop_sequence[i] for i in range(len(stop_sequence)) if keep[i]]

        if removed_count > 0:
            print(f"  Filtered out {removed_count} outlier stops from sequence")

        return filtered

    def calculate_distances(self, stop_sequence: List[Dict]) -> List[float]:
        """Calculate distances between consecutive stops"""
        distances = [0.0]  # First stop has 0 distance

        for i in range(1, len(stop_sequence)):
            prev_stop = stop_sequence[i-1]
            curr_stop = stop_sequence[i]

            dist = self.haversine_distance(
                prev_stop["lon"], prev_stop["lat"],
                curr_stop["lon"], curr_stop["lat"]
            )
            distances.append(dist)

        return distances

    def generate_trips_and_times(self):
        """Generate trips and stop times based on schedules"""
        print("\n" + "="*60)
        print("PHASE 5: GENERATING TRIPS AND STOP TIMES")
        print("="*60 + "\n")

        self.trips = []
        self.stop_times = []
        trip_counter = 1

        for short_name, route_info in self.route_mappings.items():
            # Get schedule data
            route_id = route_info["route_id"]
            route_dir = self.raw_dir / str(route_id)
            schedule_file = route_dir / "schedules.json"

            # Default schedule if not available
            default_schedules = [
                {"start_time": "06:00:00", "end_time": "07:00:00"},
                {"start_time": "08:00:00", "end_time": "09:00:00"},
                {"start_time": "10:00:00", "end_time": "11:00:00"},
                {"start_time": "14:00:00", "end_time": "15:00:00"},
                {"start_time": "17:00:00", "end_time": "18:00:00"},
                {"start_time": "19:00:00", "end_time": "20:00:00"},
            ]

            schedules = default_schedules

            if schedule_file.exists():
                with open(schedule_file) as f:
                    schedule_data = json.load(f)
                    trips_data = schedule_data.get("respData", [])

                    if trips_data:
                        schedules = []
                        for trip_data in trips_data:
                            start_time = trip_data.get("start_time", "")
                            end_time = trip_data.get("end_time", "")

                            if start_time and end_time:
                                schedules.append({
                                    "start_time": start_time,
                                    "end_time": end_time,
                                })

            # Generate trips for each direction
            for seq_info in route_info["sequences"]:
                direction_id = seq_info["direction_id"]
                stops = seq_info["stops"]

                if len(stops) < 2:
                    continue

                # Filter out outlier stops (>10km from neighbors)
                stops = self.filter_outlier_stops(stops, max_distance_km=100.0)

                if len(stops) < 2:
                    print(f"  Skipping route {short_name} direction {direction_id} - not enough stops after filtering")
                    continue

                # Calculate distances
                distances = self.calculate_distances(stops)
                total_distance = sum(distances)

                if total_distance == 0:
                    total_distance = 1.0  # Avoid division by zero

                # Generate trips for each schedule
                for schedule in schedules:
                    trip_id = f"trip_{trip_counter}"
                    trip_counter += 1

                    # Create trip
                    self.trips.append({
                        "route_id": short_name,
                        "service_id": "weekday",
                        "trip_id": trip_id,
                        "trip_headsign": stops[-1]["stop_name"],
                        "direction_id": direction_id,
                    })

                    # Parse start and end times
                    start_time = schedule["start_time"]
                    end_time = schedule["end_time"]

                    try:
                        start_dt = datetime.strptime(start_time, "%H:%M:%S")
                        end_dt = datetime.strptime(end_time, "%H:%M:%S")

                        # Handle case where end time is on next day
                        if end_dt < start_dt:
                            end_dt += timedelta(days=1)

                        total_seconds = (end_dt - start_dt).total_seconds()
                    except:
                        total_seconds = 3600  # Default 1 hour
                        start_dt = datetime.strptime("08:00:00", "%H:%M:%S")

                    # Generate stop times based on distance proportion
                    current_time = start_dt
                    cumulative_distance = 0

                    for idx, stop in enumerate(stops):
                        stop_sequence_num = idx + 1

                        # Calculate arrival time based on distance proportion
                        if idx > 0:
                            cumulative_distance += distances[idx]
                            time_proportion = cumulative_distance / total_distance
                            seconds_elapsed = total_seconds * time_proportion
                            current_time = start_dt + timedelta(seconds=seconds_elapsed)

                        time_str = current_time.strftime("%H:%M:%S")

                        # Handle times >= 24:00:00
                        if current_time.day > start_dt.day:
                            hours = 24 + current_time.hour
                            time_str = f"{hours:02d}:{current_time.minute:02d}:{current_time.second:02d}"

                        self.stop_times.append({
                            "trip_id": trip_id,
                            "arrival_time": time_str,
                            "departure_time": time_str,
                            "stop_id": stop["stop_id"],
                            "stop_sequence": stop_sequence_num,
                        })

        print(f"Generated {len(self.trips)} trips")
        print(f"Generated {len(self.stop_times)} stop times")

    # ==================== GTFS GENERATION ====================

    def generate_gtfs_files(self):
        """Generate all GTFS text files"""
        print("\n" + "="*60)
        print("PHASE 6: GENERATING GTFS FILES")
        print("="*60 + "\n")

        # Clean output directory
        if self.output_dir.exists():
            shutil.rmtree(self.output_dir)
        self.output_dir.mkdir()

        # Generate agency.txt
        self.generate_agency_txt()

        # Generate feed_info.txt
        self.generate_feed_info_txt()

        # Generate calendar.txt
        self.generate_calendar_txt()

        # Generate stops.txt
        self.generate_stops_txt()

        # Generate routes.txt
        self.generate_routes_txt()

        # Generate trips.txt
        self.generate_trips_txt()

        # Generate stop_times.txt
        self.generate_stop_times_txt()

        print("All GTFS files generated successfully!")

    def generate_agency_txt(self):
        """Generate agency.txt"""
        with open(self.output_dir / "agency.txt", "w") as f:
            f.write("agency_id,agency_name,agency_url,agency_timezone\n")
            f.write("JCTSL,Jaipur City Transport Services Limited,https://www.jctsl.com,Asia/Kolkata\n")

    def generate_feed_info_txt(self):
        """Generate feed_info.txt"""
        today = datetime.now().strftime("%Y%m%d")
        end = (datetime.now() + timedelta(days=180)).strftime("%Y%m%d")

        with open(self.output_dir / "feed_info.txt", "w") as f:
            f.write("feed_publisher_name,feed_publisher_url,feed_lang,feed_start_date,feed_end_date\n")
            f.write(f"Aayush Rai,https://github.com/croyla/,en,{today},{end}\n")

    def generate_calendar_txt(self):
        """Generate calendar.txt"""
        today = datetime.now().strftime("%Y%m%d")
        end = (datetime.now() + timedelta(days=180)).strftime("%Y%m%d")

        with open(self.output_dir / "calendar.txt", "w") as f:
            f.write("service_id,monday,tuesday,wednesday,thursday,friday,saturday,sunday,start_date,end_date\n")
            f.write(f"weekday,1,1,1,1,1,1,1,{today},{end}\n")

    def generate_stops_txt(self):
        """Generate stops.txt"""
        with open(self.output_dir / "stops.txt", "w") as f:
            f.write("stop_id,stop_code,stop_name,stop_lat,stop_lon\n")

            for stop_id, stop_info in self.unique_stops.items():
                f.write(f"{stop_info['stop_id']},{stop_info['stop_code']},\"{stop_info['stop_name']}\",{stop_info['stop_lat']},{stop_info['stop_lon']}\n")

    def generate_routes_txt(self):
        """Generate routes.txt"""
        with open(self.output_dir / "routes.txt", "w") as f:
            f.write("route_id,agency_id,route_short_name,route_long_name,route_type\n")

            for short_name, route_info in self.route_mappings.items():
                long_name = route_info["route_long_name"]
                # Route type 3 = Bus
                f.write(f"{short_name},JCTSL,{short_name},\"{long_name}\",3\n")

    def generate_trips_txt(self):
        """Generate trips.txt"""
        with open(self.output_dir / "trips.txt", "w") as f:
            f.write("route_id,service_id,trip_id,trip_headsign,direction_id\n")

            for trip in self.trips:
                f.write(f"{trip['route_id']},{trip['service_id']},{trip['trip_id']},\"{trip['trip_headsign']}\",{trip['direction_id']}\n")

    def generate_stop_times_txt(self):
        """Generate stop_times.txt"""
        with open(self.output_dir / "stop_times.txt", "w") as f:
            f.write("trip_id,arrival_time,departure_time,stop_id,stop_sequence\n")

            for stop_time in self.stop_times:
                f.write(f"{stop_time['trip_id']},{stop_time['arrival_time']},{stop_time['departure_time']},{stop_time['stop_id']},{stop_time['stop_sequence']}\n")

    def create_gtfs_zip(self):
        """Create ZIP archive of GTFS feed"""
        print("\n" + "="*60)
        print("PHASE 7: CREATING GTFS ZIP ARCHIVE")
        print("="*60 + "\n")

        zip_path = "jctsl_gtfs.zip"

        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for file in self.output_dir.glob("*.txt"):
                zipf.write(file, file.name)

        print(f"GTFS feed created: {zip_path}")
        return zip_path

    def generate_route_mapping_json(self):
        """Generate route long name -> short name + sequences JSON"""
        print("\n" + "="*60)
        print("GENERATING ROUTE MAPPING JSON")
        print("="*60 + "\n")

        output = {}

        for short_name, route_info in self.route_mappings.items():
            long_name = route_info["route_long_name"]

            output[long_name] = {
                "route_short_name": short_name,
                "stop_sequences": []
            }

            for seq_info in route_info["sequences"]:
                direction_id = seq_info["direction_id"]
                stops = seq_info["stops"]

                stop_list = [
                    {
                        "stop_id": stop["stop_id"],
                        "stop_name": stop["stop_name"],
                    }
                    for stop in stops
                ]

                output[long_name]["stop_sequences"].append({
                    "direction_id": direction_id,
                    "stops": stop_list,
                })

        # Save to file
        with open("route_mappings.json", "w") as f:
            json.dump(output, f, indent=2)

        print(f"Route mapping JSON saved: route_mappings.json")
        return output

    def run(self, skip_api: bool = False):
        """Run the complete GTFS generation pipeline

        Args:
            skip_api: If True, skip API calls and use existing data in raw/
        """
        try:
            # Phase 1: Data Collection or Loading
            if skip_api:
                self.load_existing_routes()
            else:
                self.collect_all_data()

            # Phase 2-4: Data Processing
            self.process_unique_stops()
            self.process_route_sequences()
            self.merge_bidirectional_routes()

            # Phase 5: Trip Generation
            self.generate_trips_and_times()

            # Phase 6-7: GTFS Output
            self.generate_gtfs_files()
            self.create_gtfs_zip()

            # Generate route mapping JSON
            self.generate_route_mapping_json()

            print("\n" + "="*60)
            print("GTFS GENERATION COMPLETE!")
            print("="*60)
            print(f"\nOutput files:")
            print(f"  - jctsl_gtfs.zip (GTFS feed)")
            print(f"  - route_mappings.json (Route mappings)")
            print(f"  - gtfs_output/ (Individual GTFS files)")
            print(f"  - raw/ (Raw API responses)")

        except Exception as e:
            print(f"\nError during GTFS generation: {e}")
            raise


def main():
    """Main entry point with argument parsing"""
    parser = argparse.ArgumentParser(
        description="Generate GTFS feed from JCTSL bus data",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Fetch data from API and generate GTFS
  python generate_gtfs.py

  # Skip API calls and process existing data in raw/
  python generate_gtfs.py --skip-api
        """
    )

    parser.add_argument(
        "--skip-api",
        action="store_true",
        help="Skip API calls and process existing data from raw/ directory"
    )

    args = parser.parse_args()

    generator = GTFSGenerator()
    generator.run(skip_api=args.skip_api)


if __name__ == "__main__":
    main()