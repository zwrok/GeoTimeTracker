
import json
import csv
import pandas as pd
import os
from datetime import timedelta, datetime
from geopy.distance import great_circle
from ics import Calendar
from collections import defaultdict

# Default settings - file paths based on script's current location
script_location = os.path.dirname(os.path.abspath(__file__))
DEFAULT_JSON_FILE = os.path.join(script_location, 'Records.json')
DEFAULT_INPUT_FILE = os.path.join(script_location, 'outputs.csv')
DEFAULT_ICS_FILE = os.path.join(script_location, 'calendar_events.ics')
DEFAULT_CENTER_COORDINATES = (-90.0000, 0.0000)  # Arbitrary location near the South Pole
DEFAULT_RADIUS_METERS = 300  # Default radius in meters
DEFAULT_TIME_OFFSET_HOURS = 2  # Default time offset in hours
DEFAULT_MIN_CLUSTER_TIME_MINUTES = 0.5  # Minimum time (in minutes) for a cluster to be valid

# Default year and month (current month and year)
CURRENT_DATE = datetime.now()
DEFAULT_YEAR = CURRENT_DATE.year
DEFAULT_MONTH = CURRENT_DATE.month

# JSON to CSV conversion
def convert_json_to_csv(json_file, csv_file):
    def has_keys(dictionary, keys):
        return all(key in dictionary for key in keys)

    def make_reader(in_json):
        # Opening location history data
        with open(in_json, 'r') as file:
            json_data = json.load(file)
            
        # Keys to check in the data
        keys_to_check = ['timestamp', 'longitudeE7', 'latitudeE7', 'accuracy']
        
        # Extract fields from JSON data
        for item in json_data['locations']:
            if has_keys(item, keys_to_check):
                timestamp = item['timestamp']
                if '.' in timestamp:
                    date = datetime.strptime(timestamp, '%Y-%m-%dT%H:%M:%S.%fZ').date()
                else:
                    date = datetime.strptime(timestamp, '%Y-%m-%dT%H:%M:%SZ').date()
                tm = timestamp.split('T')[1].split('Z')[0]
                longitude = item['longitudeE7'] / 10000000.0
                latitude = item['latitudeE7'] / 10000000.0
                accuracy = item['accuracy']
                
                yield [date, tm, longitude, latitude, accuracy]

    features = [['Date', 'Time', 'Longitude', 'Latitude', 'Accuracy']]  # Headers
    print(f"Reading JSON file {json_file}")

    reader = make_reader(json_file)

    # Append data to list
    for r in reader:
        features.append(r)

    # Write to CSV file
    with open(csv_file, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerows(features)
    
    print(f"Conversion complete. Data saved to {csv_file}")

# Check if a point is within the radius
def is_within_radius(lat, lon, center_lat, center_lon, radius_meters):
    return great_circle((lat, lon), (center_lat, center_lon)).meters <= radius_meters

# Round time to the nearest 5 minutes
def round_to_nearest_5_minutes(dt):
    discard = timedelta(minutes=dt.minute % 5, seconds=dt.second, microseconds=dt.microsecond)
    dt -= discard
    if discard >= timedelta(minutes=2.5):
        dt += timedelta(minutes=5)
    return dt

# Detect clusters of GPS points within the radius
def detect_clusters(df, center_lat, center_lon, radius_meters, min_cluster_time_minutes=10):
    clusters = []
    current_cluster = []
    last_in_radius_time = None

    for _, row in df.iterrows():
        if is_within_radius(row['Latitude'], row['Longitude'], center_lat, center_lon, radius_meters):
            if not current_cluster:
                current_cluster.append(row)
            elif (row['DateTime'] - current_cluster[-1]['DateTime']).seconds > 60:
                current_cluster.append(row)
            last_in_radius_time = row['DateTime']
        else:
            if current_cluster and last_in_radius_time:
                duration = (last_in_radius_time - current_cluster[0]['DateTime']).total_seconds() / 60.0
                if duration >= min_cluster_time_minutes:
                    clusters.append((current_cluster[0]['DateTime'], last_in_radius_time))
                current_cluster = []
    return clusters

# Filter GPS data by year and month
def filter_csv(df, year, month):
    if not pd.api.types.is_datetime64_any_dtype(df['Date']):
        df['Date'] = pd.to_datetime(df['Date'])
    return df[(df['Date'].dt.year == year) & (df['Date'].dt.month == month)]

# Load and filter events from .ics file
def load_events_from_ics(ics_file, month, year):
    with open(ics_file, 'r', encoding='utf-8') as f:
        calendar = Calendar(f.read())

    events_by_day = defaultdict(list)

    for event in calendar.events:
        event_start = event.begin.datetime
        if event_start.month == month and event_start.year == year and '*' in event.name:
            event_description = f"{event.name} ({event_start.strftime('%H:%M')} - {event.end.strftime('%H:%M') if event.end else ''})"
            events_by_day[event_start.date()].append(event_description)
    
    return events_by_day

# Function to apply time offset to datetime objects
def apply_time_offset(time, offset_hours):
    return time + timedelta(hours=offset_hours)

# Main function to analyze location data and integrate with calendar events
def analyze_location_and_calendar(input_file, ics_file, output_csv, center_lat, center_lon, radius_meters, filter_year, filter_month, time_offset_hours, min_cluster_time_minutes=10):
    # Check if the input files exist
    if not os.path.isfile(input_file) or not os.path.isfile(ics_file):
        print(f"Error: One or both files do not exist in the script directory.")
        return

    # Load GPS data
    df = pd.read_csv(input_file)

    # Combine 'Date' and 'Time' columns into a single 'DateTime' column
    try:
        df['DateTime'] = pd.to_datetime(df['Date'] + ' ' + df['Time'], errors='coerce')
    except Exception as e:
        print(f"Error processing DateTime: {e}")
        return

    # Drop rows with invalid DateTime
    df = df.dropna(subset=['DateTime'])

    # Ensure latitude and longitude are floats
    df.loc[:, 'Latitude'] = pd.to_numeric(df['Latitude'], errors='coerce')
    df.loc[:, 'Longitude'] = pd.to_numeric(df['Longitude'], errors='coerce')

    # Drop rows with missing or invalid latitude/longitude
    df = df.dropna(subset=['Latitude', 'Longitude'])

    # Apply the year and month filter to the GPS data
    df = filter_csv(df, filter_year, filter_month)

    # Detect clusters (entry/exit times)
    clusters = detect_clusters(df, center_lat, center_lon, radius_meters, min_cluster_time_minutes)

    # Load events from the calendar
    events_by_day = load_events_from_ics(ics_file, filter_month, filter_year)

    # Prepare the final entry-exit pairs per day, integrating with calendar events
    entry_exit_by_day = {}
    for entry_time, exit_time in clusters:
        entry_time = round_to_nearest_5_minutes(entry_time)
        exit_time = round_to_nearest_5_minutes(exit_time)
        day = entry_time.date()

        if day not in entry_exit_by_day:
            entry_exit_by_day[day] = {'entry': entry_time, 'exit': exit_time}
        else:
            entry_exit_by_day[day]['exit'] = exit_time  # Update exit time if it's later in the day

    # Prepare data for each day and integrate events
    entry_exit_pairs = []
    for day, times in sorted(entry_exit_by_day.items()):
        entry_time = apply_time_offset(times['entry'], time_offset_hours)
        exit_time = apply_time_offset(times['exit'], time_offset_hours)

        duration = (exit_time - entry_time).total_seconds() / 60.0
        hours = int(duration // 60)
        minutes = int(duration % 60)
        time_worked = f"{hours:02}:{minutes:02}:00"

        # Format date to DD.MM.YYYY
        formatted_day = day.strftime('%d.%m.%Y')

        # Get events for this day (if any)
        events = "\n".join(events_by_day.get(day, []))

        entry_exit_pairs.append({
            'Date': formatted_day,
            'Event': events,
            'Work Duration': time_worked,
            'From': entry_time.strftime('%H:%M:%S'),
            'To': exit_time.strftime('%H:%M:%S')
        })

    # Create DataFrame
    df_entries = pd.DataFrame(entry_exit_pairs)

    # Calculate total work time
    total_duration_minutes = sum(
        [(datetime.strptime(row['To'], '%H:%M:%S') - datetime.strptime(row['From'], '%H:%M:%S')).total_seconds() / 60.0 
         for _, row in df_entries.iterrows() if row['From'] and row['To']]
    )
    total_hours = int(total_duration_minutes // 60)
    total_minutes = int(total_duration_minutes % 60)
    total_time_str = f"{total_hours:02}:{total_minutes:02}:00"

    # Add summary row
    summary_row = pd.DataFrame({
        'Date': ['Total'],
        'Event': [''],
        'Work Duration': [total_time_str],
        'From': [''],
        'To': ['']
    })
    work_hours_df = pd.concat([df_entries, summary_row], ignore_index=True)

    # Save to CSV
    work_hours_df.to_csv(output_csv, index=False)
    print(f"Work hours and events saved to {output_csv}")

if __name__ == "__main__":
    # First, convert JSON to CSV
    convert_json_to_csv(DEFAULT_JSON_FILE, DEFAULT_INPUT_FILE)
    
    # Then, analyze work hours and calendar events
    output_csv = os.path.join(script_location, f"work_hours_{DEFAULT_YEAR}_{DEFAULT_MONTH}.csv")
    analyze_location_and_calendar(
        DEFAULT_INPUT_FILE, 
        DEFAULT_ICS_FILE, 
        output_csv, 
        DEFAULT_CENTER_COORDINATES[0], 
        DEFAULT_CENTER_COORDINATES[1], 
        DEFAULT_RADIUS_METERS, 
        DEFAULT_YEAR, 
        DEFAULT_MONTH, 
        DEFAULT_TIME_OFFSET_HOURS, 
        DEFAULT_MIN_CLUSTER_TIME_MINUTES
    )
