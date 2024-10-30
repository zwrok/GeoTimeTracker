import json
import csv
import pandas as pd
import os
from datetime import timedelta, datetime
from geopy.distance import great_circle
from ics import Calendar
from collections import defaultdict

# Ustawienia domyślne - ścieżki do plików bazujące na lokalizacji skryptu
script_location = os.path.dirname(os.path.abspath(__file__))
DEFAULT_JSON_FILE = os.path.join(script_location, 'Records.json')
DEFAULT_INPUT_FILE = os.path.join(script_location, 'outputs.csv')
DEFAULT_ICS_FILE = os.path.join(script_location, 'sckmi.ics')
DEFAULT_CENTER_COORDINATES = (50.04615637899176, 19.941147883134725)  # Default coordinates
DEFAULT_RADIUS_METERS = 300  # Default radius in meters
DEFAULT_TIME_OFFSET_HOURS = 2  # Default time offset in hours
DEFAULT_MIN_CLUSTER_TIME_MINUTES = 0.5  # Minimum time (in minutes) for a cluster to be valid

# Default year and month (current month and year)
CURRENT_DATE = datetime.now()
DEFAULT_YEAR = CURRENT_DATE.year
DEFAULT_MONTH = CURRENT_DATE.month

# Konwersja JSON do CSV
def convert_json_to_csv(json_file, csv_file):
    def has_keys(dictionary, keys):
        return all(key in dictionary for key in keys)

    def make_reader(in_json):
        # Otwieranie danych historii lokalizacji
        with open(in_json, 'r') as file:
            json_data = json.load(file)
            
        # Klucze do sprawdzenia w danych
        keys_to_check = ['timestamp', 'longitudeE7', 'latitudeE7', 'accuracy']
        
        # Pobieranie pól z danych JSON
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

    features = [['Date', 'Time', 'Longitude', 'Latitude', 'Accuracy']]  # Nagłówki
    print(f"Czytanie pliku JSON {json_file}")

    reader = make_reader(json_file)

    # Dodanie danych do listy
    for r in reader:
        features.append(r)

    # Zapis do pliku CSV
    with open(csv_file, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerows(features)
    
    print(f"Konwersja zakończona. Dane zapisane do {csv_file}")

# Sprawdzanie, czy punkt znajduje się w promieniu
def is_within_radius(lat, lon, center_lat, center_lon, radius_meters):
    return great_circle((lat, lon), (center_lat, center_lon)).meters <= radius_meters

# Zaokrąglanie czasu do najbliższych 5 minut
def round_to_nearest_5_minutes(dt):
    discard = timedelta(minutes=dt.minute % 5, seconds=dt.second, microseconds=dt.microsecond)
    dt -= discard
    if discard >= timedelta(minutes=2.5):
        dt += timedelta(minutes=5)
    return dt

# Wykrywanie klastrów punktów GPS w obrębie promienia
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

# Filtrowanie danych GPS według roku i miesiąca
def filter_csv(df, year, month):
    if not pd.api.types.is_datetime64_any_dtype(df['Date']):
        df['Date'] = pd.to_datetime(df['Date'])
    return df[(df['Date'].dt.year == year) & (df['Date'].dt.month == month)]

# Ładowanie i filtrowanie wydarzeń z pliku .ics
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

# Funkcja do dodawania przesunięcia czasowego
def apply_time_offset(time, offset_hours):
    return time + timedelta(hours=offset_hours)

# Główna funkcja analizująca dane lokalizacji i integrująca z wydarzeniami kalendarza
def analyze_location_and_calendar(input_file, ics_file, output_csv, center_lat, center_lon, radius_meters, filter_year, filter_month, time_offset_hours, min_cluster_time_minutes=10):
    # Sprawdzenie, czy pliki wejściowe istnieją
    if not os.path.isfile(input_file) or not os.path.isfile(ics_file):
        print(f"Error: One or both files do not exist in the script directory.")
        return

    # Wczytanie danych GPS
    df = pd.read_csv(input_file)

    # Kombinacja kolumn 'Date' i 'Time' w pojedynczą kolumnę 'DateTime'
    try:
        df['DateTime'] = pd.to_datetime(df['Date'] + ' ' + df['Time'], errors='coerce')
    except Exception as e:
        print(f"Error processing DateTime: {e}")
        return

    # Usunięcie wierszy z niepoprawną kolumną DateTime
    df = df.dropna(subset=['DateTime'])

    # Zapewnienie, że latitude i longitude są typu float
    df.loc[:, 'Latitude'] = pd.to_numeric(df['Latitude'], errors='coerce')
    df.loc[:, 'Longitude'] = pd.to_numeric(df['Longitude'], errors='coerce')

    # Usunięcie wierszy z brakującymi lub nieprawidłowymi danymi geograficznymi
    df = df.dropna(subset=['Latitude', 'Longitude'])

    # Filtrowanie danych według roku i miesiąca
    df = filter_csv(df, filter_year, filter_month)

    # Wykrywanie klastrów (czasy wejścia i wyjścia)
    clusters = detect_clusters(df, center_lat, center_lon, radius_meters, min_cluster_time_minutes)

    # Ładowanie wydarzeń z kalendarza
    events_by_day = load_events_from_ics(ics_file, filter_month, filter_year)

    # Przygotowanie końcowych par wejścia-wyjścia dla każdego dnia, integrując z wydarzeniami
    entry_exit_by_day = {}
    for entry_time, exit_time in clusters:
        entry_time = round_to_nearest_5_minutes(entry_time)
        exit_time = round_to_nearest_5_minutes(exit_time)
        day = entry_time.date()

        if day not in entry_exit_by_day:
            entry_exit_by_day[day] = {'entry': entry_time, 'exit': exit_time}
        else:
            entry_exit_by_day[day]['exit'] = exit_time

    entry_exit_pairs = []
    for day, times in sorted(entry_exit_by_day.items()):
        entry_time = apply_time_offset(times['entry'], time_offset_hours)
        exit_time = apply_time_offset(times['exit'], time_offset_hours)

        duration = (exit_time - entry_time).total_seconds() / 60.0
        hours = int(duration // 60)
        minutes = int(duration % 60)
        time_worked = f"{hours:02}:{minutes:02}:00"

        formatted_day = day.strftime('%d.%m.%Y')
        events = "\n".join(events_by_day.get(day, []))

        entry_exit_pairs.append({
            'Data': formatted_day,
            'Zadanie': events,
            'Czas pracy': time_worked,
            'Od': entry_time.strftime('%H:%M:%S'),
            'Do': exit_time.strftime('%H:%M:%S')
        })

    df_entries = pd.DataFrame(entry_exit_pairs)
    total_duration_minutes = sum(
        [(datetime.strptime(row['Do'], '%H:%M:%S') - datetime.strptime(row['Od'], '%H:%M:%S')).total_seconds() / 60.0 
         for _, row in df_entries.iterrows() if row['Od'] and row['Do']]
    )
    total_hours = int(total_duration_minutes // 60)
    total_minutes = int(total_duration_minutes % 60)
    total_time_str = f"{total_hours:02}:{total_minutes:02}:00"

    summary_row = pd.DataFrame({
        'Data': ['Suma'],
        'Zadanie': [''],
        'Czas pracy': [total_time_str],
        'Od': [''],
        'Do': ['']
    })
    work_hours_df = pd.concat([df_entries, summary_row], ignore_index=True)

    work_hours_df.to_csv(output_csv, index=False)
    print(f"Godziny pracy i wydarzenia zapisane do {output_csv}")

if __name__ == "__main__":
    # Najpierw konwersja JSON do CSV
    convert_json_to_csv(DEFAULT_JSON_FILE, DEFAULT_INPUT_FILE)
    
    # Następnie analiza godzin pracy i kalendarza
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
