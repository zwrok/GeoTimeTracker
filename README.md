# GeoTimeTracker

**Project Description:**


GeoTimeTracker is a tool that automates the analysis of location history based on GPS data, integrating these data with calendar events to generate a detailed work hours report. This project merges location data (from Google Timeline, downloaded as JSON) and events from .ics calendar files, allowing users to see the time spent in specific locations alongside daily events and total hours worked.

**Features:**

JSON to CSV Location Data Conversion: Converts raw JSON location history from Google into a CSV format that’s easier to process.
Location Cluster Detection: Analyzes location data to identify periods when a user stayed within a defined radius for a specified time, recording entry and exit times.
Calendar Integration: Imports events from a .ics file and includes them in the report, linking specific events with times spent in designated locations.
Report Generation: Creates a daily CSV report detailing entry and exit times, total work hours, and any calendar events for each day.

**Files:** 

**Records.json:** JSON file containing the user’s location history, downloaded from Google Takeout.

**calendar_events.ics** :Calendar .ics file with events to be included in the report.

The final work hours report is generated in CSV format : work_hours_<year>_<month>.csv: 

I was working on calendar data exported from MacOS callendar in the .ics format. 

**Requirements:**

Python 3.x
Pandas 
Geopy 
Ics



**Use Cases:**

Work Hours Tracking: Useful for tracking time spent in specific work locations.
Event Reporting: Combines location data with calendar events, making it easier to document time at work and meeting attendance.
GeoTimeTracker is an ideal tool for anyone looking to analyze their location and calendar data in detail, to better manage and document time.
