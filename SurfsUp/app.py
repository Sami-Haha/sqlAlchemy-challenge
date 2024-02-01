# Import the dependencies.
import datetime as dt
import numpy as np
# Python SQL toolkit and Object Relational Mapper
import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func
# import flask
from flask import Flask, jsonify

#################################################
# Database Setup
#################################################

# Create engine using the `hawaii.sqlite` database file
engine = create_engine("sqlite:///Resources/hawaii.sqlite")
# Declare a Base using `automap_base()`
Base = automap_base()
# Use the Base class to reflect the database tables
Base.prepare(autoload_with=engine)

# Assign the measurement class to a variable called `Measurement` and
# the station class to a variable called `Station`
Measurement = Base.classes.measurement
Station = Base.classes.measurement

# Create a session
#session = Session(engine)

#################################################
# Flask Setup
#################################################
app = Flask(__name__)

#################################################
# Flask Routes
#################################################

@app.route("/")
def welcome():
    """Listing all available api routes."""
    return (
        f"Welcome to the surfsup challenge!<br/><br/>"
        f"These are the available api routes:<br/><br/>"
        f"<b>/api/v1.0/precipitation</b> - Returns JSON list of precipitation for the last year<br/><br/>"
        f"<b>/api/v1.0/stations</b> - Returns JSON list of the available weather stations<br/><br/>"
        f"<b>/api/v1.0/tobs</b> - Returns JSON list of temperature observations for the most active station<br/><br/>"
        f"<b>/api/v1.0/<start></b> - Returns JSON list of temperature statistics from the specifed start date onwards<br/>"
        f"Please provide the start date (in the format YYYY-MM-DD) in the URL when using the api above.<br/><br/>"
        f"<b>/api/v1.0/<start>/<end></b> -  Returns JSON list of temperature statistics for specified start and end dates inclusive. <br/>"
        f"Please provide the start and end date (in the format YYYY-MM-DD/YYYY-MM-DD) in the URL when using the api above."
    )

#Precipitation listing for last year
@app.route("/api/v1.0/precipitation")
def precip():
    # Create our session (link) from Python to the DB
    session = Session(engine)

    """Return a dictionary of date and precipitation"""
    # Calculate the date one year from the last date in data set.
    query_date = dt.date(2017, 8, 23) - dt.timedelta(days=365)
    # Get precipation data from query date
    results = session.query(Measurement.date, Measurement.prcp).\
        filter(Measurement.date >= query_date).all()
 
   #close session 
    session.close()

    # Convert results to a dictionary
    precipitation_dict = {}
    for date, prcp in results:
        precipitation_dict[date] = prcp

    return jsonify(precipitation_dict)

# Stations listing
@app.route("/api/v1.0/stations")
def station():
    # Create our session (link) from Python to the DB
    session = Session(engine)

    """Return a json list of stations from the dataset"""
    # Query for station listing
    unique_stations = session.query(Station.station).distinct().all()

   #close session 
    session.close()

    # Extract station names from the query results
    station_list = [station[0] for station in unique_stations]

    return jsonify(station_list)


# tobs listing
@app.route("/api/v1.0/tobs")
def temperature_observations():
    # Create our session (link) from Python to the DB
    session = Session(engine)

    """Dates and temperature observations for the most active station for the previous year."""
    # Calculate the date one year from the last date in data set.
    query_date = dt.date(2017, 8, 23) - dt.timedelta(days=365)
    # List the stations and their counts in descending order.
    list_active_stations = session.query(Station.station, func.count(Station.id).label('count')) \
                               .group_by(Station.station) \
                               .order_by(func.count(Station.id).desc()) \
                               .all()
    # Get the most active station id from the previous query
    most_active_station = list_active_stations[0][0]  
    # Query the last 12 months of temperature observation data for this station 
    results = session.query(Measurement.date, Measurement.tobs) \
        .filter(Measurement.station == most_active_station) \
            .filter(Measurement.date >= query_date) \
                 .all()
    #close session    
    session.close()

   # Create a list of dictionaries containing dates and temperature observations
    temperature_data = []
    for date, tobs in results:
        temperature_data.append({"date": date, "tobs": tobs})
    # Print the station name to console before jsonify return
    print(f"Most Active Station: {most_active_station}")    
    return jsonify(temperature_data)

# for a specified start date calculate TMIN, TAVG, TMAX for all dates >= start date
@app.route("/api/v1.0/<start_date>")
def temp_stats_start(start_date):
    # Create our session (link) from Python to the DB
    session = Session(engine)

     # Query the minimum, average, and maximum temperatures for the specified start date and onwards
    temp_stats = session.query(func.min(Measurement.tobs).label("min_temp"),
                               func.avg(Measurement.tobs).label("avg_temp"),
                               func.max(Measurement.tobs).label("max_temp")) \
                        .filter(Measurement.date >= start_date) \
                        .all()

   #close session 
    session.close()

    # Extracting values from the query result
    min_temp, avg_temp, max_temp = temp_stats[0]

    # Create a dictionary with the temperature statistics, renaming keys
    temp_stats_dict = {
        "TMIN": min_temp,
        "TAVG": avg_temp,
        "TMAX": max_temp
    }

    # Return the JSON response
    return jsonify(temp_stats_dict)

# for a specified start date and end datecalculate TMIN, TAVG, TMAX for all dates >= start date
@app.route("/api/v1.0/<start_date>/<end_date>")
def temp_stats_start_end(start_date, end_date):
    # Create our session (link) from Python to the DB
    session = Session(engine)

     # Query the minimum, average, and maximum temperatures for the specified start date and onwards
    temp_stats = session.query(func.min(Measurement.tobs).label("min_temp"),
                               func.avg(Measurement.tobs).label("avg_temp"),
                               func.max(Measurement.tobs).label("max_temp")) \
                        .filter(Measurement.date >= start_date) \
                        .filter(Measurement.date <= end_date) \
                        .all()

    #close session 
    session.close()

    # Extracting values from the query result
    min_temp, avg_temp, max_temp = temp_stats[0]

    # Create a dictionary with the temperature statistics, renaming keys
    temp_stats_dict = {
        "TMIN": min_temp,
        "TAVG": avg_temp,
        "TMAX": max_temp
    }

    # Return the JSON response
    return jsonify(temp_stats_dict)

#app run statement 
if __name__ == "__main__":
    app.run(debug=True, port=8000)