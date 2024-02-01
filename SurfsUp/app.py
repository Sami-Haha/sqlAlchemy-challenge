# Import the dependencies.
import datetime as dt
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

###################################################
#DRY - create functions for DRY principles
###################################################

#Session query open and closes session and returns a simple result
def make_query(query):
    session = Session(engine)
    results = query(session)
    session.close()
    return results

# Precipitation data query
def get_precipitation_data():
     # Calculate the date one year from the last date in data set.
    query_date = dt.date(2017, 8, 23) - dt.timedelta(days=365)
    # Get precipation data from query date using a function
    def query(session):
        return session.query(Measurement.date, Measurement.prcp).\
        filter(Measurement.date >= query_date).all()
    # complete the query using session engine function
    return make_query(query)
   
# Station Lists query
def get_station_list():
    # Get station info using a function
    def query(session):
        return session.query(Station.station).distinct().all()
    # complete the query using the session engine function
    return [station[0] for station in make_query(query) ]

#Create function to call temperature stats for start date, start and end date, station.
def get_temp_stats(session, start_date=None, end_date=None, station=None):
    # define the common query
    def temp_stats_query(session):
        return session.query(func.min(Measurement.tobs).label("min_temp"),
                             func.avg(Measurement.tobs).label("avg_temp"),
                             func.max(Measurement.tobs).label("max_temp"))
    # Add filters based on parameters provided
    temp_stats_filtered_query = temp_stats_query(session) 
    if start_date and end_date:
        # query for ths date range
        temp_stats_filtered_query = temp_stats_filtered_query.filter(Measurement.date >= start_date) \
            .filter(Measurement.date <= end_date)
    elif start_date:
        # Query temperature statistics for the specified start date onwards
        temp_stats_filtered_query = temp_stats_filtered_query.filter(Measurement.date >= start_date)
    elif station:
        # Query temperature statistics for the specified station
        temp_stats_filtered_query = temp_stats_filtered_query.filter(Measurement.station == station)
        
    #  Execute query and retreive the results
    temp_stats = temp_stats_filtered_query.all()
    #Extracting values from the query result
    min_temp, avg_temp, max_temp = temp_stats[0]

    # Return temperature statistics as a dictionary
    return {
        "TMIN": min_temp,
        "TAVG": avg_temp,
        "TMAX": max_temp
    }
    

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
    results = get_precipitation_data()
    precipitation_dict = {date: prcp for date, prcp in results}

    return jsonify(precipitation_dict)

# Stations listing
@app.route("/api/v1.0/stations")
def station():
    station_list = get_station_list()

    return jsonify(station_list)


# tobs listing
@app.route("/api/v1.0/tobs")
def temperature_observations():
    # create a session
    session = Session(engine)
    #query to get the most active station from part 1
    list_active_stations = session.query(Station.station, func.count(Station.id).label('count')) \
                               .group_by(Station.station) \
                               .order_by(func.count(Station.id).desc()) \
                               .all()
    most_active_station = list_active_stations[0][0]
    #get temperature data
    temperature_data = get_temp_stats(session, station=most_active_station)
    # close the session
    session.close()
    # Return the JSON response  
    return jsonify(temperature_data)

# for a specified start date calculate TMIN, TAVG, TMAX for all dates >= start date
@app.route("/api/v1.0/<start_date>")
def temp_stats_start(start_date):
    # create a session
    session = Session(engine)
    #Get temperature stats using temp stats function
    temp_stats = get_temp_stats(session, start_date=start_date)
    # close the session
    session.close()
    # Return the JSON response
    return jsonify(temp_stats)

# for a specified start date and end datecalculate TMIN, TAVG, TMAX for all dates >= start date
@app.route("/api/v1.0/<start_date>/<end_date>")
def temp_stats_range(start_date, end_date):
    # create a session
    session = Session(engine)
    # Get temperature statistics for the specified date range using temp stats function
    temp_range = get_temp_stats(session, start_date=start_date, end_date=end_date)
    # close the session
    session.close()
    # Return the JSON response
    return jsonify(temp_range)

#app run statement 
if __name__ == "__main__":
    app.run(debug=True, port=8000)