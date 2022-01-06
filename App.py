from flask import Flask, request, render_template,redirect,url_for
from flask_cors import cross_origin
import sklearn
import pickle
from Logger import Logger
import pandas as pd
import csv
from Logger import Logger
from Database import Connector
app = Flask(__name__)
model = pickle.load(open("flight.pkl","rb"))
logging = Logger('logFile.log')

@app.route("/",methods = ["GET","POST"])
def predict():
    if request.method == 'POST':
        try:
     # Date_of_Journey
            date_dep = request.form["Dep_Time"]
            Day_of_travel = int(pd.to_datetime(date_dep, format="%Y-%m-%dT%H:%M").day)
            Month_of_travel = int(pd.to_datetime(date_dep, format ="%Y-%m-%dT%H:%M").month)
     # Departure
            Dep_hour = int(pd.to_datetime(date_dep, format ="%Y-%m-%dT%H:%M").hour)
            Dep_min = int(pd.to_datetime(date_dep, format ="%Y-%m-%dT%H:%M").minute)
     # Total Stops
            Total_stops = int(request.form["stops"])
     # Airline
            airline=request.form['airline']
            if(airline=='Jet Airways'):
                Jet_Airways = 1
                IndiGo = 0
                Air_India = 0
                Multiple_carriers = 0
                SpiceJet = 0
                Vistara = 0
                GoAir = 0
                Multiple_carriers_Premium_economy = 0
                Jet_Airways_Business = 0
                Vistara_Premium_economy = 0
                Trujet = 0
                Air_Asia = 0

            elif (airline=='Air Asia'):
                Jet_Airways = 0
                IndiGo = 0
                Air_India = 0
                Multiple_carriers = 0
                SpiceJet = 0
                Vistara = 0
                GoAir = 0
                Multiple_carriers_Premium_economy = 0
                Jet_Airways_Business = 0
                Vistara_Premium_economy = 0
                Trujet = 0
                Air_Asia = 1

            elif (airline=='IndiGo'):
                Jet_Airways = 0
                IndiGo = 1
                Air_India = 0
                Multiple_carriers = 0
                SpiceJet = 0
                Vistara = 0
                GoAir = 0
                Multiple_carriers_Premium_economy = 0
                Jet_Airways_Business = 0
                Vistara_Premium_economy = 0
                Trujet = 0
                Air_Asia = 0


            elif (airline=='Air India'):
                Jet_Airways = 0
                IndiGo = 0
                Air_India = 1
                Multiple_carriers = 0
                SpiceJet = 0
                Vistara = 0
                GoAir = 0
                Multiple_carriers_Premium_economy = 0
                Jet_Airways_Business = 0
                Vistara_Premium_economy = 0
                Trujet = 0
                Air_Asia = 0

            elif (airline=='Multiple carriers'):
                Jet_Airways = 0
                IndiGo = 0
                Air_India = 0
                Multiple_carriers = 1
                SpiceJet = 0
                Vistara = 0
                GoAir = 0
                Multiple_carriers_Premium_economy = 0
                Jet_Airways_Business = 0
                Vistara_Premium_economy = 0
                Trujet = 0

            elif (airline=='SpiceJet'):
                Jet_Airways = 0
                IndiGo = 0
                Air_India = 0
                Multiple_carriers = 0
                SpiceJet = 1
                Vistara = 0
                GoAir = 0
                Multiple_carriers_Premium_economy = 0
                Jet_Airways_Business = 0
                Vistara_Premium_economy = 0
                Trujet = 0
                Air_Asia = 0

            elif (airline=='Vistara'):
                Jet_Airways = 0
                IndiGo = 0
                Air_India = 0
                Multiple_carriers = 0
                SpiceJet = 0
                Vistara = 1
                GoAir = 0
                Multiple_carriers_Premium_economy = 0
                Jet_Airways_Business = 0
                Vistara_Premium_economy = 0
                Trujet = 0
                Air_Asia = 0

            elif (airline=='GoAir'):
                Jet_Airways = 0
                IndiGo = 0
                Air_India = 0
                Multiple_carriers = 0
                SpiceJet = 0
                Vistara = 0
                GoAir = 1
                Multiple_carriers_Premium_economy = 0
                Jet_Airways_Business = 0
                Vistara_Premium_economy = 0
                Trujet = 0
                Air_Asia = 0

            elif (airline=='Multiple carriers Premium economy'):
                Jet_Airways = 0
                IndiGo = 0
                Air_India = 0
                Multiple_carriers = 0
                SpiceJet = 0
                Vistara = 0
                GoAir = 0
                Multiple_carriers_Premium_economy = 1
                Jet_Airways_Business = 0
                Vistara_Premium_economy = 0
                Trujet = 0
                Air_Asia = 0

            elif (airline=='Jet Airways Business'):
                Jet_Airways = 0
                IndiGo = 0
                Air_India = 0
                Multiple_carriers = 0
                SpiceJet = 0
                Vistara = 0
                GoAir = 0
                Multiple_carriers_Premium_economy = 0
                Jet_Airways_Business = 1
                Vistara_Premium_economy = 0
                Trujet = 0
                Air_Asia = 0

            elif (airline=='Vistara Premium economy'):
                Jet_Airways = 0
                IndiGo = 0
                Air_India = 0
                Multiple_carriers = 0
                SpiceJet = 0
                Vistara = 0
                GoAir = 0
                Multiple_carriers_Premium_economy = 0
                Jet_Airways_Business = 0
                Vistara_Premium_economy = 1
                Trujet = 0
                Air_Asia = 0

            elif (airline=='Trujet'):
                Jet_Airways = 0
                IndiGo = 0
                Air_India = 0
                Multiple_carriers = 0
                SpiceJet = 0
                Vistara = 0
                GoAir  = 0
                Multiple_carriers_Premium_economy = 0
                Jet_Airways_Business = 0
                Vistara_Premium_economy = 0
                Trujet = 1
                Air_Asia = 0


     # Source

            Source = request.form["Source"]
            if (Source == 'Delhi'):
                Source_Delhi = 1
                Source_Kolkata = 0
                Source_Mumbai = 0
                Source_Chennai = 0

            elif (Source == 'Kolkata'):
                Source_Delhi = 0
                Source_Kolkata = 1
                Source_Mumbai = 0
                Source_Chennai = 0

            elif (Source == 'Mumbai'):
                Source_Delhi = 0
                Source_Kolkata = 0
                Source_Mumbai = 1
                Source_Chennai = 0

            elif (Source == 'Chennai'):
                Source_Delhi = 0
                Source_Kolkata = 0
                Source_Mumbai = 0
                Source_Chennai = 1

            else:
                Source_Delhi = 0
                Source_Kolkata = 0
                Source_Mumbai = 0
                Source_Chennai = 0

     # Destination
     # Banglore = 0 (not in column)
            Source2 = request.form["Destination"]
            if (Source2 == 'Cochin'):
                Destination_Cochin = 1
                Destination_Delhi = 0
                Destination_New_Delhi = 0
                Destination_Hyderabad = 0
                Destination_Kolkata = 0

            elif (Source2 == 'Delhi'):
                Destination_Cochin = 0
                Destination_Delhi = 1
                Destination_New_Delhi = 0
                Destination_Hyderabad = 0
                Destination_Kolkata = 0

            elif (Source2 == 'New_Delhi'):
                Destination_Cochin = 0
                Destination_Delhi = 0
                Destination_New_Delhi = 1
                Destination_Hyderabad = 0
                Destination_Kolkata = 0

            elif (Source2 == 'Hyderabad'):
                Destination_Cochin = 0
                Destination_Delhi = 0
                Destination_New_Delhi = 0
                Destination_Hyderabad = 1
                Destination_Kolkata = 0

            elif (Source2 == 'Kolkata'):
                Destination_Cochin = 0
                Destination_Delhi = 0
                Destination_New_Delhi = 0
                Destination_Hyderabad = 0
                Destination_Kolkata = 1

            else:
                Destination_Cochin = 0
                Destination_Delhi = 0
                Destination_New_Delhi = 0
                Destination_Hyderabad = 0
                Destination_Kolkata = 0

            prediction=model.predict([[
                Total_stops,
                Day_of_travel,
                Month_of_travel,
                Dep_hour,
                Dep_min,
                Air_India,
                Air_Asia,
                GoAir,
                IndiGo,
                Jet_Airways,
                Jet_Airways_Business,
                Multiple_carriers,
                Multiple_carriers_Premium_economy,
                SpiceJet,
                Trujet,
                Vistara,
                Vistara_Premium_economy,
                Source_Chennai,
                Source_Delhi,
                Source_Kolkata,
                Source_Mumbai,
                Destination_Cochin,
                Destination_Delhi,
                Destination_Hyderabad,
                Destination_Kolkata,
                Destination_New_Delhi
            ]])
            output = round(prediction[0],2)
            if Source == Source2:
                return render_template('home.html',Flight_Price="The Flight price prediction is 0 Because Source and Destination are same")
            else:
                return render_template('home.html',Flight_Price="The Flight prediction price is Rs. {} Departure date {} source {} destination {}".format(output,date_dep,Source,Source2))
        except Exception as a:
            print('Operation not successful'+str(a))
    return render_template("home.html")

@app.route("/DatabasePage", methods = ['GET','POST'])
def test():
    heading = ("id", "Airline", "Source","Destination"," Total_Stops","Departure")
    data = Connector()
    return render_template('database.html', heading=heading, data=data.getData())



if __name__ == "__main__":
    app.run(debug = True, port = 5003)
