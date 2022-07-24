from ast import arg
from asyncio.windows_events import NULL
from wsgiref.util import request_uri
from flask import Flask
from flask_restful import reqparse, abort, Api, Resource
from flask_caching import Cache
import sim
import time 
import threading
import psycopg2
import os
from beebotte import *
import time
import paho.mqtt.client as mqtt
from flask.views import MethodView

app = Flask(__name__)
api = Api(app)

# read database connection url from the environment variable 
DATABASE_URL = os.environ.get('DATABASE_URL')
con = None

# Instantiate the cache
cache = Cache()
cache.init_app(app=app, config={"CACHE_TYPE": "filesystem",'CACHE_DIR': './tmp'})

# global configuration variables
clientID=-1
initialRate=1 

# Do query
def query(*args):
    try:
        # create a new database connection by calling the connect() function
        con = psycopg2.connect(DATABASE_URL)
        # create a new cursor
        cur = con.cursor()
        if(len(args)==1):
            # execute the statement
            cur.execute(args[0])
            # commit the changes to the database
            con.commit()
        

        if(len(args)==2):
            # execute the statement
            cur.execute(args[0], args[1])
            # commit the changes to the database
            con.commit()
        
        
        # close communication with the database
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if con is not None:
            con.close()


def on_connect(client, data, flags, rc):
    client.subscribe("ISCF/accel_x", 1)
    client.subscribe("ISCF/accel_y", 1)
    client.subscribe("ISCF/accel_z", 1)

bclient = BBT("3zuKenn5p5pQkhnIWYTPobrZ", "uAw2wbUS5tJP3uSGJmIV8T9znhdYaB3Z") # bclient = BBT("API_KEY", "SECRET_KEY")
client = mqtt.Client()
client.username_pw_set("uAw2wbUS5tJP3uSGJmIV8T9znhdYaB3Z") # Secret Key is in https://beebotte.com/account#credentials
client.on_connect = on_connect
client.connect("mqtt.beebotte.com", 1883, 60)

client.loop_start()

# Helper function provided by the teaching staff
def get_data_from_simulation(id):
    """Connects to the simulation and gets a float signal value

    Parameters
    ----------
    id : str
        The signal id in CoppeliaSim. Possible values are 'accelX', 'accelY' and 'accelZ'.

    Returns
    -------
    data : float
        The float value retrieved from the simulation. None if retrieval fails.
    """
    if clientID!=-1:        
        res, data = sim.simxGetFloatSignal(clientID, id, sim.simx_opmode_blocking)
        if res==sim.simx_return_ok:            
            return data        
    return None



# TODO LAB 1 - Implement the data collection loop in a thread
class DataCollection(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        
        # TODO LAB1 - initialize the "current_rate" value in the cache if there's no value      
        if cache.get('rate') == None :
            cache.set('rate', initialRate)
        
        client.publish("ISCF/rate",str(cache.get('rate')),1)

        #publishNode('rate',str(cache.get('rate')))

        # TODO LAB2 - initialize db connection    
        createAccelTable = """
                CREATE TABLE IF NOT EXISTS accel (
                    id SERIAL PRIMARY KEY,
                    x FLOAT NOT NULL,
                    y FLOAT NOT NULL,
                    z FLOAT NOT NULL,
                    ts TIMESTAMP DEFAULT NOW()
                )
            """

        query(createAccelTable)

        self.daemon = True
        self.start()
        return None
        

    def run(self):

        while True:
            # TODO LAB 1 - Get acceleration data values (x, y and z) from the simulation in a cycle and print them to the console
            x = get_data_from_simulation('accelX')
            y = get_data_from_simulation('accelY')
            z = get_data_from_simulation('accelZ') 
           
            if x==None:
                print("Please start the simulation")
            else:
                print( "Aceleration (x,y,z): (" + str(x) + "," + str(y) + "," + str(z) + ")" )
                client.publish("ISCF/accel_x",str(x),1)
                client.publish("ISCF/accel_y",str(y),1)
                client.publish("ISCF/accel_z",str(z),1)
                
                bclient.write('ISCF', 'beebotte_accel_x',float(x)) # isto aqui serve apenas ficar na base de dados do beebotte
                bclient.write('ISCF', 'beebotte_accel_y',float(y))
                bclient.write('ISCF', 'beebotte_accel_z',float(z))

                # TODO LAB 2 - Insert the data into the PostgreSQL database on Heroku
                query("""INSERT INTO accel (x,y,z) VALUES (%s,%s,%s)""", (x,y,z))              
            
            time.sleep(cache.get('rate')) 
        return None



# TODO LAB 1 - Implement the UpdateRate resource
class UpdateRateAPI(MethodView):
    def __init__(self):
        self.reqparse = reqparse.RequestParser()
        self.reqparse.add_argument('rate', type=int, required=True,
                                   help='No rate time provided.')
        #super(UpdateRateAPI, self).__init__()

    def get(self):
        return cache.get('rate')

    def put(self):    
        args = self.reqparse.parse_args()     
        cache.set('rate', args['rate'])   
        client.publish("ISCF/rate",str(cache.get('rate')),1)
        return cache.get('rate')


# TODO LAB 1 - Define the API resource routing
api.add_resource(UpdateRateAPI, '/currentRate')

if __name__ == '__main__':
    sim.simxFinish(-1) # just in case, close all opened connections
    clientID=sim.simxStart('127.0.0.1',19997,True,True,5000,5) # Connect to CoppeliaSim 
    if clientID!=-1:
        # TODO LAB 1 - Start the data collection as a daemon thread
        DataCollection()
        app.run(debug=True, threaded=True)   
    else:
        exit()
    
