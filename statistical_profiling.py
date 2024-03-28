# -*- coding: utf-8 -*-
"""
Created on Mon Jan 29 11:55:17 2024

@author: bmkea
"""
import logging
import warnings
import configparser
import pandas as pd
import psycopg2 as pg
import psycopg2.extras as extras
import time

class statistical_profiling():
    
    def __init__(self):

        #Set basic logging parameters, info --> logfile, debug --> console
        #Set logging format
        log_format = logging.Formatter('%(asctime)s %(levelname)s (%(threadName)s): %(message)s', 
                                       datefmt='%m/%d/%Y %I:%M:%S %p')
        #Initiate logger object
        self.logger = logging.getLogger(__name__)
        
        #Create console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.DEBUG) ###########make sure to change back to INFO once debug is complete
        console_handler.setFormatter(log_format)
        
        #add console handler to logger
        self.logger.addHandler(console_handler)
        
        #create file handler
        file_handler = logging.FileHandler('logfile.log', mode='a')
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(log_format)
        
        #add file handler
        self.logger.addHandler(file_handler)
        
        #set default logger level
        self.logger.setLevel(logging.DEBUG)
        
        #Initial log message
        self.logger.debug('Initializing...')

        
        #Ignore Deprecation Wanring
        warnings.filterwarnings("ignore", category=DeprecationWarning) 
        
    def main(self):
        self.logger.debug('Please be patient...')
        
        #Read Parameters from Config file
        self.get_parameters()
        
        #Check for resetting baseline flag
        if self.reset_baseline == 'True':
            
            #Reset Baseline
            self.set_baseline()
            
            #Flip baseline reset flag
            self.get_parameters(write=True)
        
        #Check for anomalies
        anomalies = self.data_processing()
        
        #Store Anomalies in Postgres
        if len(anomalies) > 1:
            self.database_conn(query=5, input_dataframe=anomalies)       
        
    def get_parameters(self, write=False):
        
        #Create config object
        #Read from Config file
        config = configparser.ConfigParser()
        config.readfp(open(r'config.txt'))
        
        
        #Read parameters and create variables
        if write == False:
            self.logger.debug('Loading Parameters...')
            
            #Get parameters
            self.agg_interval = config.get('stats_config', 'agg_interval') + " " + "minutes"
            self.anomaly_threshold = config.get('stats_config', 'anomaly_threshold')
            self.agg_type = config.get('stats_config', 'agg_type')
            self.send_interval = (int(config.get('stats_config', 'agg_interval'))* 60)
            self.raw_db_address = config.get('stats_config', 'raw_db_address')
            self.raw_db_username = config.get('stats_config', 'raw_db_username')
            self.raw_db_password = config.get('stats_config', 'raw_db_password')
            self.raw_db_name = config.get('stats_config', 'raw_db_name')
            self.processed_db_address = config.get('stats_config', 'raw_db_address')
            self.processed_db_username = config.get('stats_config', 'raw_db_username')
            self.processed_db_password = config.get('stats_config', 'raw_db_password')
            self.processed_db_name = config.get('stats_config', 'raw_db_name')
            self.reset_baseline = config.get('stats_config', 'reset_baseline')
            self.baseline_start_time = config.get('stats_config', 'baseline_start_time')
            self.baseline_end_time = config.get('stats_config', 'baseline_end_time')
            self.sample_start_time = config.get('stats_config', 'sample_start_time')
            self.sample_end_time = config.get('stats_config', 'sample_end_time')
        
        #Write to parameters
        elif write == True:
            
            #Flip reset baseline flag
            stats_config = config["stats_config"]
            stats_config['reset_baseline'] = 'False'
            
            # Writing to config file
            with open(r'config.txt', 'w') as configfile:
                config.write(configfile)
            
        
        
        
        
    def set_baseline(self):
        self.logger.debug('Building baseline from historical data...')
        dataframe = self.database_conn(query=1)
        self.database_conn(query=2, input_dataframe=dataframe)
        return dataframe
        
        
    def database_conn(self, query=1, input_dataframe=False):
        ''' 
        This method handles all connections and queries used in the program. Database credentials are defined in config file.
        
        ''' 
        #Keep trying database connection until a god connection is made
        while(True):
            try:
                self.logger.debug("Connecting to postgres database...")
                
                #Decide which query will be called
                #Case in witch raw data is needed to reset baseline
                if query==1:
                
                    #Attempt Initial Connection to database where raw data is stored
                    conn = pg.connect(
                        host=str(self.raw_db_address),
                        database=self.raw_db_name,
                        user=self.raw_db_username,
                        password=self.raw_db_password)
                    
    
                    self.logger.debug('Retrieving Raw Data...')
                    #Open a cursor to perform database operations
                    cur = conn.cursor()
                    
                    #Build Dynamic Query
                    sql = """SELECT "Robot_Name", MIN(time_by_minute) as start_time, MAX(time_by_minute) as end_time,
                    		ROUND(AVG(max_amp_1), 5) as mean_of_max_amp_01, ROUND(STDDEV(max_amp_1), 5) as std_of_max_amp_01,
                    		ROUND(AVG(max_amp_2), 5) as mean_of_max_amp_02, ROUND(STDDEV(max_amp_2), 5) as std_of_max_amp_02,
                    		ROUND(AVG(max_amp_3), 5) as mean_of_max_amp_03, ROUND(STDDEV(max_amp_3), 5) as std_of_max_amp_03,
                    		ROUND(AVG(max_amp_4), 5) as mean_of_max_amp_04, ROUND(STDDEV(max_amp_4), 5) as std_of_max_amp_04,
                    		ROUND(AVG(max_amp_5), 5) as mean_of_max_amp_05, ROUND(STDDEV(max_amp_5), 5) as std_of_max_amp_05,
                    		ROUND(AVG(max_amp_6), 5) as mean_of_max_amp_06, ROUND(STDDEV(max_amp_6), 5) as std_of_max_amp_06
                    FROM
                    		(SELECT date_trunc('minute', "Time_Stamp") AS time_by_minute, "Robot_Name",
                    			ROUND(CAST(MAX("Amp_1") as numeric), 5) as max_amp_1, 
                    			ROUND(CAST(MAX("Amp_2") as numeric), 5) as max_amp_2, 
                    			ROUND(CAST(MAX("Amp_3") as numeric), 5) as max_amp_3, 
                    			ROUND(CAST(MAX("Amp_4") as numeric), 5) as max_amp_4, 
                    			ROUND(CAST(MAX("Amp_5") as numeric), 5) as max_amp_5, 
                    			ROUND(CAST(MAX("Amp_6") as numeric), 5) as max_amp_6
                    			FROM public.everything
                    			WHERE "Time_Stamp" > %(baseline_start_time)s and "Time_Stamp" < %(baseline_end_time)s
                    			GROUP  BY 1, "Robot_Name"
                    			ORDER BY time_by_minute ASC)
                    AS totals
                    GROUP BY "Robot_Name";
                            """
                    
                    #Execute Query
                    cur.execute(sql, {'baseline_start_time':self.baseline_start_time, 'baseline_end_time':self.baseline_end_time})
                    
                    #Get query Result regardless of query case
                    #Get Column names from query
                    colnames = [desc[0] for desc in cur.description]
                    
                    # Retrieve query results
                    records = cur.fetchall()
                    
                    #Convert to Pandas Dataframe for Processing
                    query_results = pd.DataFrame(data = records, columns = colnames) 

                    return query_results                               
                
                #Case in which processed baseline needs to be stored to a database    
                elif query==2:
                    
                    #Attempt Initial Connection to database where baseline is stored
                    conn = pg.connect(
                        host=str(self.processed_db_address),
                        database=self.processed_db_name,
                        user=self.processed_db_username,
                        password=self.processed_db_password)
                    
    
                    self.logger.debug('Storing Baseline...')
                    
                    #Remove old baseline
                    cur = conn.cursor()
                    sql = """TRUNCATE public.stats_profile_baseline;"""
                    cur.execute(sql)
                    
                    #Create method to insert pd dataframe directly into postgres
                    def execute_values(conn, df, table): 
  
                        tuples = [tuple(x) for x in df.to_numpy()] 
                      
                        cols = ','.join(list(df.columns)) 
                        # SQL query to execute 
                        query = "INSERT INTO %s(%s) VALUES %%s" % (table, cols) 
                        cursor = conn.cursor() 
                        try: 
                            extras.execute_values(cursor, query, tuples) 
                            conn.commit() 
                        except (Exception, pg.DatabaseError) as error: 
                            print("Error: %s" % error) 
                            conn.rollback() 
                            cursor.close() 
                            return 1
                        cursor.close() 
                    
                    #Call above function to insert baseline dataframe into baseline table in postgres
                    execute_values(conn, input_dataframe, 'stats_profile_baseline')
                    
                    self.logger.debug('Stored Baseline Successfully...')
                
                #Query to get raw data. This is the sample data that is compared to the baseline
                elif query==3:
                
                    #Attempt Initial Connection to database where raw data is stored
                    conn = pg.connect(
                        host=str(self.raw_db_address),
                        database=self.raw_db_name,
                        user=self.raw_db_username,
                        password=self.raw_db_password)
                    
    
                    self.logger.debug('Retrieving Raw Data...')
                    #Open a cursor to perform database operations
                    cur = conn.cursor()
                    
                    #Build Dynamic Query
                    sql = """SELECT date_trunc('minute', "Time_Stamp") AS time_by_minute, "Robot_Name",
                                ROUND(CAST(MAX("Amp_1") as numeric), 5) as max_amp_1, 
                                ROUND(CAST(MAX("Amp_2") as numeric), 5) as max_amp_2, 
                                ROUND(CAST(MAX("Amp_3") as numeric), 5) as max_amp_3, 
                                ROUND(CAST(MAX("Amp_4") as numeric), 5) as max_amp_4, 
                                ROUND(CAST(MAX("Amp_5") as numeric), 5) as max_amp_5, 
                                ROUND(CAST(MAX("Amp_6") as numeric), 5) as max_amp_6
                                FROM public.everything
                                WHERE "Time_Stamp" > %(sample_start_time)s and "Time_Stamp" < %(sample_end_time)s
                                GROUP  BY 1, "Robot_Name"
                                ORDER BY time_by_minute ASC;
                            """
                    
                    #Execute Query
                    cur.execute(sql, {'sample_start_time':self.sample_start_time, 'sample_end_time':self.sample_end_time})
                    
                    #Get query Result regardless of query case
                    #Get Column names from query
                    colnames = [desc[0] for desc in cur.description]
                    
                    # Retrieve query results
                    records = cur.fetchall()
                    
                    #Convert to Pandas Dataframe for Processing
                    query_results = pd.DataFrame(data = records, columns = colnames) 

                    return query_results
                
                #Query to retrieve processed baseline data
                elif query==4:
                
                    #Attempt Initial Connection to database where raw data is stored
                    conn = pg.connect(
                        host=str(self.raw_db_address),
                        database=self.raw_db_name,
                        user=self.raw_db_username,
                        password=self.raw_db_password)
                    
    
                    self.logger.debug('Retrieving Baseline...')
                    #Open a cursor to perform database operations
                    cur = conn.cursor()
                    
                    #Build Dynamic Query
                    sql = """SELECT * FROM public.stats_profile_baseline;
                            """
                    
                    #Execute Query
                    cur.execute(sql)
                    
                    #Get query Result regardless of query case
                    #Get Column names from query
                    colnames = [desc[0] for desc in cur.description]
                    
                    # Retrieve query results
                    records = cur.fetchall()
                    
                    #Convert to Pandas Dataframe for Processing
                    query_results = pd.DataFrame(data = records, columns = colnames) 

                    return query_results
                
                #Case in which processed baseline needs to be stored to a database    
                elif query==5:
                    
                    #Attempt Initial Connection to database where baseline is stored
                    conn = pg.connect(
                        host=str(self.processed_db_address),
                        database=self.processed_db_name,
                        user=self.processed_db_username,
                        password=self.processed_db_password)
                    
    
                    self.logger.debug('Storing Anomalies...')
                    
                    #Create method to insert pd dataframe directly into postgres
                    def execute_values(conn, df, table): 
  
                        tuples = [tuple(x) for x in df.to_numpy()] 
                      
                        cols = ','.join(list(df.columns)) 
                        # SQL query to execute 
                        query = "INSERT INTO %s(%s) VALUES %%s" % (table, cols) 
                        cursor = conn.cursor() 
                        try: 
                            extras.execute_values(cursor, query, tuples) 
                            conn.commit() 
                        except (Exception, pg.DatabaseError) as error: 
                            print("Error: %s" % error) 
                            conn.rollback() 
                            cursor.close() 
                            return 1
                        cursor.close() 
                    
                    #Call above function to insert baseline dataframe into baseline table in postgres
                    execute_values(conn, input_dataframe, 'detected_anomalies')
                    
                    self.logger.debug('Stored Anomalies Successfully...')
            
                #If everything goes well break this loop
                break
        
            except Exception as error:
                
                # Print Exception and wait 5 seconds to reconnect
                self.logger.debug("An exception occurred:", type(error).__name__, "–", error)
                time.sleep(5)
                self.logger.debug("Reattempting postgres database connection...") 
    
    def data_processing(self):
        self.logger.debug('Processing data...')
        """
        This is to transform/scale data. This queries the raw data, aggregates, and uses the z-score
        formula, (z = (x - mean) /std), to find anomalies.
        """
        try:
            #Query raw database, process data in postgres, and return results
            self.logger.debug('Retrieving Sample Data...')
            sample_dataframe = self.database_conn(query=3)
            
            #Retrieve Baseline Dataframe (contains mean of max amps, std of max amps)
            self.logger.debug('Retrieving Baseline...')
            baseline_dataframe = self.database_conn(query=4)
            
            #Anomaly Detection Procedure
            #Get unique list of robots from baseline (this will only check robots that exist in baseline)
            robot_list = baseline_dataframe['robot_name'].unique()
            
            #Create blank anomaly list to append to
            anomaly_list =[]
            #Create list for the name of each joint
            joint_list = ['max_amp_1', 'max_amp_2', 'max_amp_3', 'max_amp_4', 'max_amp_5', 'max_amp_6']
            
            
            #Zscore calculation (this function is applied to each data point)
            def z_score (value, mean, std):                
                #zscore calc
                anomaly_score = (value - mean) / std                
                return round(anomaly_score, 5)
            
            #Loop over each robot for anomalies
            for r in robot_list:
                
                #filter for a single robot at a time
                single_robot_sample = sample_dataframe[sample_dataframe['Robot_Name'].str.contains(r)] 
                single_robot_baseline = baseline_dataframe[baseline_dataframe['robot_name'].str.contains(r)]
                
            #Iterate over each joint
                for j in joint_list:
                    
                    #Iterate over each value in each joint
                    for ind, v in enumerate(single_robot_sample[j]):
                        z = z_score(v, single_robot_baseline['mean_of_max_amp_01'].iloc[0], single_robot_baseline['mean_of_max_amp_01'].iloc[0])
                        
                        #set anomaly threshold
                        if z > float(self.anomaly_threshold) or z < (-1 * float(self.anomaly_threshold)):
                            #append list with anomalies including information about the anomaly
                            anomaly_list.append([single_robot_sample['Robot_Name'].iloc[ind], j, single_robot_sample['time_by_minute'].iloc[ind],  z, single_robot_sample[j].iloc[ind]])
                            
            # Create the pandas DataFrame 
            anomaly_dataframe = pd.DataFrame(anomaly_list, columns = ['robot_name', 'joint', 'time_stamp', 'zscore', 'actual_value']) 
            
            return anomaly_dataframe
                            
                            
                        
                        
                    

                    
        except Exception as error:
            
            # Print Exception and wait 5 seconds to reconnect
            self.logger.debug("An exception occurred:", type(error).__name__, "–", error)
            time.sleep(5)
            self.logger.debug("Reattempting postgres database connection...") 
        
    def find_anomalies(self):
        self.logger.debug('Searching for anomalies...')
        
    def send_results(self):
        self.logger.debug('Sending results...')
        
#Run Program if Main Program
if __name__ == '__main__':
    
    #Create Object
    stat_profile = statistical_profiling()
    
    #Run main
    out = stat_profile.main()

        
    