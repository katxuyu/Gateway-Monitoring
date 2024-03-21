import logging
from things import GatewayServerService 
from things import GatewayRegistryService

import json
from dateutil import parser
import pandas as pd

import psycopg2 as pg
from datetime import datetime

url1 = ""
url2 = ""
API_KEY = ''

pg_HOST = ""
pg_DATABASE = "postgres"
pg_USER = ""
pg_PASSWORD = ""

gr = GatewayRegistryService(url1,API_KEY=API_KEY)
gs = GatewayServerService(url2,API_KEY=API_KEY)

def connect_to_db():
    try:
        conn = pg.connect(
            host=pg_HOST,
            database=pg_DATABASE,
            user=pg_USER,
            password=pg_PASSWORD)
    except Exception as e:
        return f"Error in connecting to database: {e}"
    else:
        return conn

def get_values_on_db(conn,gateway_id):
    try:
        cursor = conn.cursor()
        postgreSQL_select_Query = f"SELECT last_connected, disconnected_since FROM gateway_statuses WHERE gateway_id = '{gateway_id}'"
        cursor.execute(postgreSQL_select_Query)
        mobile_records = cursor.fetchall()
    except Exception as e:
        return f"Error in fetching data to database: {e}"
    else:
        return mobile_records
    finally:
        if conn:
            cursor.close()

def insert_values_on_db(conn, values):
    try:
        cursor = conn.cursor()
        postgreSQL_select_Query = """INSERT INTO gateway_statuses (gateway_id, eui, name, connected_at, disconnected_at, 
                                    is_connected, last_connected, downtime_duration, last_uplink_received_at, 
                                    last_downlink_received_at, last_status_received_at, disconnected_since, 
                                    last_check) 
                                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
        cursor.execute(postgreSQL_select_Query, tuple(values))
        conn.commit()
    except (Exception, pg.DatabaseError) as e:
        return f"Error in inserting data to database: {e}"
    else:
        return "Success!"
    finally:
        if conn:
            cursor.close()

def update_values_on_db(conn, values):
    try:
        cursor = conn.cursor()
        postgreSQL_select_Query = """UPDATE gateway_statuses
                                    SET 
                                    name = %s,
                                    connected_at = %s,
                                    disconnected_at = %s,
                                    is_connected = %s,
                                    last_connected = %s,
                                    downtime_duration = %s,
                                    last_uplink_received_at = %s, 
                                    last_downlink_received_at = %s, 
                                    last_status_received_at = %s, 
                                    disconnected_since = %s, 
                                    last_check = %s
                                
                                    where gateway_id = %s and eui = %s"""
        cursor.execute(postgreSQL_select_Query, tuple(values))
        conn.commit()
    except (Exception, pg.DatabaseError) as e:
        return f"Error in inserting data to database: {e}"
    else:
        return "Success!"
    finally:
        if conn:
            cursor.close()

def get_stats(db_connection):
    gateways = json.loads(gr.ListGateways())
    
    batches = [[]]
    ids = []
    euis = []
    index = 0
    
    for gateway in gateways['gateways']:
        if len(batches[index]) == 50:
            index += 1
            batches.append([])
            euis.append([])
        batches[index].append({"gateway_id":gateway["ids"]["gateway_id"]})
        ids.append(gateway["ids"]["gateway_id"])
        euis.append(gateway["ids"]["eui"])

    responses = []
    
    for batch in batches:
        res = gs.BatchGetGatewayConnectionStats(DATA={"gateway_ids": batch})
        res = str({"entries": {"Error": res}}) if 'HTTP error occurred' in res else res
        
        responses.append(res)
        

    records = []
    for id, eui in zip(ids, euis):
        in_entries = False
        last_check = datetime.utcnow()
        disconnected_since = last_check
        db_last_connected = ""
        db_disconnected_since = ""
        rec_data = get_values_on_db(db_connection, id)
        gw_exist = False
        if type(rec_data) != str and "Error" not in rec_data:
            if len(rec_data) == 1:
                gw_exist = True
                for row in rec_data:
                    db_last_connected = str(row[0])
                    db_disconnected_since = str(row[1])
                    #print(row)
            elif len(rec_data) > 1:
                logging.warn("Warning in data_validation: To many records for this gateway. Deleting record and will insert the latest one.")
                #### DELETE THE OLDEST ONE ####
            else:
                gw_exist = False
        else:
            return rec_data

        for res in responses:
            res = json.loads(res)
            
            if id in res['entries']:
                in_entries = True
                connected_at = res['entries'][id]["connected_at"] if "connected_at" in res['entries'][id] else ""
                disconnected_at = res['entries'][id]["disconnected_at"] if "disconnected_at" in res['entries'][id] else ""
                last_uplink_received_at = res['entries'][id]["last_uplink_received_at"] if "last_uplink_received_at" in res['entries'][id] else ""
                last_downlink_received_at = res['entries'][id]["last_downlink_received_at"] if "last_downlink_received_at" in res['entries'][id] else ""
                last_status_received_at = res['entries'][id]["last_status_received_at"] if "last_status_received_at" in res['entries'][id] else ""


                no_time = parser.parse("0001-01-01T01:01:0Z")
                cv_last_uplink_received_at =  parser.parse(last_uplink_received_at) if last_uplink_received_at != "" else no_time
                cv_last_downlink_received_at=  parser.parse(last_downlink_received_at) if last_downlink_received_at != "" else no_time
                cv_last_status_received_at = parser.parse(last_status_received_at) if last_status_received_at != "" else no_time
                cv_connected_at = parser.parse(connected_at) if connected_at != "" else no_time
                cv_disconnected_at = parser.parse(disconnected_at) if disconnected_at != "" else no_time

                last_connected = str(max(cv_last_uplink_received_at, cv_last_downlink_received_at, cv_last_status_received_at, cv_connected_at, cv_disconnected_at)).replace(" ", "T").replace("+00:00","Z")
                               
                if connected_at:
                    is_connected = True
                    disconnected_since = ""
                elif disconnected_at:
                    is_connected = False
                    if parser.parse(last_connected) != no_time:
                        disconnected_since = last_connected 
                    elif db_disconnected_since !=  "None"  and db_disconnected_since != "":
                        disconnected_since = db_disconnected_since
                    

                if parser.parse(last_connected) == no_time and db_last_connected !=  "None" and db_last_connected != "":
                    last_connected = db_last_connected
                
                if parser.parse(last_connected) != no_time and not is_connected and disconnected_at != "":
                    downtime_duration = (parser.parse(disconnected_at) - parser.parse(last_connected)).total_seconds()
                elif db_disconnected_since !=  'None' and db_disconnected_since != "" and not is_connected:
                    downtime_duration = (last_check - parser.parse(db_disconnected_since)).total_seconds()
                else:
                    downtime_duration = 0
                
                

                cv = []
                for item in [connected_at, disconnected_at, last_connected, last_uplink_received_at,last_downlink_received_at,last_status_received_at, str(disconnected_since)]:
                    item = parser.parse(item).replace(microsecond=0) if item != "" and parser.parse(item) != no_time else None
                    cv.append(str(item).replace("+00:00","") if item != None else item)
                    
                 
                connected_at, disconnected_at, last_connected, last_uplink_received_at,last_downlink_received_at,last_status_received_at, disconnected_since = tuple(cv)
                
                data = [gw_exist, id, eui, connected_at, disconnected_at, is_connected, last_connected, round(downtime_duration, 4), last_uplink_received_at, last_downlink_received_at, last_status_received_at, disconnected_since]
                
                records.append(data)
        if not in_entries:
            if db_disconnected_since !=  "None" and db_disconnected_since != "":
                disconnected_since = parser.parse(db_disconnected_since)
            downtime_duration = (last_check - disconnected_since).total_seconds()
            records.append([gw_exist,id, eui, None, None, False, None, downtime_duration, None, None, None, str(disconnected_since.replace(microsecond=0)).replace("+00:00","")])
        

        
    for record in records:
        gw_exist = record.pop(0) 
        res = gr.GetGateway(GATEWAY_ID=record[0], PARAMS={"field_mask": "name"})
        res = json.loads(res)
        record.insert(2, res["name"])
        
        if gw_exist:
            id = record.pop(0)
            eui = record.pop(0) 
            record.extend([str(last_check.replace(microsecond=0)), id, eui])
            res = update_values_on_db(db_connection, record)
            print(res, record)
        else:
            record.append(str(last_check.replace(microsecond=0)))
            res = insert_values_on_db(db_connection, record)
            
    # header = ["id", "eui", "name", "connected_at", "disconnected_at", "is_connected", "last_connected", "downtime_duration", "last_uplink_received_at","last_downlink_received_at","last_status_received_at"]
    # df = pd.DataFrame(cv_rec)
    # df.to_csv('output.csv', header=header, index=False)



    

def main():
    conn = connect_to_db()
    if type(conn) != str or "Error" not in conn:
        res = get_stats(conn)
        logging.error(res)
        if conn:
            conn.close()
    else:
        logging.error(conn)
    




main()