import datetime
import logging

import azure.functions as func

from things import GatewayServerService 
from things import GatewayRegistryService

import json
from dateutil import parser
import pandas as pd

url1 = ""
url2 = ""
API_KEY = ''

gr = GatewayRegistryService(url1,API_KEY=API_KEY)
gs = GatewayServerService(url2,API_KEY=API_KEY)

def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    if mytimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function ran at %s', utc_timestamp)


def get_stats():
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
        print(res if 'HTTP error occurred' in res else "")
        res = {"entries": {"Error": res}} if 'HTTP error occurred' in res else res
        
        responses.append(res)

    records = []
    for id, eui in zip(ids, euis):
        in_entries = False
        for res in responses:
            res = json.loads(res)
            if id in res['entries']:
                in_entries = True
                connected_at = res['entries'][id]["connected_at"] if "connected_at" in res['entries'][id] else ""
                disconnected_at = res['entries'][id]["disconnected_at"] if "disconnected_at" in res['entries'][id] else ""
                last_uplink_received_at = res['entries'][id]["last_uplink_received_at"] if "last_uplink_received_at" in res['entries'][id] else ""
                last_downlink_received_at = res['entries'][id]["last_downlink_received_at"] if "last_downlink_received_at" in res['entries'][id] else ""
                last_status_received_at = res['entries'][id]["last_status_received_at"] if "last_status_received_at" in res['entries'][id] else ""


                no_time = "0001-01-01T01:01:0Z"
                cv_last_uplink_received_at =  parser.parse(last_uplink_received_at) if last_uplink_received_at != "" else parser.parse(no_time)
                cv_last_downlink_received_at=  parser.parse(last_downlink_received_at) if last_downlink_received_at != "" else parser.parse(no_time)
                cv_last_status_received_at = parser.parse(last_status_received_at) if last_status_received_at != "" else parser.parse(no_time)
                cv_connected_at = parser.parse(connected_at) if connected_at != "" else parser.parse(no_time)
                cv_disconnected_at = parser.parse(disconnected_at) if disconnected_at != "" else parser.parse(no_time)

                latest_update = str(max(cv_last_uplink_received_at, cv_last_downlink_received_at, cv_last_status_received_at, cv_connected_at, cv_disconnected_at)).replace(" ", "T").replace("+00:00","Z")

                data = [id, eui, connected_at, disconnected_at, last_uplink_received_at,last_downlink_received_at,last_status_received_at,latest_update]
                
                records.append(data)
        if not in_entries:
            records.append([id, eui, " ", "DISCONNECTED", " ", " ", " ", " "])

    for record in records:  
        res = gr.GetGateway(GATEWAY_ID=record[0], PARAMS={"field_mask": "name"})
        res = json.loads(res)
        record.insert(2, res["name"])

    header = ["id", "eui", "name", "connected_at", "disconnected_at", "last_uplink_received_at","last_downlink_received_at","last_status_received_at","latest_update"]
    df = pd.DataFrame(records)
    df.to_csv('output.csv', header=header, index=False)
    