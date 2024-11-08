import json
from dotenv import load_dotenv

import os 
load_dotenv()

from tasks.db import *
from tasks.classify import * 
from tasks.upload import *
from tasks.pii import *


with open('sample.json', 'r') as file:
    items = json.load(file)
    data = items["items"][2]
    try:
        size = len(data)
        print(size)
        data_json = data
        print(data_json)
        history_item = get_history_by_id(data_json["_id"])
        print(history_item)
        address = history_item["address"]
        print(address)
        user = increment_data_quantity(address, size)
        print(user)

        # update the database -> history update and activity_graph update 
        activity_json = user["activity_json"]
        print(activity_json)
        activity = get_most_relevant_activity(data_json.get("summary", data_json.get("title", "")))
        print(activity)
        kleo_points = user["kleo_points"]
        if activity not in activity_json:
            activity_json[activity] = 1
        else:
            activity_json[activity] += 1

        print(get_total_history_and_check_fifty(address))
        if get_total_history_and_check_fifty(address):
            history_items = get_user_history(address)
            
            prepared_json = prepare_history_json(history_items, address, user)
            print(prepared_json)
            irys_hash = upload_to_arweave(prepared_json)
            print(irys_hash)
            u = update_user_by_address(address, {"activity_json": activity_json, "previous_hash": irys_hash, "kleo_points": int(kleo_points) + 12})
            print(u)
        else:
            u = update_user_by_address(address, {"activity_json": activity_json})
            print(u)

        
        print("Activity {activity}, Data Size: {ds}".format(
                activity=activity,
                ds=size))
    except:
        pass