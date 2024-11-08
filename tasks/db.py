# functions for getting user, updating user, getting history and updating history

from collections import Counter
from bson import ObjectId
import pymongo
from datetime import datetime
import os
import json


# MongoDB connection URI
mongo_uri = os.environ.get("DB_URL")
db_name = os.environ.get("DB_NAME")

# Connect to MongoDB
client = pymongo.MongoClient(mongo_uri)
db = client.get_database(db_name)




def get_history_by_id(history_id):
    try:
        object_id = ObjectId(history_id)
        history_item = db.history.find_one({"_id": object_id})
        
        if not history_item:
            raise ValueError("History item not found")
            
        return history_item
        
    except pymongo.errors.InvalidId:
        raise ValueError("Invalid ObjectId format")
    except Exception as e:
        raise e

def update_history_item(history_id, update_data):
    try:
        object_id = ObjectId(history_id)

        if not db.history.find_one({"_id": object_id}):
            raise ValueError("History item not found")

        filtered_update = {k: v for k, v in update_data.items() if v is not None}
        
        filtered_update["updated_timestamp"] = int(datetime.now().timestamp())
        
        result = db.history.find_one_and_update(
            {"_id": object_id},
            {"$set": filtered_update},
            return_document=pymongo.ReturnDocument.AFTER
        )
        
        if not result:
            raise ValueError("Update failed")
            
        return result
        
    except pymongo.errors.InvalidId:
        raise ValueError("Invalid ObjectId format")
    except Exception as e:
        raise e

def increment_data_quantity(address, increment_factor):
    """
    Increment total_data_quantity and milestones.data_owned by specified factor
    """
    try:
        update_result = db.users.find_one_and_update(
            {"address": address},
            {
                "$inc": {
                    "total_data_quantity": increment_factor,
                    "milestones.data_owned": increment_factor
                }
            },
            projection={"_id": 0},
            upsert=True,  # Create if doesn't exist
            return_document=pymongo.ReturnDocument.AFTER
        )
        return update_result
        
    except Exception as e:
        print(f"Error incrementing data quantity: {e}")
        return None

def update_user_by_address(address, update_data):
    try:
        if not db.users.find_one({"address": address}):
            return None
            
        filtered_update = {k: v for k, v in update_data.items() if v is not None}
        
        updated_user = db.users.find_one_and_update(
            {"address": address},
            {"$set": filtered_update},
            projection={"_id": 0},  # Exclude _id field
            return_document=pymongo.ReturnDocument.AFTER
        )
        
        return updated_user
        
    except Exception as e:
        print(f"An error occurred while updating user: {e}")
        return None

def get_user_by_address(address):
    try:
        pipeline = [
            {
                "$match": {
                    "$expr": {"$eq": [{"$toLower": "$address"}, address.lower()]}
                }
            },
            {"$project": {"_id": 0}}  # Exclude _id field
        ]
        
        user = db.users.aggregate(pipeline).next()
        return user
        
    except StopIteration:
        return None
    except Exception as e:
        print(f"An error occurred while fetching user: {e}")
        return None

def get_user_history(address, limit=50):
    try:
        # Create pipeline to match address and sort by visitTime
        pipeline = [
            {"$match": {"address": address}},
            {"$sort": {"visitTime": -1}},  # Sort by visitTime in descending order
            {"$limit": limit},
            {"$project": {"_id": 0}}  # Exclude _id field
        ]
        
        # Execute pipeline and convert cursor to list
        history_items = list(db.history.aggregate(pipeline))
        return history_items
        
    except Exception as e:
        print(f"Error fetching user history: {e}")
        return []

def get_total_history_and_check_fifty(address):
    try:
        total_count = db.history.count_documents({"address": address})
        is_multiple_of_fifty = total_count % 50 == 0 and total_count != 0
        
        return total_count, is_multiple_of_fifty
        
    except Exception as e:
        print(f"Error getting history count: {e}")
        return 0, False
        
# class History:
#     def __init__(
#         self,
#         address,
#         title,
#         category,
#         url,
#         visitTime,
#         subcategory="",
#         domain="",
#         summary="",
#         create_timestamp=int(datetime.now().timestamp()),
#     ):
#         assert isinstance(address, str)
#         assert isinstance(create_timestamp, int)
#         assert isinstance(title, str)
#         assert isinstance(category, str)
#         assert isinstance(subcategory, str)
#         assert isinstance(url, str)
#         assert isinstance(domain, str)
#         assert isinstance(summary, str)
#         assert isinstance(visitTime, float)

#         self.document = {
#             "address": address,
#             "create_timestamp": create_timestamp,
#             "title": title,
#             "category": category,
#             "subcategory": subcategory,
#             "url": url,
#             "domain": domain,
#             "summary": summary,
#             "visitTime": visitTime,
#         }

#     def save(self):
#         if find_by_address_and_time(
#             self.document["address"], self.document["visitTime"], self.document["url"]
#         ):
#             return
#         db.history.insert_one(self.document)