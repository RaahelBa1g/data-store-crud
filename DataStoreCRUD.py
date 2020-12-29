import threading
import time
from threading import *


class DataStoreCRUD:
    data_store = {}  # 'd' is the dictionary in which we store data
    """
    CREATE OPERATION
    Syntax - create(key_name, value, time_to_live)
    time_to_live is optional
    """
    def create(self, key, value, time_to_live=0):
        if key in self.data_store:
            print(f"Error: {key} already exists.")
        else:
            if key.isalpha():
                if isinstance(value, dict):
                    if self.data_store.__sizeof__() < (1024 * 1024 * 1024) and value.__sizeof__() <= (16 * 1024 * 1024):
                        """
                        Non Functional Requirements:
                            1. Maximum size for the file storing data - 1GB
                            2. Maximuum value of the JSON value - 16KB
                        """
                        if time_to_live == 0:
                            store = [value, time_to_live]
                        else:
                            store = [value, time.time() + time_to_live]
                        # Key is always a string - capped at 32 chars
                        if len(key) <= 32:
                            self.data_store[key] = store
                    else:
                        print("Error: Memory Limit Exceeded! Max size of file is 1GB and Max value of JSON is 16KB.")
                else:
                    print(f"Error: Given value: {value} is not a Dictionary! Value should be JSON and capped at 16KB.")
            else:
                print(f"Error: Given key: {key} is not a string. Key should be a string capped at 32 characters.")

    """
    READ OPERATION
    Syntax - read(key_name)
    """
    def read(self, key):
        if key not in self.data_store:
            print(f"Error: Given key: {key} does not exist in database. Please enter a valid key.")
        else:
            pair = self.data_store[key]
            if pair[1] != 0:
                if time.time() < pair[1]:  # comparing the present time with expiry time
                    form = str(key) + ":" + str(pair[0])  # to return the value in the format of JSON i.e., "key_name:value"
                    return form
                else:
                    print(f"Error: Time-to-live of {key} has expired.")
            else:
                form = str(key) + ":" + str(pair[0])
                return form

    """
    UPDATE OPERATION
    Syntax - update(key_name)
    """
    def update(self, key, value):
        pair = self.data_store[key]
        if pair[1] != 0:
            if time.time() < pair[1]:
                if key not in self.data_store:
                    print(f"Error: Given key: {key} does not exist in database. Please enter a valid key.")
                else:
                    store = [value, pair[1]]
                    self.data_store[key] = store
            else:
                print(f"Error: Time-to-live of {key} has expired.")
        else:
            if key not in self.data_store:
                print(f"Error: Given key: {key} does not exist in database. Please enter a valid key.")
            else:
                store = [value, pair[1]]
                self.data_store[key] = store

    """
    DELETE OPERATION
    Syntax - delete(key_name)
    """
    def delete(self, key):
        if key not in self.data_store:
            print(f"Error: Given key: {key} does not exist in database. Please enter a valid key.")
        else:
            pair = self.data_store[key]
            if pair[1] != 0:
                if time.time() < pair[1]:
                    del self.data_store[key]
                    print("Key is successfully deleted!")
                else:
                    print(f"Error: Time-to-live of {key} has expired.")
            else:
                del self.data_store[key]
                print("Key is successfully deleted!")
