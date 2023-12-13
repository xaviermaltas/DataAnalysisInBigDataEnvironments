from time import sleep
import socket
import json
from mastodon import Mastodon

my_access_token = 'BNzHT7-XN8r9KMnEX0SLtXsNSoMyCH7b8aRgqrdlBhU'

api = Mastodon(access_token=my_access_token, api_base_url="https://mastodon.social")

HOST = 'localhost'  # hostname o IP address
PORT = 20046  # puerto socket server

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.connect((HOST, PORT))
toot_dict_anterior = {}
received_messages = []  # List to store received messages


while True:
    print('\nSending information to Flume', HOST, PORT)
    try:
        while(True):
            resultats = api.timeline_hashtag('war', limit=1)
            json_str = json.dumps(resultats,indent=4,sort_keys=True, default=str)
            json_object = json.loads(json_str)
            
            toot_dict = {
                "missatge": json_object[0]["content"],
                "usuari": json_object[0]["account"]["username"],
                "data": json_object[0]["created_at"]
            }
            
            if toot_dict != toot_dict_anterior:
                # Add the toot information to the list
                received_messages.append(toot_dict)
                toot_dict_anterior = toot_dict

                if len(received_messages) >= 5:
                    t_str = json.dumps(received_messages, indent=4, sort_keys=True, default=str)
                    t = t_str.encode('utf-8')
                    s.send(t)
                    print(received_messages)
                    received_messages = []  # Reset the list for the next set of messages

            sleep(10)  

    except socket.error:
        print('Error.\n\nServer disconnected.\n')
        s.close()