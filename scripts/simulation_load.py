import json
import threading
import time
import os

IP = "192.168.122.13"
PORT = 1323

file_path = "../dqn_utils/arrivi.json"

def invoke_function(function_name, param, class_name):
    command = f"../bin/serverledge-cli invoke -H {IP} -P {PORT} -f {function_name} -c \"{class_name}\" -p \"n:{param}\""
    os.system(command)

with open(file_path, "r") as f:
    data = json.load(f)

threads = []
prev_key = None
for key, value in data.items():
    if prev_key == None:
        delay = float(key)
    else:
        delay = float(key) - prev_key
    prev_key = float(key)
    time.sleep(delay)

    print(key, value)

    f = value[0]
    c = value[1]

    if f == "f1":
        param = 8100
    elif f == "f2":
        param = 6000
    elif f == "f3":
        param = 7200
    elif f == "f4":
        param = 6550
    else:
        param = 8500

    thread = threading.Thread(target=invoke_function, args=(f, param, c))
    threads.append(thread)
    thread.start()

for thread in threads:
    thread.join()

print("Tutti i thread hanno terminato.")
