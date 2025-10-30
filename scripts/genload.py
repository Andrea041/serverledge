import numpy as np
import random
import threading
import time
import os

IP = "192.168.122.13"
PORT = 1323

np.random.seed()
random.seed()

func_to_list = {"f1":[], "f2":[], "f3":[], "f4":[]}

# --- CLASSI: pesi casuali, normalizzati ---
arrival_weights = np.random.dirichlet(np.ones(4), size=1)[0]

classes = [
    {"name": "standard",   "arrival_weight": arrival_weights[0]},
    {"name": "critical-1", "arrival_weight": arrival_weights[1]},
    {"name": "critical-2", "arrival_weight": arrival_weights[2]},
    {"name": "batch",      "arrival_weight": arrival_weights[3]},
]

# Rate varia tra 1 e 5, param tra 6000 e 9000
functions = []
for fname in ["f1", "f2", "f3", "f4"]:
    func = {
        "name": fname,
        "rate": round(random.uniform(1.0, 4.0), 2),
        "param": 0
    }
    functions.append(func)

# --- STAMPA CONFIGURAZIONE CORRENTE ---
print("\n=== PARAMETRI SIMULAZIONE ===")
print("Classi (pesi di arrivo):")
for c in classes:
    print(f"  {c['name']}: {c['arrival_weight']:.3f}")
print("\nFunzioni:")
for f in functions:
    print(f"  {f['name']}: rate={f['rate']}")
print("===============================\n")

weights = [cls['arrival_weight'] for cls in classes]
cumulative_weights = np.cumsum(weights)

def generate_poisson_arrivals(rate, duration):
    return np.cumsum(np.random.exponential(1/rate, int(rate * duration)))

def select_class():
    rand_value = random.uniform(0, cumulative_weights[-1])
    for i, weight in enumerate(cumulative_weights):
        if rand_value <= weight:
            return classes[i]['name']


class ArrivalGenerator(threading.Thread):
    def __init__(self, function, duration):
        super().__init__()
        self.function = function
        self.duration = duration

    def run(self):
        arrivals = generate_poisson_arrivals(self.function['rate'], self.duration)
        for arrival_time in arrivals:
            class_name = select_class()

            param = random.randint(6000, 9000)
        
            func_to_list[self.function['name']].append((arrival_time, self.function['name'], param, class_name))


def invoke_function(function_name, param, class_name):
    command = f"../bin/serverledge-cli invoke -H {IP} -P {PORT} -f {function_name} -c \"{class_name}\" -p \"n:{param}\""
    os.system(command)


duration = 3600

threads = []
for func in functions:
    thread = ArrivalGenerator(func, duration)
    threads.append(thread)
    thread.start()

for thread in threads:
    thread.join()

complete_list = []
for key in func_to_list.keys():
    for t in func_to_list[key]:
        complete_list.append(t)
complete_list.sort(key=lambda x: x[0])

for i, (arrival_time, function_name, param, class_name) in enumerate(complete_list):
    if i == 0:
        delay = arrival_time
    else:
        delay = arrival_time - complete_list[i-1][0]
    time.sleep(delay)
    threading.Thread(target=invoke_function, args=(function_name, param, class_name)).start()