import threading
import time
import socket
import csv
from datetime import datetime
import json


class myThread(threading.Thread):
    def __init__(self, data1, port1, data2, port2):
        threading.Thread.__init__(self)
        self.data1 = data1
        self.port1 = port1
        self.data2 = data2
        self.port2 = port2

    def run(self):
        j = 1
        for i in range(1, len(self.data1)):
            str_time = str(datetime.now())

            s1 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

            s1.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s2.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

            s1.bind(('localhost', self.port1))
            s2.bind(('localhost', self.port2))

            s1.listen(5)
            s2.listen(5)

            client1, addr1 = s1.accept()
            client2, addr2 = s2.accept()

            tmp_list = []
            tmp_list.append(str_time)
            tmp_list.append(self.data1[i])

            data = json.dumps({"climate": tmp_list})
            client1.sendall(data.encode())

            final_tmp_list = []
            final_tmp_list.append(str_time)

            temp_list = []

            temp_list.append(self.data2[j])
            temp_list.append(self.data2[j + 1])
            temp_list.append(self.data2[j + 2])
            temp_list.append(self.data2[j + 3])
            temp_list.append(self.data2[j + 4])
            final_tmp_list.append(temp_list)
            j = j + 5
            # client1.sendall(str(self.data1[i]).encode())
            data1 = json.dumps({"fire": final_tmp_list})
            client2.sendall(data1.encode())

            print(str(tmp_list))
            print(str(final_tmp_list))

            time.sleep(1)

            client1.close()
            client2.close()


with open('ClimateData-Part2.csv', 'r') as f:
    climate = []
    fire = []
    reader = csv.reader(f)
    for row in reader:
        climate.append(row)
with open('FireData-Part2.csv', 'r') as g:
    reader1 = csv.reader(g)
    for row1 in reader1:
        fire.append(row1)
# Create new threads
thread1 = myThread(climate, 9999, fire, 9998)

# Start new Threads
thread1.start()

print("Exiting Main Thread")