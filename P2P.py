# Tested and fully operational on CSE computers
# z5275538
# Rohan Ajay Jain


import os
import socket
import sys
import threading
import time


class Peer:
    def __init__(self):
        self.host = "localhost"
        self.id = int(sys.argv[2])
        self.run_type = sys.argv[1].lower()
        self.hash = 256
        self.files = set()
        self.lock = threading.Condition()

        # Base port values
        self.def_udp = 30000
        self.def_tcp = 40000

        self.udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.udp_socket.bind((self.host, self.def_udp + self.id))

        # Successors
        self.s1_id = None
        self.s2_id = None
        self.s1_count = 0
        self.s2_count = 0

        # Predecessors
        self.p1_id = None
        self.p2_id = None

        if self.run_type == "init":
            self.ping_interval = int(sys.argv[5])

            self.s1_id = int(sys.argv[3])
            self.s2_id = int(sys.argv[4])

        elif self.run_type == "join":
            self.ping_interval = int(sys.argv[4])
            # Known peer
            self.kp_id = int(sys.argv[3])

    # Sends data to a socket via UDP
    def udp_send(self):
        while True:
            print(f"Ping request sent to Peers {self.s1_id} and {self.s2_id}")

            # First successor is no longer alive
            if self.s1_count > 2:
                data = f"SVR: {self.id} 1"
                self.tcp_send((self.host, self.def_tcp + self.s2_id), data)
                self.s1_count = 0

            # Second successor is no longer alive
            elif self.s2_count > 2:
                data = f"SVR: {self.id} 2"
                self.tcp_send((self.host, self.def_tcp + self.s1_id), data)
                self.s2_count = 0

            else:
                # Send ping requests to successors
                data = "REQUEST:Ping 1".encode()
                address = (self.host, self.def_udp + self.s1_id)
                self.udp_socket.sendto(data, address)
                self.s1_count += 1

                data = "REQUEST:Ping 2".encode()
                address = (self.host, self.def_udp + self.s2_id)
                self.udp_socket.sendto(data, address)
                self.s2_count += 1

            time.sleep(self.ping_interval)

    # Sends data to an address via TCP
    @staticmethod
    def tcp_send(address, data):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect(address)
        s.sendall(data.encode())
        s.close()

    # Receives data via UDP
    def udp_receive(self):
        while True:
            data, send_socket = self.udp_socket.recvfrom(2048)

            self.lock.acquire()
            send_id = send_socket[1] - self.def_udp
            data = data.decode()
            reply = None

            # Received ping request
            if data.startswith("REQUEST:Ping "):
                print(f"Ping request message received from Peer {send_id}")
                if data[-1] == "1":
                    self.p1_id = send_id

                elif data[-1] == "2":
                    self.p2_id = send_id

                reply = "REPLY:Ping".encode()

            # Received ping reply
            elif data == "REPLY:Ping":
                print(f"Ping response received from Peer {send_id}")

                # Decrement successor counts to show peer is alive in udp_send
                if send_id == self.s1_id:
                    self.s1_count -= 1

                elif send_id == self.s2_id:
                    self.s2_count -= 1

            # Send reply
            if reply:
                self.udp_socket.sendto(reply, send_socket)

            self.lock.release()

    # Receives data via TCP
    def tcp_receive(self):
        # Wait for incoming TCP connections
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.bind((self.host, self.def_tcp + self.id))
        s.listen()

        while True:
            # Accept connection to client
            c = s.accept()[0]
            data = c.recv(2048).decode()
            self.lock.acquire()

            # Parse data to determine action taken

            if data.startswith("REQUEST:Join "):
                # Received request to join
                j_id = int(data.split()[1])

                # Determine if joining peer should be placed as next peer
                if (self.id < j_id < self.s1_id
                        or j_id > self.id > self.s1_id
                        or self.id > self.s1_id > j_id):

                    print(f"Peer {j_id} request received")

                    data = f"REPLY:Join {self.s1_id} {self.s2_id}"

                    # Notify requesting peer that the request has been approved
                    address = (self.host, self.def_tcp + j_id)
                    self.tcp_send(address, data)

                    # New first successor is now the joining peer
                    self.s2_id = self.s1_id
                    self.s1_id = j_id

                    print(f"My new first successor is Peer {self.s1_id}")
                    print(f"My new second successor is Peer {self.s2_id}")

                    data = f"SCR {self.s1_id}"

                    # Notify first predecessor of successor changes
                    address = (self.host, self.def_tcp + self.p1_id)
                    self.tcp_send(address, data)

                else:
                    # Join request forwarded to successor
                    address = (self.host, self.def_tcp + self.s1_id)
                    self.tcp_send(address, data)
                    print(f"Peer {j_id} Join request forwarded to my successor")

            elif data.startswith("SCR "):
                # Received successor change request
                self.s2_id = int(data.split()[1])
                print(f"My new first successor is Peer {self.s1_id}")
                print(f"My new second successor is Peer {self.s2_id}")

            elif data.startswith("REPLY:Join "):
                # Received join approval, set successors
                data_list = data.split()
                self.s1_id = int(data_list[1])
                self.s2_id = int(data_list[2])
                print(f"Join request has been accepted")
                print(f"My new first successor is Peer {self.s1_id}")
                print(f"My new second successor is Peer {self.s2_id}")

            elif data.startswith("QUIT "):
                # Received graceful departure notification
                data_list = data.split()

                # Updating successors
                if data_list[-1] == "1":
                    self.s1_id = int(data_list[2])
                    self.s2_id = int(data_list[3])

                elif data_list[-1] == "2":
                    self.s2_id = int(data_list[2])

                print(f"Peer {data_list[1]} will depart from the network")
                print(f"My new first successor is Peer {self.s1_id}")
                print(f"My new second successor is Peer {self.s2_id}")

            elif data.startswith("SVR: "):
                # Peer found dead successor
                # Sending successors and departed successor number
                _, send_id, dead = data.split()
                data = f"REPLY:SVR: {self.s1_id} {self.s2_id} {dead}"
                address = (self.host, self.def_tcp + int(send_id))
                self.tcp_send(address, data)

            elif data.startswith("REPLY:SVR: "):
                # Received abrupt departure reply
                _, s1, s2, dead = data.split()
                s1 = int(s1)

                if dead == "1":
                    print(f"Peer {self.s1_id} is no longer alive")
                    self.s1_id = self.s2_id
                    self.s2_id = s1

                elif dead == "2":
                    print(f"Peer {self.s2_id} is no longer alive")
                    if s1 == self.s2_id:
                        # Successor sent outdated successor details

                        self.s2_id = int(s2)
                    else:
                        # Successor sent latest successor details
                        self.s2_id = s1

                print(f"My new first successor is Peer {self.s1_id}")
                print(f"My new second successor is Peer {self.s2_id}")

            elif data.startswith("STORE: "):
                self.store_file(data.split()[1])

            elif data.startswith("REQUEST:FILE: "):
                _, f_name, requester = data.split()
                self.retrieve_file(f_name, int(requester))

            elif data.startswith("TRANSFER: "):
                _, send_id, entry_name = data.split()
                reply = "APPROVED"
                c.sendall(reply.encode())
                new_f_name = "received_" + entry_name
                with open(new_f_name, "wb") as f:
                    l = c.recv(2048)
                    while l:
                        f.write(l)
                        l = c.recv(2048)
                print(f"File {entry_name.split('.')[0]} received")

            self.lock.release()

    # Store file at peer
    def store_file(self, f_name):
        storing_peer = int(f_name) % self.hash

        if (self.id == storing_peer
                or self.id < self.p1_id < storing_peer > self.id
                or self.id < self.p1_id > storing_peer < self.id
                or self.id > storing_peer > self.p1_id):

            # Store file at this peer
            self.files.add(f_name)
            print(f"Store {f_name} request accepted")

        else:
            # Forward store request to successor
            data = f"STORE: {f_name}"
            address = (self.host, self.def_tcp + self.s1_id)
            self.tcp_send(address, data)
            print(f"Store {f_name} request forwarded to my successor")

    # Retrieve file from peer
    def retrieve_file(self, f_name, requester):
        storing_peer = int(f_name) % self.hash

        if (self.id == storing_peer
                or self.id < self.p1_id < storing_peer > self.id
                or self.id < self.p1_id > storing_peer < self.id
                or self.id > storing_peer > self.p1_id):

            # File stored at this peer
            if f_name in self.files:
                requester_address = (self.host, self.def_tcp + requester)
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

                with os.scandir(os.getcwd()) as cwd:
                    for entry in cwd:
                        if entry.name.startswith(f_name) and entry.is_file:
                            print(f"File {f_name} is stored here")
                            data = f"TRANSFER: {self.id} {entry.name}"
                            s.connect(requester_address)
                            s.sendall(data.encode())
                            reply = s.recv(2048)

                            if reply.decode() != "APPROVED":
                                print("ERROR: Send request rejected")
                                return

                            with open(entry.name, "rb") as f:
                                print(f"Sending file {f_name} to Peer {requester}")
                                s.sendfile(f)
                            print("The file has been sent")


        else:
            # Forward request to successor
            data = f"REQUEST:FILE: {f_name} {requester}"
            address = (self.host, self.def_tcp + self.s1_id)
            self.tcp_send(address, data)
            print(f"File request for {f_name} has been sent to my successor")


if __name__ == "__main__":
    p = Peer()

    # Initialise threads
    tcp_receive_thread = threading.Thread(target=p.tcp_receive, daemon=True)
    udp_send_thread = threading.Thread(target=p.udp_send, daemon=True)
    udp_receive_thread = threading.Thread(target=p.udp_receive, daemon=True)

    tcp_receive_thread.start()

    if p.run_type == "join":
        # Ping known peer address to join network
        msg = f"REQUEST:Join {p.id}"
        p.tcp_send((p.host, p.def_tcp + p.kp_id), msg)

    udp_send_thread.start()
    udp_receive_thread.start()

    while True:
        try:
            command = input().lower()
            if command == "quit":
                # Indicating exit to successors
                msg = f"QUIT {p.id} {p.s1_id} {p.s2_id} 1"
                p.tcp_send((p.host, p.def_tcp + p.p1_id), msg)

                msg = f"QUIT {p.id} {p.s1_id} 2"
                p.tcp_send((p.host, p.def_tcp + p.p2_id), msg)

                sys.exit()

            elif command.startswith("store "):
                # Data insertion
                file_name = command.split()[1]
                p.store_file(file_name)

            elif command.startswith("request "):
                # Data retrieval
                _, file_name = command.split()
                if file_name in p.files:
                    print(f"File already at {p.id}")
                else:
                    p.retrieve_file(file_name, p.id)

            else:
                print("Invalid input...Try again")

        except KeyboardInterrupt:
            sys.exit()