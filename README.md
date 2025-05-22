# raftandspark
Developed a distributed, fault-tolerant course registration system using the Raft consensus algorithm and Apache Spark. Designed to manage student, course, and enrollment data across multiple servers, ensuring consistency, high availability, and scalability in a simulated university environment.

# Steps to Run the Code on multiple devices
1. Find the IP address of all devices and add it to cluster.txt file in both the main folder and client folder. Examples:
    - "IP_address": "random_number"
2. To run the cluster: Copy and paste "go run raftNode.go x cluster.txt" and change the x to the corresponding index [0, number_or_processes].
3. To run the client (test included): Copy and paste "go run dataClient.go cluster.txt"
4. After running the client code, there should be 3 separate json files on each devices: enrollment, courses, students
5. To run the test again, remove the json files to get the most accurate results

# Steps to Run the Code on single device
1. Replace IP address with "localhost:" and add it to cluster.txt file in both the main folder and client folder. Examples:
    - localhost:"random_number"
    - localhost:4040
    - localhost:4041
    - localhost:4042
    - localhost:4043
2. To run the cluster: Copy and paste "go run raftNode.go x cluster.txt" and change the x to the corresponding index [0, number_or_processes].
3. To run the client (test included): Copy and paste "go run dataClient.go cluster.txt"
4. After running the client code, there should be 3 separate json files on each devices: enrollment, courses, students
5. To run the test again, remove the json files to get the most accurate results

In the event that one terminal is kill/disconnected for some reason, it can always be connected back through re-running "go run raftNode.go x cluster.txt". It might take a while to reconnect, but it will get there!
