package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"
)

type RaftNode int

type Command struct {
	Operation string
	Key       string
	Value     string
	FileType  string
}

type VoteArguments struct {
	Term         int
	CandidateID  int
	LastLogIndex int // for log safety
	LastLogTerm  int // for log safety
}

type VoteReply struct {
	Term       int
	ResultVote bool
}

type LogEntry struct {
	Index   int
	Term    int
	Command Command
}

type ClientArguments struct {
	EntityType  string
	CommandType string
	EntityID    string
	Data        string
}

type ClientReply struct {
	Content       string // if "R" return this content to client, elif "W" return empty string for content
	Success       bool   // true only after data is applied to file and all logs are consistent
	LeaderAddress string // the IP:port of the true leader
}

type AppendEntryArgument struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntryReply struct {
	Term      int
	Success   bool
	NextIndex int
}

type ServerConnection struct {
	serverID      int
	Address       string
	rpcConnection *rpc.Client
}

// file data
type File struct {
	Students    []Student    `json:"students"`
	Courses     []Course     `json:"courses"`
	Enrollments []Enrollment `json:"enrollments"`
}
type Student struct {
	StudentID int    `json:"studentID"`
	Name      string `json:"name"`
}

type Course struct {
	CourseID int    `json:"courseID"`
	Name     string `json:"name"`
}

type Enrollment struct {
	EnrollmentID int `json:"enrollmentID"`
	StudentID    int `json:"studentID"`
	CourseID     int `json:"courseID"`
}

var selfID int
var serverNodes []ServerConnection
var currentTerm int = 0 // latest term server has seen
var votedFor int = -1   // candidateId that recieved vote in current term (or -1 if none)
var mutex sync.Mutex
var state string = "Follower" // Follower, Candidate, or Leader
var electionTimer *time.Timer
var leaderID int = -1

var log_entries []LogEntry // log entries; each entry contains command for state machine, and terms when entry was received by leader (Specify first index)

// Volatile state on all servers
var commitIndex int = 0 // index of highest log entry known to be committed (increase monotonically)
var lastApplied int = 0 // index of highest log entry applied to state machine (increase monotomically)

// Volatile state on leaders
var nextIndex []int  // for each server, index of the next log entry to send to that server (initialize to leader last log index + 1)
var matchIndex []int // for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)

var student_key_value = make(map[string]string)
var course_key_value = make(map[string]string)
var enrollment_key_value = make(map[string]string)
var myPort string

// The RequestVote RPC as defined in Raft
// Hint 1: Use the description in Figure 2 of the paper
// Hint 2: Only focus on the details related to leader election and majority votes
func (*RaftNode) RequestVote(arguments VoteArguments, reply *VoteReply) error {
	mutex.Lock()
	defer mutex.Unlock()

	// Reply false if term < currentTerm
	if arguments.Term < currentTerm {
		reply.Term = currentTerm
		reply.ResultVote = false
		return nil
	}

	if arguments.Term > currentTerm {
		currentTerm = arguments.Term
		state = "Follower"
		votedFor = -1
	}

	lastLogIndex := 0
	lastLogTerm := 0
	if len(log_entries) > 0 {
		lastLogIndex = log_entries[len(log_entries)-1].Index
		lastLogTerm = log_entries[len(log_entries)-1].Term
	}

	//grant vote if when the log term is larger
	// DONE: If votedFor is null or candidateID, and candidate's log is at least as up-to-date as receiver's log, grant vote
	logOk := false
	if arguments.LastLogTerm > lastLogTerm {
		logOk = true
	} else if arguments.LastLogTerm == lastLogTerm && arguments.LastLogIndex >= lastLogIndex {
		logOk = true
	}

	if (votedFor == -1 || votedFor == arguments.CandidateID) && logOk {
		votedFor = arguments.CandidateID
		reply.ResultVote = true
		log.Printf("Voting for candidate %d in term %d\n", arguments.CandidateID, arguments.Term)
		mutex.Unlock()
		resetElectionTimer()
		mutex.Lock()
	} else {
		if !logOk {
			log.Printf("Rejecting vote for %d because their log is outdated\n", arguments.CandidateID)
		}
		reply.ResultVote = false
	}

	reply.Term = currentTerm
	return nil
}

// The AppendEntry RPC as defined in Raft
func (*RaftNode) AppendEntry(arguments AppendEntryArgument, reply *AppendEntryReply) error {
	mutex.Lock()
	defer mutex.Unlock()

	reply.Success = false
	reply.Term = currentTerm

	// 1. Reply false if term < currentTerm
	if arguments.Term < currentTerm {
		return nil
	}

	leaderID = arguments.LeaderID
	mutex.Unlock()
	resetElectionTimer()
	mutex.Lock()

	if arguments.Term > currentTerm {
		currentTerm = arguments.Term
		state = "Follower"
		votedFor = -1
	}

	// DONE: If an exisiting entry conflicts with a new one (same index but different terms), delete the exisiting entry and all that follow it.
	// start at the index suggested by the match array in the arguments log and check until which one is the wrong index
	// After that append anything that is not already correct in the log and remove entries that are wrong after the log
	// DONE: Append any new entries not already in the log

	// 2. Reply false if log doesn't contain an entry at PrevLogIndex matching PrevLogTerm
	if arguments.PrevLogIndex > 0 {
		if len(log_entries) < arguments.PrevLogIndex {
			log.Printf("Log have %d entries, need index %d\n",
				len(log_entries), arguments.PrevLogIndex)
			if len(log_entries) > 0 {
				reply.NextIndex = len(log_entries)
			} else {
				reply.NextIndex = 1
			}
			return nil
		}

		// If have the entry but terms don't match
		if arguments.PrevLogIndex <= len(log_entries) &&
			(arguments.PrevLogIndex == 0 ||
				log_entries[arguments.PrevLogIndex-1].Term != arguments.PrevLogTerm) {
			log.Printf("Previous log term mismatch at index %d\n", arguments.PrevLogIndex)
			return nil
		}
	}

	// 3. If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it
	if len(arguments.Entries) > 0 {
		newEntries := make([]LogEntry, 0)

		for i := 0; i < arguments.PrevLogIndex; i++ {
			if i < len(log_entries) {
				newEntries = append(newEntries, log_entries[i])
			}
		}

		// Append all the new entries
		newEntries = append(newEntries, arguments.Entries...)

		// Update log
		if len(arguments.Entries) > 0 {
			log.Printf("Appending %d entries starting at index %d\n",
				len(arguments.Entries), arguments.PrevLogIndex+1)
		}
		log_entries = newEntries
	}

	// 4. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, last new entry index)
	if arguments.LeaderCommit > commitIndex {
		lastIndex := 0
		if len(log_entries) > 0 {
			lastIndex = log_entries[len(log_entries)-1].Index
		}

		commitIndex = min(arguments.LeaderCommit, lastIndex)
		log.Printf("Updated commitIndex to %d\n", commitIndex)

		// Apply any newly committed entries
		for lastApplied < commitIndex {
			lastApplied++
			if lastApplied <= len(log_entries) {
				applyEntry(log_entries[lastApplied-1])
				// Apply log_entries[lastApplied-1]
				log.Printf("Applied entry %d: term: %d, index: %d\n", lastApplied, log_entries[lastApplied-1].Term, log_entries[lastApplied-1].Index)

			}
		}
	}

	reply.Success = true
	return nil
}

func applyEntry(entry LogEntry) {
	command := entry.Command

	if command.Operation == "PUT" {
		log.Printf("have added the key_value pair: key %s and value %s\n", command.Key, command.Value)
		if command.FileType == "student" {
			student_key_value[command.Key] = command.Value
		}
		if command.FileType == "course" {
			course_key_value[command.Key] = command.Value
		}
		if command.FileType == "enrollment" {
			enrollment_key_value[command.Key] = command.Value
		}
		save(command.FileType)
	} else if command.Operation == "DELETE" {
		if command.FileType == "student" {
			delete(student_key_value, command.Key)
		}
		if command.FileType == "course" {
			delete(course_key_value, command.Key)
		}
		if command.FileType == "enrollment" {
			delete(enrollment_key_value, command.Key)
		}
		log.Printf("have deleted the key_value pair: key%s\n", command.Key)
		save(command.FileType)
	}
}

func save(file string) error {
	//json
	//{"students": [{student 1}, {student 2}]}
	//this is only for write
	//comment that you finished committing changes to hard disk
	//if students.json file to change
	//
	var fileName string
	var data interface{}

	if file == "student" {
		fileName = "student.json"
		var students []Student
		for _, val := range student_key_value {
			var student Student
			json.Unmarshal([]byte(val), &student)
			students = append(students, student)
		}
		data = students
	} else if file == "course" {
		fileName = "course.json"
		var courses []Course
		for _, val := range course_key_value {
			var course Course
			json.Unmarshal([]byte(val), &course)
			courses = append(courses, course)
		}
		data = courses
	} else if file == "enrollment" {
		fileName = "enrollment.json"
		var enrollments []Enrollment
		for _, val := range enrollment_key_value {
			var enrollment Enrollment
			json.Unmarshal([]byte(val), &enrollment)
			enrollments = append(enrollments, enrollment)
		}
		data = enrollments
	}

	jsonData, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		log.Println(err)
		return err
	}

	err = os.WriteFile(myPort+fileName, jsonData, 0644)
	if err != nil {
		log.Println(err)
		return err
	}

	log.Println("saved data to", fileName)
	return nil
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// You may use this function to help with handling the election time out
// Hint: It may be helpful to call this method every time the node wants to start an election
func LeaderElection() {
	//if the heartbeat is not sent:
	//all the nodes wait for a little, and don't act immediately
	//will start leader election after a little bit of time
	//there is a randomized timer for each node when they all realize leader is gone (no heartbeats)
	//each node has a timer between 100-500ms
	//whoever timer runs out first will start the leader election

	//whoever starts the leader election:
	//increment the term and vote for self
	//send nomination message and request votes from other nodes
	//call RequestVote RPC function

	//only need majority of votes (50%) -- it knows all nodes in system
	mutex.Lock()
	if state == "Leader" {
		mutex.Unlock()
		return
	}

	var wg sync.WaitGroup
	votes := 1
	votedFor = selfID
	currentTerm++
	state = "Candidate"

	term := currentTerm
	log.Printf("Starting election for term %d\n", currentTerm)

	lastLogIndex := 0
	lastLogTerm := 0
	if len(log_entries) > 0 {
		lastLogIndex = log_entries[len(log_entries)-1].Index
		lastLogTerm = log_entries[len(log_entries)-1].Term
	} //for safety check
	mutex.Unlock()

	args := VoteArguments{
		Term:         term,
		CandidateID:  selfID,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}

	for i := range serverNodes {
		wg.Add(1)
		go func(serverIndex int) {
			defer wg.Done()

			server := &serverNodes[serverIndex]
			reply := VoteReply{}
			err := safeCall(server, "RaftNode.RequestVote", args, &reply)
			if err != nil {
				log.Printf("Error requesting vote: %v", err)
				return
			}

			mutex.Lock()
			defer mutex.Unlock()
			//count the number of votes, if majority=>declare itself as leader
			if currentTerm != term || state != "Candidate" {
				return
			}

			// IF RPC request or response continas term T > currentTerm: set currentTerm = T, convert to follower
			if reply.Term > currentTerm {
				// Higher term found, step down to Follower
				state = "Follower"
				currentTerm = reply.Term
				votedFor = -1
				log.Printf("Node with higher term %d found, stepping down to follower\n", reply.Term)
				return
			}

			if reply.ResultVote {
				votes++
				log.Printf("Server %d voted, total votes: %d\n", server.serverID, votes)
				if votes > (len(serverNodes)+1)/2 && state == "Candidate" {
					state = "Leader"
					leaderID = selfID
					log.Printf("Term %d: won election with %d total votes\n", currentTerm, votes)

					nextIndex = make([]int, len(serverNodes))
					matchIndex = make([]int, len(serverNodes))

					nextIdx := 1
					if len(log_entries) > 0 {
						nextIdx = log_entries[len(log_entries)-1].Index + 1
					}

					for i := range nextIndex {
						nextIndex[i] = nextIdx
						matchIndex[i] = 0
					}
					go Heartbeat()
				}
			}
		}(i)
	}
}

// safeCall was added so that when we kill a process and add it back in, it will still be connected by the other nodes through the new channel, instead of
// only connected through the old channel
func safeCall(server *ServerConnection, serviceMethod string, args any, reply any) error {
	err := server.rpcConnection.Call(serviceMethod, args, reply)
	if err != nil {
		log.Printf("RPC call to %s failed for server %d: %v. Trying to reconnect...", serviceMethod, server.serverID, err)
		newClient, connErr := rpc.DialHTTP("tcp", server.Address)
		if connErr != nil {
			log.Printf("Reconnection to server %d at %s failed: %v", server.serverID, server.Address, connErr)
			return err // still return the original error
		}
		server.rpcConnection = newClient
		log.Printf("Reconnected to server %d at %s", server.serverID, server.Address)
		// try again
		return server.rpcConnection.Call(serviceMethod, args, reply)
	}
	return nil
}

// You may use this function to help with handling the periodic heartbeats
// Hint: Use this only if the node is a leader
func Heartbeat() {
	mutex.Lock()
	if state != "Leader" {
		mutex.Unlock()
		return
	}

	term := currentTerm
	leaderId := selfID
	mutex.Unlock()

	for i, server := range serverNodes {
		go func(serverIndex int, server ServerConnection) {
			mutex.Lock()

			if state != "Leader" || currentTerm != term {
				mutex.Unlock()
				return
			}

			prevLogIndex := nextIndex[serverIndex] - 1
			prevLogTerm := 0

			if prevLogIndex > 0 && prevLogIndex <= len(log_entries) {
				prevLogTerm = log_entries[prevLogIndex-1].Term
			}

			// entries to send
			entries := make([]LogEntry, 0)
			if len(log_entries) > prevLogIndex {
				entries = append(entries, log_entries[prevLogIndex:]...)
			}

			args := AppendEntryArgument{
				Term:         term,
				LeaderID:     leaderId,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      entries,
				LeaderCommit: commitIndex,
			}

			mutex.Unlock()

			var reply AppendEntryReply
			err := safeCall(&serverNodes[serverIndex], "RaftNode.AppendEntry", args, &reply) //use safeCall
			if err != nil {
				log.Printf("Error sending AppendEntry to server %d: %v", serverIndex, err)
				return
			}

			mutex.Lock()
			defer mutex.Unlock()

			if state != "Leader" || currentTerm != term {
				return
			}

			if reply.Term > currentTerm {
				// Step down to follower if the reply term is higher than the leader term
				currentTerm = reply.Term
				state = "Follower"
				votedFor = -1
				log.Printf("Found higher term %d, stepping down to follower\n", reply.Term)
				return
			}

			if reply.Success {
				if len(entries) > 0 {
					nextIndex[serverIndex] = prevLogIndex + len(entries) + 1
					matchIndex[serverIndex] = prevLogIndex + len(entries)
					log.Printf("Server %d successfully replicated up to index %d\n",
						serverIndex, matchIndex[serverIndex])
				}
				updateCommitIndex()
			} else {
				if reply.NextIndex > 0 {
					nextIndex[serverIndex] = reply.NextIndex
				} else {
					// If log inconsistency exists, decrement nextIndex and retry
					if nextIndex[serverIndex] > 1 {
						nextIndex[serverIndex]--
					}
				}
				log.Printf("AppendEntries failed for server %d, will try index %d next\n",
					serverIndex, nextIndex[serverIndex])
			}
		}(i, server)
	}
}

// update commitIndex based on matchIndex
func updateCommitIndex() {
	if state != "Leader" {
		return
	}

	// For each possible index N from commitIndex+1 to last log index
	for N := commitIndex + 1; N <= len(log_entries); N++ {
		// Count the number of nodes with matchIndex >= N
		count := 1
		for i := range serverNodes {
			if matchIndex[i] >= N {
				count++
			}
		}

		// If majority of nodes have replicated entry at index N and entry is from current term, commit it
		if count > (len(serverNodes)+1)/2 && log_entries[N-1].Term == currentTerm {
			commitIndex = N
			log.Printf("Committed log entry at index %d\n", N)

			// Apply newly committed entries
			for lastApplied < commitIndex {
				lastApplied++
				applyEntry(log_entries[lastApplied-1])
				// Apply log_entries[lastApplied-1]
				log.Printf("Applied entry %d: term: %d, index: %d\n", lastApplied, log_entries[lastApplied-1].Term, log_entries[lastApplied-1].Index)
			}
		}
	}
}

// This function is designed to emulate a client reaching out to the server. Note that many of the realistic details are removed, for simplicity
func (r *RaftNode) ClientAddToLog(args ClientArguments, reply *ClientReply) error {
	mutex.Lock()
	defer mutex.Unlock()
	if state != "Leader" {
		reply.Success = false
		if leaderID >= 0 && leaderID < len(serverNodes) {
			reply.LeaderAddress = serverNodes[leaderID].Address
			log.Printf("sending client the leader address %s", reply.LeaderAddress)
		} else {
			reply.LeaderAddress = ""
			log.Println("leader still undetermined")
		}
		return nil
	}

	var command Command

	if args.CommandType == "R" {
		command = Command{
			Operation: "GET",
			Key:       args.EntityID,
			Value:     "",
			FileType:  args.EntityType,
		}
		var value string
		var in bool
		if command.FileType == "student" {
			value, in = student_key_value[args.EntityID]
		}
		if command.FileType == "course" {
			value, in = course_key_value[args.EntityID]
		}
		if command.FileType == "enrollment" {
			value, in = enrollment_key_value[args.EntityID]
		}
		if in {
			reply.Content = value
			reply.Success = true
		} else {
			reply.Content = ""
			reply.Success = true
		}
	}

	if args.CommandType == "W" {
		command = Command{
			Operation: "PUT",
			Key:       args.EntityID,
			Value:     args.Data,
			FileType:  args.EntityType,
		}

		newIndex := 1
		if len(log_entries) > 0 {
			newIndex = log_entries[len(log_entries)-1].Index + 1
		}

		entry := LogEntry{
			Index:   newIndex,
			Term:    currentTerm,
			Command: command,
		}

		log_entries = append(log_entries, entry)
		log.Println("client sent a new log entry with index " + strconv.Itoa(newIndex))
		mutex.Unlock()
		mutex.Lock()
		reply.Success = true
		reply.LeaderAddress = serverNodes[selfID].Address
	}
	return nil

}

func load(file string) error {
	fileName := "data" + file + ".json"
	data, err := os.ReadFile(fileName)
	if err != nil {
		return err
	}
	if file == "student" {
		var students []Student
		if err := json.Unmarshal(data, &students); err != nil {
			log.Println(err)
			return err
		}
		for _, student := range students {
			key := "student_" + strconv.Itoa(student.StudentID)
			val, _ := json.Marshal(student)
			student_key_value[key] = string(val)
		}
	} else if file == "course" {
		var courses []Course
		if err := json.Unmarshal(data, &courses); err != nil {
			log.Println(err)
			return err
		}
		for _, course := range courses {
			key := "course_" + strconv.Itoa(course.CourseID)
			val, _ := json.Marshal(course)
			course_key_value[key] = string(val)
		}
	} else if file == "enrollment" {
		var enrollments []Enrollment
		if err := json.Unmarshal(data, &enrollments); err != nil {
			log.Println(err)
			return err
		}
		for _, enrollment := range enrollments {
			key := "enrollment_" + strconv.Itoa(enrollment.EnrollmentID)
			val, _ := json.Marshal(enrollment)
			enrollment_key_value[key] = string(val)
		}
	}

	log.Printf("have completed loading data for file %s\n", file)
	return nil

}

func sendHeartbeat() {
	for {
		time.Sleep(time.Duration(50) * time.Millisecond)
		mutex.Lock()
		if leaderID == selfID {
			mutex.Unlock()
			Heartbeat()
		} else {
			mutex.Unlock()
		}
	}
}

func resetElectionTimer() {
	mutex.Lock()
	defer mutex.Unlock()

	timeout := time.Duration(100+rand.Intn(401)) * time.Millisecond

	if electionTimer != nil {
		electionTimer.Stop()
	}

	electionTimer = time.AfterFunc(timeout, func() {
		log.Printf("Election timeout after %v ms\n", timeout.Milliseconds())
		LeaderElection()
	})
}

func main() {
	// The assumption here is that the command line arguments will contain:
	// This server's ID (zero-based), location and name of the cluster configuration file
	arguments := os.Args
	if len(arguments) == 1 {
		fmt.Println("Please provide cluster information.")
		return
	}

	// Read the values sent in the command line

	// Get this sever's ID (same as its index for simplicity)
	myID, err := strconv.Atoi(arguments[1])
	// Get the information of the cluster configuration file containing information on other servers
	file, err := os.Open(arguments[2])
	if err != nil {
		log.Fatal(err)
	}
	selfID = myID
	defer file.Close()

	myPort = "localhost"

	// Read the IP:port info from the cluster configuration file
	scanner := bufio.NewScanner(file)
	lines := make([]string, 0)
	index := 0
	for scanner.Scan() {
		// Get server IP:port
		text := scanner.Text()
		log.Printf(text, index)
		if index == myID {
			myPort = text
		}
		// Save that information as a string for now
		lines = append(lines, text)
		index++
	}
	// If anything wrong happens with reading the file, simply exit
	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}

	// Following lines are to register the RPCs of this object of type RaftNode
	api := new(RaftNode)
	err = rpc.Register(api)
	if err != nil {
		log.Fatal("error registering the RPCs", err)
	}
	rpc.HandleHTTP()
	go http.ListenAndServe(myPort, nil)
	log.Printf("serving rpc on port" + myPort)

	// Connect to other servers
	for index, element := range lines {
		// Attempt to connect to the other server node
		client, err := rpc.DialHTTP("tcp", element)
		// If connection is not established
		for err != nil {
			// Record it in log
			log.Println("Trying again. Connection error: ", err)
			// Try again!
			client, err = rpc.DialHTTP("tcp", element)
		}
		// Once connection is finally established
		// Save that connection information in the servers list
		serverNodes = append(serverNodes, ServerConnection{index, element, client})
		// Record that in log
		fmt.Println("Connected to " + element)
	}

	// Once all the connections are established, we can start the typical operations within Raft
	// Leader election and heartbeats are concurrent and non-stop in Raft

	// HINT 1: You may need to start a thread here (or more, based on your logic)
	// Hint 2: Main process should never stop
	// Hint 3: After this point, the threads should take over
	// Heads up: they never will be done
	// Hint 4: wg.Wait() might be helpful here
	load("student")
	load("course")
	load("enrollment")
	rand.Seed(time.Now().UnixNano() + int64(selfID))

	var wg sync.WaitGroup
	wg.Add(2)
	// Start the election timeout checking loop
	go func() {
		defer wg.Done()
		fmt.Print("checkpoint: in go func() before resetElectionTimer\n")
		resetElectionTimer()
		fmt.Print("checkpoint: in go func() after resetElectionTimer \n")
	}()
	fmt.Print("checkpoint: after first go func() \n")
	// Start the heartbeat loop (will only send if we're leader)
	go func() {
		defer wg.Done()
		sendHeartbeat()
	}()

	// Wait for goroutines to complete (they won't in practice)
	wg.Wait()
}
