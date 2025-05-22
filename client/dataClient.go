// Team 4
// Your client is using your storage system to keep track of student and course enrollement.
// They need to store data about students and courses.
// So, there are two types of data items:
// 1- Students, in which each student is defined by their ID and name
// 2- Courses, in which each course is defined by its ID and name
// 3- The enrollement of the students in these courses. You have the freedom to save that info in any format you prefer.
// You can create a separate file to keep track of enrollements (similar to SQL databases)
// Or, you can store the IDs of the courses enrolled by a student into that student's profile
// Or, you can store the IDs of the students enrolled in a course into that course's profile

// The simple write operation is to add a new student, create a new course, or assign a student to a course.
// The simple read operation is to retrieve the information of a student or course.

package main

import (
	"bufio"
	"errors"
	"fmt"
	"log"
	"math/rand"
	netRPC "net/rpc"
	"os"
	"time"
)

// Object type to manage connections to the cluster
type SystemConnection struct {
	connection    netRPC.Client
	addressString string
}

var connectionPoint SystemConnection

type ClientArguments struct {
	EntityType  string // ex. "student", "course"
	CommandType string // "R" for read, "W" for write
	EntityID    string // ID of item to read / write
	Data        string // empty for reads; data that client wants to write
}

type ClientReply struct {
	Content       string // if "R" return this content to client, elif "W" return empty string for content
	Success       bool   // true only after data is applied to file and all logs are consistent
	LeaderAddress string // the IP:port of the true leader
}

type Response struct {
	Result string
}

func WriteData(args *ClientArguments, result *Response) error {
	// Prepare the variables for RPC call
	theArgs := ClientArguments{
		CommandType: "W",
		EntityType:  args.EntityType,
		EntityID:    args.EntityID,
		Data:        args.Data,
	}
	theReply := ClientReply{}

	// Make the RPC with appropriate arguments
	err := connectionPoint.connection.Call("RaftNode.ClientAddToLog", theArgs, &theReply)
	if err != nil {
		return fmt.Errorf("initial RPC error: %v", err)
	}

	// First check if node was indeed leader
	if theReply.Success {
		*result = Response{Result: "Success"}
		return nil
	}

	if theReply.LeaderAddress == "" {
		return errors.New("Couldn't get to leader")
	}

	log.Printf("Redirecting to new leader at %s", theReply.LeaderAddress)
	// Otherwise, renew connection object
	// Connect to the indicated leader node
	client, err := netRPC.DialHTTP("tcp", theReply.LeaderAddress)
	if err != nil {
		log.Fatal("Problem with dialing:", err)
	}
	// Save new connection information
	connectionPoint = SystemConnection{*client, theReply.LeaderAddress}

	// Try again
	// Make the RPC with appropriate arguments
	theReply = ClientReply{}
	err = connectionPoint.connection.Call("RaftNode.ClientAddToLog", theArgs, &theReply)
	if err != nil {
		return fmt.Errorf("RPC error after redirect: %v", err)
	}
	// First, check if node was indeed leader
	// By now, this should always be true
	log.Printf("DEBUG: Reply.Success=%v, Reply.LeaderAddress=%q", theReply.Success, theReply.LeaderAddress)
	if theReply.Success {
		*result = Response{Result: "Success"}
		return nil
	}
	// Otherwise, indicate an error
	return errors.New("still couldn't get to leader")
}

func ReadData(args *ClientArguments, result *Response) error {
	// Prepare the variables for RPC call
	theArgs := ClientArguments{
		EntityType:  args.EntityType,
		EntityID:    args.EntityID,
		CommandType: "R",
	}
	theReply := ClientReply{}

	// Make the RPC with appropriate arguments
	err := connectionPoint.connection.Call("RaftNode.ClientAddToLog", theArgs, &theReply)
	if err != nil {
		log.Printf("RPC error:", err)
	}

	// First check if node was indeed leader
	if theReply.Success {
		*result = Response{Result: theReply.Content}
		return nil
	}
	// Otherwise, renew connection object
	// Connect to the indicated leader node
	client, err := netRPC.DialHTTP("tcp", theReply.LeaderAddress)
	if err != nil {
		log.Fatal("Problem with dialing:", err)
	}
	// Save new connection information
	connectionPoint = SystemConnection{*client, theReply.LeaderAddress}

	// Try again
	// Make the RPC with appropriate arguments
	theReply = ClientReply{}
	err = connectionPoint.connection.Call("RaftNode.ClientAddToLog", theArgs, &theReply)
	if err != nil {
		log.Fatal("RPC error:", err)
	}
	// First, check if node was indeed leader
	// By now, this should always be true
	if theReply.Success {
		*result = Response{Result: theReply.Content}
		return nil
	}
	// Otherwise, indicate an error
	return errors.New("Couldn't get to leader")
}

func connectToNode(fileName string) {
	// --- Read the IP:port info from the cluster configuration file
	// Get the information of the cluster configuration file containing information on other servers
	file, err := os.Open(fileName)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	lines := make([]string, 0)
	index := 0
	for scanner.Scan() {
		// Get server IP:port
		text := scanner.Text()
		// Save that information as a string for now
		lines = append(lines, text)
		index++
	}
	// If anything wrong happens with reading the file, simply exit
	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}

	// Choose a random cluster node to connect to
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	IPAddress := lines[r.Intn(len(lines))]
	log.Println("Chose %s from cluster", IPAddress)

	// Connect to the chosen node
	client, err := netRPC.DialHTTP("tcp", IPAddress)
	if err != nil {
		log.Fatal("dialing:", err)
	}

	// Save connection information
	connectionPoint = SystemConnection{*client, IPAddress}
}
func testingStudent() {
	fmt.Println("testing student")

	student1 := ClientArguments{
		EntityType:  "student",
		CommandType: "W",
		EntityID:    "student_1",
		Data:        `{"studentID": 1, "name":"Joe Ye"}`,
	}

	student2 := ClientArguments{
		EntityType:  "student",
		CommandType: "W",
		EntityID:    "student_2",
		Data:        `{"studentID": 2, "name":"Cynthia Erivo"}`,
	}

	student3 := ClientArguments{
		EntityType:  "student",
		CommandType: "W",
		EntityID:    "student_3",
		Data:        `{"studentID": 3, "name":"Charlie Brown"}`,
	}

	student4 := ClientArguments{
		EntityType:  "student",
		CommandType: "W",
		EntityID:    "student_4",
		Data:        `{"studentID": 4, "name":"Tony Stark"}`,
	}

	student5 := ClientArguments{
		EntityType:  "student",
		CommandType: "W",
		EntityID:    "student_5",
		Data:        `{"studentID": 5, "name":"Peter Parker"}`,
	}

	student6 := ClientArguments{
		EntityType:  "student",
		CommandType: "W",
		EntityID:    "student_6",
		Data:        `{"studentID": 6, "name":"Mickey Mouse"}`,
	}

	student7 := ClientArguments{
		EntityType:  "student",
		CommandType: "W",
		EntityID:    "student_7",
		Data:        `{"studentID": 7, "name":"Evangeline Lilly"}`,
	}

	student8 := ClientArguments{
		EntityType:  "student",
		CommandType: "W",
		EntityID:    "student_8",
		Data:        `{"studentID": 8, "name":"Mario Cart"}`,
	}

	student9 := ClientArguments{
		EntityType:  "student",
		CommandType: "W",
		EntityID:    "student_9",
		Data:        `{"studentID": 9, "name":"Indira Gandhi"}`,
	}

	student10 := ClientArguments{
		EntityType:  "student",
		CommandType: "W",
		EntityID:    "student_10",
		Data:        `{"studentID": 10, "name":"Sandy Cheeks"}`,
	}

	var result Response

	if err := WriteData(&student1, &result); err != nil {
		fmt.Println(err)
	} else {
		fmt.Println("added student 1", result.Result)
	}

	if err := WriteData(&student2, &result); err != nil {
		fmt.Println(err)
	} else {
		fmt.Println("added student 2", result.Result)
	}

	if err := WriteData(&student3, &result); err != nil {
		fmt.Println(err)
	} else {
		fmt.Println("added student 3", result.Result)
	}

	if err := WriteData(&student4, &result); err != nil {
		fmt.Println(err)
	} else {
		fmt.Println("added student 4", result.Result)
	}

	if err := WriteData(&student5, &result); err != nil {
		fmt.Println(err)
	} else {
		fmt.Println("added student 5", result.Result)
	}

	if err := WriteData(&student6, &result); err != nil {
		fmt.Println(err)
	} else {
		fmt.Println("added student 6", result.Result)
	}

	if err := WriteData(&student7, &result); err != nil {
		fmt.Println(err)
	} else {
		fmt.Println("added student 7", result.Result)
	}

	if err := WriteData(&student8, &result); err != nil {
		fmt.Println(err)
	} else {
		fmt.Println("added student 8", result.Result)
	}

	if err := WriteData(&student9, &result); err != nil {
		fmt.Println(err)
	} else {
		fmt.Println("added student 9", result.Result)
	}

	if err := WriteData(&student10, &result); err != nil {
		fmt.Println(err)
	} else {
		fmt.Println("added student 10", result.Result)
	}

	//allow time for writes to process before read
	time.Sleep(200 * time.Millisecond)

	//read student 2
	readStudent1 := ClientArguments{
		EntityType:  "student",
		CommandType: "R",
		EntityID:    "student_2",
	}

	if err := ReadData(&readStudent1, &result); err != nil {
		fmt.Println(err)
	} else {
		fmt.Println("student 2:", result.Result)
	}

	//read student 7
	readStudent2 := ClientArguments{
		EntityType:  "student",
		CommandType: "R",
		EntityID:    "student_7",
	}

	if err := ReadData(&readStudent2, &result); err != nil {
		fmt.Println(err)
	} else {
		fmt.Println("student 7:", result.Result)
	}

	//read student 9
	readStudent3 := ClientArguments{
		EntityType:  "student",
		CommandType: "R",
		EntityID:    "student_9",
	}

	if err := ReadData(&readStudent3, &result); err != nil {
		fmt.Println(err)
	} else {
		fmt.Println("student 9:", result.Result)
	}

	//read student 10
	readStudent4 := ClientArguments{
		EntityType:  "student",
		CommandType: "R",
		EntityID:    "student_10",
	}

	if err := ReadData(&readStudent4, &result); err != nil {
		fmt.Println(err)
	} else {
		fmt.Println("student 10:", result.Result)
	}
}

// Helper function to test course operations
func testingCourse() {
	fmt.Println("testing course")

	// Add courses
	course1 := ClientArguments{
		EntityType:  "course",
		CommandType: "W",
		EntityID:    "course_CS343",
		Data:        `{"courseID": 343, "name":"Distributed Computing"}`,
	}

	course2 := ClientArguments{
		EntityType:  "course",
		CommandType: "W",
		EntityID:    "course_CS344",
		Data:        `{"courseID": 344, "name":"Deep Learning"}`,
	}

	course3 := ClientArguments{
		EntityType:  "course",
		CommandType: "W",
		EntityID:    "course_CS111",
		Data:        `{"courseID": 111, "name":"Computer Programming and Problem Solving"}`,
	}

	course4 := ClientArguments{
		EntityType:  "course",
		CommandType: "W",
		EntityID:    "course_CS220",
		Data:        `{"courseID": 220, "name":"Human-Computer Interaction"}`,
	}

	course5 := ClientArguments{
		EntityType:  "course",
		CommandType: "W",
		EntityID:    "course_CS242",
		Data:        `{"courseID": 242, "name":"Computer Networks"}`,
	}

	course6 := ClientArguments{
		EntityType:  "course",
		CommandType: "W",
		EntityID:    "course_CS230",
		Data:        `{"courseID": 230, "name":"Data Structures"}`,
	}

	course7 := ClientArguments{
		EntityType:  "course",
		CommandType: "W",
		EntityID:    "course_CS231",
		Data:        `{"courseID": 231, "name":"Design and Analysis of Algorithms"}`,
	}

	course8 := ClientArguments{
		EntityType:  "course",
		CommandType: "W",
		EntityID:    "course_CS235",
		Data:        `{"courseID": 235, "name":"Theory of Computation"}`,
	}

	course9 := ClientArguments{
		EntityType:  "course",
		CommandType: "W",
		EntityID:    "course_CS236",
		Data:        `{"courseID": 236, "name":"Spatial-Temporal Mechanism Design"}`,
	}

	course10 := ClientArguments{
		EntityType:  "course",
		CommandType: "W",
		EntityID:    "course_CS317",
		Data:        `{"courseID": 317, "name":"Mobile App Development"}`,
	}

	var result Response

	// Write course 1
	err := WriteData(&course1, &result)
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Println("added course 1:", result.Result)
	}

	// Write course 2
	err = WriteData(&course2, &result)
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Println("added course 2:", result.Result)
	}

	// Write course 3
	err = WriteData(&course3, &result)
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Println("added course 3:", result.Result)
	}

	// Write course 4
	err = WriteData(&course4, &result)
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Println("added course 4:", result.Result)
	}

	// Write course 5
	err = WriteData(&course5, &result)
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Println("added course 5:", result.Result)
	}

	// Write course 6
	err = WriteData(&course6, &result)
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Println("added course 6:", result.Result)
	}

	// Write course 7
	err = WriteData(&course7, &result)
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Println("added course 7:", result.Result)
	}

	// Write course 8
	err = WriteData(&course8, &result)
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Println("added course 8:", result.Result)
	}

	// Write course 9
	err = WriteData(&course9, &result)
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Println("added course 9:", result.Result)
	}

	// Write course 10
	err = WriteData(&course10, &result)
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Println("added course 10:", result.Result)
	}

	//allow time for writes to process before read
	time.Sleep(200 * time.Millisecond)

	// Read course data

	//read course 1
	readCourse1 := ClientArguments{
		EntityType:  "course",
		CommandType: "R",
		EntityID:    "course_CS343",
	}

	err = ReadData(&readCourse1, &result)
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Println("CS343 data:", result.Result)
	}

	//read course 2
	readCourse2 := ClientArguments{
		EntityType:  "course",
		CommandType: "R",
		EntityID:    "course_CS236",
	}

	err = ReadData(&readCourse2, &result)
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Println("CS236 data:", result.Result)
	}

	//read course 3
	readCourse3 := ClientArguments{
		EntityType:  "course",
		CommandType: "R",
		EntityID:    "course_CS242",
	}

	err = ReadData(&readCourse3, &result)
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Println("CS242 data:", result.Result)
	}

}

// Helper function to test enrollment operations
func testingEnrollment() {
	fmt.Println("testing enrollment")

	// Add enrollments (student to course)

	//2 students enrolled in CS343
	enrollment1 := ClientArguments{
		EntityType:  "enrollment",
		CommandType: "W",
		EntityID:    "s1_C343", // Student1 enrolls in course CS343
		Data:        `{"enrollmentID":1,"studentId":1,"courseId":343}`,
	}
	enrollment2 := ClientArguments{
		EntityType:  "enrollment",
		CommandType: "W",
		EntityID:    "s2_CS343",
		Data:        `{"enrollmentID":2,"studentId":2,"courseId":343}`,
	}

	//3 students enrolled in CS344
	enrollment3 := ClientArguments{
		EntityType:  "enrollment",
		CommandType: "W",
		EntityID:    "s3_CS344",
		Data:        `{"enrollmentID":3,"studentId":3,"courseId":344}`,
	}
	enrollment4 := ClientArguments{
		EntityType:  "enrollment",
		CommandType: "W",
		EntityID:    "s4_CS344",
		Data:        `{"enrollmentID":4,"studentId":4,"courseId":344}`,
	}
	enrollment5 := ClientArguments{
		EntityType:  "enrollment",
		CommandType: "W",
		EntityID:    "s5_CS344",
		Data:        `{"enrollmentID":5,"studentId":5,"courseId":344}`,
	}

	//4 students enrolled in CS230
	enrollment6 := ClientArguments{
		EntityType:  "enrollment",
		CommandType: "W",
		EntityID:    "s6_CS230",
		Data:        `{"enrollmentID":6,"studentId":6,"courseId":230}`,
	}
	enrollment7 := ClientArguments{
		EntityType:  "enrollment",
		CommandType: "W",
		EntityID:    "s7_CS230",
		Data:        `{"enrollmentID":7,"studentId":7,"courseId":230}`,
	}
	enrollment8 := ClientArguments{
		EntityType:  "enrollment",
		CommandType: "W",
		EntityID:    "s8_CS230",
		Data:        `{"enrollmentID":8,"studentId":8,"courseId":230}`,
	}
	enrollment9 := ClientArguments{
		EntityType:  "enrollment",
		CommandType: "W",
		EntityID:    "s9_CS230",
		Data:        `{"enrollmentID":9,"studentId":9,"courseId":230}`,
	}

	//1 student enrolled in CS236
	enrollment10 := ClientArguments{
		EntityType:  "enrollment",
		CommandType: "W",
		EntityID:    "s10_CS236",
		Data:        `{"enrollmentID":10,"studentId":10,"courseId":236}`,
	}

	var result Response

	err := WriteData(&enrollment1, &result)
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Println("added enrollment 1:", result.Result)
	}

	err = WriteData(&enrollment2, &result)
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Println("added enrollment 2:", result.Result)
	}

	err = WriteData(&enrollment3, &result)
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Println("added enrollment 3", result.Result)
	}

	err = WriteData(&enrollment4, &result)
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Println("added enrollment 4", result.Result)
	}

	err = WriteData(&enrollment5, &result)
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Println("added enrollment 5", result.Result)
	}

	err = WriteData(&enrollment6, &result)
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Println("added enrollment 6", result.Result)
	}

	err = WriteData(&enrollment7, &result)
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Println("added enrollment 7", result.Result)
	}

	err = WriteData(&enrollment8, &result)
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Println("added enrollment 8", result.Result)
	}

	err = WriteData(&enrollment9, &result)
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Println("added enrollment 9", result.Result)
	}

	err = WriteData(&enrollment10, &result)
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Println("added enrollment 10", result.Result)
	}

	//allow time for writes to process before read
	time.Sleep(200 * time.Millisecond)

	// Read enrollment data

	readEnrollment2 := ClientArguments{
		EntityType:  "enrollment",
		CommandType: "R",
		EntityID:    "s2_CS343",
	}

	err = ReadData(&readEnrollment2, &result)
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Println("enrollment s2_CS343:", result.Result)
	}

	readEnrollment3 := ClientArguments{
		EntityType:  "enrollment",
		CommandType: "R",
		EntityID:    "s3_CS344",
	}

	err = ReadData(&readEnrollment3, &result)
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Println("enrollment s4_CS344:", result.Result)
	}

	readEnrollment4 := ClientArguments{
		EntityType:  "enrollment",
		CommandType: "R",
		EntityID:    "s5_CS344",
	}

	err = ReadData(&readEnrollment4, &result)
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Println("enrollment s5_CS344:", result.Result)
	}
}

func main() {
	arguments := os.Args
	// The only value sent should be the cluster file
	if len(arguments) == 1 {
		fmt.Println("Please provide cluster information.")
		return
	}

	// Set up a connection to a random node in the cluster
	connectToNode(arguments[1])
	time.Sleep(2 * time.Second)
	// Here you can add code to test your system
	// Call ReadItem()
	// Call WriteData()
	fmt.Println("=== Starting Testing Process ===")

	// Run tests
	testingStudent()
	testingCourse()
	testingEnrollment()

	fmt.Println("\nCompleted all the testing cases!")

}
