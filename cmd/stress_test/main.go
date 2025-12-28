package main

import (
	"math/rand"
	"encoding/json"
	"time"
	"sync"
	"fmt"
	"strings"
	"log"
	"github.com/gomodule/redigo/redis"
)

const (
	address = "localhost:6379"
)

func main() {
	conn, err := redis.Dial("tcp", address)
	if err != nil {
		log.Fatal("Could not connect to server")
	}
	defer conn.Close()

	fmt.Println("STARTING TEST SUITE")
	fmt.Println("-------------------------------------------")

	testBasicGetSet(conn)
	testConditionalSet(conn)
	testExpirationAndTtl(conn)
	testLargePayloads(conn)
	testThunderingHerd(conn)
	testJsonPayloadStress(conn)

	fmt.Println("-------------------------------------------")
	fmt.Println("TEST SUITE COMPLETE")
}

func assertStatus(testName string, expected string, got interface{}) {
	gotStr, _ := redis.String(got, nil)
	if gotStr == expected {
		fmt.Printf("[PASS] %-30s | Expected: %s, Got: %s\n", testName, expected, gotStr)
	} else {
		fmt.Printf("[FAIL] %-30s | Expected: %s, Got: %s\n", testName, expected, gotStr)
	}
}

func assertInt(testName string, expected int64, got int64) {
	if expected == got {
		fmt.Printf("[PASS] %-30s | Expected: %d, Got: %d\n", testName, expected, got)
	} else {
		fmt.Printf("[FAIL] %-30s | Expected: %d, Got: %d\n", testName, expected, got)
	}
}

func testBasicGetSet(c redis.Conn) {
	fmt.Println("\nBASIC OPERATIONS")
	
	// Test SET and GET
	c.Do("SET", "key1", "val1")
	val, _ := redis.String(c.Do("GET", "key1"))
	assertStatus("Basic SET and GET", "val1", val)
}

func testConditionalSet(c redis.Conn) {
	fmt.Println("\nCONDITIONAL SET (NX XX)")
	c.Do("DEL", "key2")

	// NX: Only set if not exists
	res1, _ := c.Do("SET", "key2", "newval", "NX")
	assertStatus("SET NX on empty key", "OK", res1)

	res2, _ := c.Do("SET", "key2", "blocked", "NX")
	if res2 == nil {
		fmt.Printf("[PASS] %-30s | Expected: nil, Got: nil\n", "SET NX on existing key")
	} else {
		fmt.Printf("[FAIL] %-30s | Expected: nil, Got: exists\n", "SET NX on existing key")
	}

	// XX: Only set if exists
	c.Do("DEL", "key3")
	res3, _ := c.Do("SET", "key3", "wontwork", "XX")
	if res3 == nil {
		fmt.Printf("[PASS] %-30s | Expected: nil, Got: nil\n", "SET XX on empty key")
	} else {
		fmt.Printf("[FAIL] %-30s | Expected: nil, Got: set\n", "SET XX on empty key")
	}
}

func testExpirationAndTtl(c redis.Conn) {
	fmt.Println("\nEXPIRATION AND TTL")
	
	// Test EX (Seconds)
	c.Do("SET", "key4", "expiring", "EX", "10")
	ttl, _ := redis.Int64(c.Do("TTL", "key4"))
	if ttl > 0 && ttl <= 10 {
		fmt.Printf("[PASS] %-30s | TTL is active\n", "SET EX 10")
	} else {
		fmt.Printf("[FAIL] %-30s | TTL invalid: %d\n", "SET EX 10", ttl)
	}

	// Test PX (Milliseconds)
	c.Do("SET", "key5", "pxval", "PX", "5000")
	pttl, _ := redis.Int64(c.Do("PTTL", "key5"))
	if pttl > 0 && pttl <= 5000 {
		fmt.Printf("[PASS] %-30s | PTTL is active\n", "SET PX 5000")
	} else {
		fmt.Printf("[FAIL] %-30s | PTTL invalid: %d\n", "SET PX 5000", pttl)
	}

	// Test EXPIRE command
	c.Do("SET", "key6", "manual")
	c.Do("EXPIRE", "key6", "20")
	ttl6, _ := redis.Int64(c.Do("TTL", "key6"))
	assertInt("EXPIRE command", 20, ttl6)

	// Test PERSIST
	c.Do("PERSIST", "key6")
	ttlPersist, _ := redis.Int64(c.Do("TTL", "key6"))
	assertInt("PERSIST (TTL should be -1)", -1, ttlPersist)

	// Test EXPIRE with NX (Only if no expiry set)
	// Note: EXPIRE key seconds [NX|XX|GT|LT] is Redis 7.0+
	c.Do("SET", "key7", "val7")
	c.Do("EXPIRE", "key7", "30", "NX")
	ttl7, _ := redis.Int64(c.Do("TTL", "key7"))
	assertInt("EXPIRE NX on new key", 30, ttl7)
}


// testLargePayloads tests the server's ability to handle very long bulk strings.
func testLargePayloads(c redis.Conn) {
	fmt.Println("\nLARGE PAYLOAD OPERATIONS (1MB & 5MB)")
	fmt.Println("-------------------------------------------")

	// 1. Create a 1MB string (1,048,576 characters)
	// Using strictly alphanumerical 'A'
	size1MB := 1024 * 1024
	longValue1MB := strings.Repeat("A", size1MB)
	longKey1 := "largekey1MB"

	// Test SET
	res1, err := c.Do("SET", longKey1, longValue1MB)
	if err != nil {
		fmt.Printf("[FAIL] SET 1MB key | Error: %v\n", err)
	} else {
		assertStatus("SET 1MB key", "OK", res1)
	}

	// Test GET and verify length
	got1, _ := redis.String(c.Do("GET", longKey1))
	if len(got1) == size1MB {
		fmt.Printf("[PASS] GET 1MB key             | Length matches: %d\n", len(got1))
	} else {
		fmt.Printf("[FAIL] GET 1MB key             | Expected length: %d, Got: %d\n", size1MB, len(got1))
	}

	// 2. Create a 5MB string (5,242,880 characters)
	// Using strictly alphanumerical 'B'
	size5MB := 5 * 1024 * 1024
	longValue5MB := strings.Repeat("B", size5MB)
	longKey2 := "largekey5MB"

	// Test SET
	res2, err := c.Do("SET", longKey2, longValue5MB)
	if err != nil {
		fmt.Printf("[FAIL] SET 5MB key | Error: %v\n", err)
	} else {
		assertStatus("SET 5MB key", "OK", res2)
	}

	// Test GET and verify length
	got2, _ := redis.String(c.Do("GET", longKey2))
	if len(got2) == size5MB {
		fmt.Printf("[PASS] GET 5MB key             | Length matches: %d\n", len(got2))
	} else {
		fmt.Printf("[FAIL] GET 5MB key             | Expected length: %d, Got: %d\n", size5MB, len(got2))
	}
	
	// Cleanup
	c.Do("DEL", longKey1, longKey2)
}


const (
	addr            = "localhost:6379"
	thunderingCount = 10000 // 10k concurrent clients
)

func testThunderingHerd(masterConn redis.Conn) {
	fmt.Printf("\n🔥 STARTING THUNDERING HERD TEST (%d Clients)\n", thunderingCount)
	fmt.Println("-------------------------------------------")

	theKey := "herdkey1"
	masterConn.Do("DEL", theKey)

	var wg sync.WaitGroup
	start := time.Now()

	// Error tracking
	var errCount int
	var mu sync.Mutex

	for i := 0; i < thunderingCount; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			
			// Open a fresh connection per "client"
			c, err := redis.Dial("tcp", addr)
			if err != nil {
				mu.Lock()
				errCount++
				mu.Unlock()
				return
			}
			defer c.Close()

			if id%2 == 0 {
				// Half the clients try to SET
				c.Do("SET", theKey, "iamwinner")
			} else {
				// Half the clients try to DELETE
				c.Do("DEL", theKey)
			}
		}(i)
	}

	wg.Wait()
	duration := time.Since(start)

	// Final check to see if the server is still responsive
	pong, err := redis.String(masterConn.Do("PING"))
	
	fmt.Println("-------------------------------------------")
	if err == nil && pong == "PONG" {
		fmt.Printf("[PASS] Server survived the herd in %v\n", duration)
	} else {
		fmt.Printf("[FAIL] Server is unresponsive or crashed! Error: %v\n", err)
	}

	if errCount > 0 {
		fmt.Printf("[WARN] %d clients failed to even connect. Check OS file limits (ulimit).\n", errCount)
	} else {
		fmt.Println("[PASS] All 10,000 clients connected and executed successfully.")
	}
}


const (
	userCount    = 1000
	clientCount  = 1000
	redisAddress = "localhost:6379"
)

// UserData represents a "bloated" user profile
type UserData struct {
	ID        int      `json:"id"`
	Username  string   `json:"username"`
	Email     string   `json:"email"`
	Roles     []string `json:"roles"`
	Bio       string   `json:"bio"`
	Metadata  map[string]string `json:"metadata"`
	IsActive  bool     `json:"isActive"`
}

func generateBloatedUser(id int) string {
	user := UserData{
		ID:       id,
		Username: fmt.Sprintf("user%d", id),
		Email:    fmt.Sprintf("user%d@example.com", id),
		Roles:    []string{"admin", "editor", "viewer", "moderator"},
		Bio:      "This is a long bio intended to bloat the JSON object for stress testing purposes. " + 
		           "It contains repeated data to increase the payload size significantly.",
		Metadata: map[string]string{
			"lastLogin": time.Now().String(),
			"region":    "Uzbekistan",
			"ua":        "Mozilla50 AppleWebkit53736 Chrome120",
		},
		IsActive: true,
	}
	data, _ := json.Marshal(user)
	return string(data)
}

func testJsonPayloadStress(masterConn redis.Conn) {
	fmt.Printf("\n💎 STARTING JSON BLOAT TEST (%d Users, %d Clients)\n", userCount, clientCount)
	fmt.Println("-------------------------------------------")

	var wg sync.WaitGroup
	start := time.Now()

	// 1. Initial Seeding: Set 1000 unique users
	fmt.Print("Phase 1: Seeding JSON data... ")
	for i := 0; i < userCount; i++ {
		key := fmt.Sprintf("user%d", i)
		val := generateBloatedUser(i)
		masterConn.Do("SET", key, val)
	}
	fmt.Println("Done.")

	// 2. High Concurrency Access
	fmt.Print("Phase 2: 1000 clients fighting for GET/SET... ")
	for i := 0; i < clientCount; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			c, err := redis.Dial("tcp", redisAddress)
			if err != nil {
				return
			}
			defer c.Close()

			// Each client picks a random user to GET and a random user to update (SET)
			targetGet := rand.Intn(userCount)
			targetSet := rand.Intn(userCount)

			// Execute GET
			_, _ = c.Do("GET", fmt.Sprintf("user%d", targetGet))

			// Execute SET (Update with new "lastLogin")
			newVal := generateBloatedUser(targetSet)
			_, _ = c.Do("SET", fmt.Sprintf("user%d", targetSet), newVal)
		}(i)
	}

	wg.Wait()
	duration := time.Since(start)

	// 3. Final Verification
	finalVal, _ := redis.String(masterConn.Do("GET", "user500"))
	
	fmt.Println("\n-------------------------------------------")
	if len(finalVal) > 0 {
		fmt.Printf("✅ SUCCESS: Server handled JSON traffic in %v\n", duration)
		fmt.Printf("✅ Sample Data Size: %d bytes\n", len(finalVal))
	} else {
		fmt.Println("❌ FAIL: Data loss or server unresponsive.")
	}
}

