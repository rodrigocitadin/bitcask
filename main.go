package main

import (
	"fmt"
	"log"
	"rodrigocitadin/bitcask/bitcask"
)

func main() {
	db, err := bitcask.Open("bitcask.db")
	if err != nil {
		log.Fatalf("Error opening database: %v", err)
	}
	defer db.Close()

	fmt.Println("Inserting values...")
	db.Put("user:1", []byte("Alice"))
	db.Put("user:2", []byte("Bob"))
	db.Put("user:3", []byte("Charlie"))

	fmt.Println("Available keys:", db.ListKeys())

	value, err := db.Get("user:2")
	if err != nil {
		log.Fatalf("Error getting user:2: %v", err)
	}
        fmt.Printf("user:2 value -> %s\n", value)

	fmt.Println("Removing user:1...")
	db.Delete("user:1")

	fmt.Println("Keys aftermath:", db.ListKeys())
}
