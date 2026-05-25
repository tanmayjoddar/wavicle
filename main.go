package main

import (
	"fmt"
	"log"
	"wavicle/internal/protocol/resp3"
	"wavicle/internal/storage"
)

func main() {
	fmt.Println("Wavicle Causal Proof Engine v1.0 starting...")

	crystal, err := storage.NewCausalCrystal("crystal.log")
	if err != nil {
		log.Fatalf("Failed to initialize Causal Crystal: %v", err)
	}

	server := resp3.NewServer(crystal)

	fmt.Println("RESP3 Server listening on :6379")
	if err := server.ListenAndServe(":6379"); err != nil {
		log.Fatalf("Server failed: %v", err)
	}
}
