package main

import (
	"bufio"
	"fmt"
	"net"
	"path/filepath"
	"strings"
	"testing"
	"time"
	"wavicle/internal/protocol/resp3"
	"wavicle/internal/storage"
)

func TestEndToEnd(t *testing.T) {
	crystal, _ := storage.NewCausalCrystal(filepath.Join(t.TempDir(), "e2e_crystal.log"))
	defer crystal.Close()
	server := resp3.NewServer(crystal, "", 1000)
	defer server.Close()

	go server.ListenAndServe(":6380")
	time.Sleep(100 * time.Millisecond)

	conn, err := net.Dial("tcp", "localhost:6380")
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	fmt.Fprintf(conn, "SET mykey myvalue\r\n")
	scanner := bufio.NewScanner(conn)
	if scanner.Scan() {
		resp := scanner.Text()
		if resp != "+OK" {
			t.Errorf("Expected +OK, got %s", resp)
		}
	}

	fmt.Fprintf(conn, "GET mykey\r\n")
	if scanner.Scan() {
		resp := scanner.Text()
		if !strings.HasPrefix(resp, "$") {
			t.Errorf("Expected bulk string, got %s", resp)
		}
	}
	if scanner.Scan() {
		resp := scanner.Text()
		if !strings.Contains(resp, "myvalue") {
			t.Errorf("Expected response containing myvalue, got %s", resp)
		}
	}
}
