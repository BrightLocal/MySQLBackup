package main

import (
	"compress/gzip"
	"fmt"
	"log"
	"os"
)

var line = []byte("abcdefghijklmnopqrstuvwxyz0123456789abcdefghijklmnopqrstuvwxyz0123456789abcdefghijklmnopqrstuvwxyz0123456789\n")

func main() {
	workers := 30
	done := make(chan struct{})
	for k := 0; k < workers; k++ {
		go func(k int) {
			if err := writeFile(k, done); err != nil {
				log.Fatalf("Error writing: %s", err)
			}
		}(k)
	}
	for d := 0; d < workers; d++ {
		<-done
	}
}

func writeFile(n int, done chan struct{}) error {
	log.Printf("Starting worker %d", n)
	defer log.Printf("Finishing worker %d", n)
	defer func() { done <- struct{}{} }()
	fileName := fmt.Sprintf("%00d.gz", n)
	f, err := os.Create(fileName)
	if err != nil {
		return err
	}
	defer f.Close()
	gzWriter := gzip.NewWriter(f)
	defer gzWriter.Close()
	for i := 0; i < 10000000; i++ {
		if _, err := gzWriter.Write(line); err != nil {
			return err
		}
	}
	return nil
}
