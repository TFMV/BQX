package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/storage"
)

// GCSMessage is the payload of a GCS event.
type GCSMessage struct {
	Name   string `json:"name"`
	Bucket string `json:"bucket"`
}

func GCSHandler(w http.ResponseWriter, r *http.Request) {
	ctx := context.Background()
	var m GCSMessage

	// Parse the request body
	if err := json.NewDecoder(r.Body).Decode(&m); err != nil {
		log.Printf("json.NewDecoder: %v", err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	log.Printf("Processing file: %s from bucket: %s", m.Name, m.Bucket)

	// Process the file
	if err := processFile(ctx, m.Bucket, m.Name); err != nil {
		log.Printf("processFile: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	fmt.Fprintln(w, "File processed successfully")
}

func processFile(ctx context.Context, bucket, name string) error {
	// Initialize the GCS client
	client, err := storage.NewClient(ctx)
	if err != nil {
		return fmt.Errorf("storage.NewClient: %v", err)
	}
	defer client.Close()

	// Initialize the BigQuery client
	bqClient, err := bigquery.NewClient(ctx, os.Getenv("PROJECT_ID"))
	if err != nil {
		return fmt.Errorf("bigquery.NewClient: %v", err)
	}
	defer bqClient.Close()

	// Load the CSV data into BigQuery
	table := bqClient.Dataset("your_dataset").Table("tfmv")
	gcsRef := bigquery.NewGCSReference(fmt.Sprintf("gs://%s/%s", bucket, name))
	gcsRef.SourceFormat = bigquery.CSV

	loader := table.LoaderFrom(gcsRef)
	job, err := loader.Run(ctx)
	if err != nil {
		return fmt.Errorf("loader.Run: %v", err)
	}

	status, err := job.Wait(ctx)
	if err != nil {
		return fmt.Errorf("job.Wait: %v", err)
	}

	if err := status.Err(); err != nil {
		return fmt.Errorf("job status: %v", err)
	}

	return nil
}

func main() {
	http.HandleFunc("/", GCSHandler)
	log.Fatal(http.ListenAndServe(":8080", nil))
}
