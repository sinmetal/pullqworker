package main

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"

	taskqueue "google.golang.org/api/taskqueue/v1beta2"

	"golang.org/x/oauth2/google"
)

func main() {
	projectID, err := getProjectID()
	if err != nil {
		log.Fatalln(err.Error())
		os.Exit(1)
	}
	projectID = fmt.Sprintf("s~%s", projectID)
	log.Printf("projectID = %s", projectID)

	client, err := google.DefaultClient(context.Background(), taskqueue.TaskqueueScope)

	err = run(client, projectID, "pull-queue", 10, 60)
	if err != nil {
		log.Fatalln(err.Error())
		os.Exit(1)
	}

	log.Println("Done")
	os.Exit(0)
}

func run(client *http.Client, projectID string, queueName string, numTasks, leaseSecs int64) error {
	api, err := taskqueue.New(client)
	if err != nil {
		return fmt.Errorf("Unable to create Tasks service: %s", err.Error())
	}

	tasks, err := api.Tasks.Lease(projectID, queueName, numTasks, leaseSecs).Do()
	if err != nil {
		return fmt.Errorf("Lease Tasks: %s", err.Error())
	}

	for _, t := range tasks.Items {
		log.Printf("%s complete.\n", t.Id)
		err = api.Tasks.Delete(projectID, queueName, t.Id).Do()
		if err != nil {
			return fmt.Errorf("%s task delete miss. err = %s", t.Id, err.Error())
		}
	}

	return nil
}

func getProjectID() (string, error) {
	r, err := http.NewRequest("GET", "http://metadata.google.internal/computeMetadata/v1/project/project-id", nil)
	if err != nil {
		return "", err
	}
	r.Header.Set("Metadata-Flavor", "Google")

	client := http.DefaultClient
	res, err := client.Do(r)
	if err != nil {
		return "", err
	}
	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return "", err
	}
	return string(body), nil
}
