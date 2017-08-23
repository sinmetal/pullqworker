package main

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"sync"
	"time"

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

	for {
		err = run(client, projectID, "pull-queue", 10, 60)
		if err != nil {
			log.Fatalf("run failure %s", err.Error())
			os.Exit(1)
		}
		log.Print("loop...")
	}
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
	log.Printf("lease task len = %d", len(tasks.Items))

	var wg sync.WaitGroup
	for _, task := range tasks.Items {
		log.Printf("task %s, retryCount=%d", task.Id, task.RetryCount)
		wg.Add(1)
		go func(t *taskqueue.Task) {
			defer wg.Done()
			err = processTask(api, projectID, queueName, leaseSecs, t)
			if err != nil {
				log.Printf("processTask %s failure %s", t.Id, err.Error())
			}
		}(task)
	}

	wg.Wait()

	return nil
}

func processTask(api *taskqueue.Service, projectID string, queueName string, leaseSecs int64, task *taskqueue.Task) error {
	log.Printf("%s : %s : %s task ignite. leaseSecs = %d", projectID, queueName, task.Id, leaseSecs)

	chErr1 := make(chan error, 1)
	go func() {
		// TODO processing task
		// time.Sleep(time.Second * 20)

		log.Printf("%s task complete.\n", task.Id)
		err := api.Tasks.Delete(projectID, queueName, task.Id).Do()
		if err != nil {
			chErr1 <- fmt.Errorf("%s task delete miss. err = %s", task.Id, err.Error())
			return
		}
		chErr1 <- nil
	}()

	chErr2 := make(chan error, 1)
	chCancel := make(chan string, 1)
	go func(taskID string) {
		select {
		case <-chCancel:
			log.Printf("update %s task cancel!", taskID)
			return
		case <-time.After(time.Second * time.Duration(leaseSecs-10)):
			// 間に合わなさそうだったら、地味に伸ばす
			log.Printf("%s : %s task extension lease time.", queueName, taskID)
			task.QueueName = queueName
			newTask, err := api.Tasks.Update(projectID, queueName, task.Id, leaseSecs, task).Do()
			if err != nil {
				chErr2 <- fmt.Errorf("queue %s task %s update failure : %s", queueName, taskID, err.Error())
			}
			log.Printf("%s new lease time set. new task = %v", taskID, newTask)
			return
		}
	}(task.Id)

	select {
	case err := <-chErr1:
		chCancel <- "cancel"
		if err != nil {
			return fmt.Errorf("task %s process failure : %s", task.Id, err.Error())
		}
		return nil
	case err := <-chErr2:
		if err != nil {
			return fmt.Errorf("task %s update failure : %s", task.Id, err.Error())
		}
		return nil
	}
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
