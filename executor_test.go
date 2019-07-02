package otus_lesson8

import (
	"errors"
	"sync"
	"testing"
	"time"
)

func TestNewBufferedExecutor(t *testing.T) {
	testValidation(t)
	testExecuteClosedExecutor(t)
	testCloseByErrors(t)
	testDoWork(t)
}

func testValidation(t *testing.T) {
	_, err := NewUnbufferedExecutor(0, 1)

	if err == nil {
		t.Fatal("No validation for maxErrors param")
	}

	_, err = NewUnbufferedExecutor(1, 0)

	if err == nil {
		t.Fatal("No validation for maxWorkers param")
	}

	_, err = NewBufferedExecutor(0, 1, 1)

	if err == nil {
		t.Fatal("No validation for maxErrors param")
	}

	_, err = NewBufferedExecutor(1, 0, 1)

	if err == nil {
		t.Fatal("No validation for maxWorkers param")
	}

	_, err = NewBufferedExecutor(1, 1, 0)

	if err == nil {
		t.Fatal("No validation for maxWorkers param")
	}

}

func testExecuteClosedExecutor(t *testing.T) {
	errorFunc := func() error { return errors.New("some error") }
	executor, _ := NewUnbufferedExecutor(6, 6)
	success := executor.Shutdown(false)

	if !success {
		t.Fatal("Expected success result. Actual: false.")
	}

	tasks := []UnitOfWork{ errorFunc }
	err := executor.Execute(tasks)

	if err == nil {
		t.Fatal("Executing new tasks on closed executor must return error.")
	}
}

func testCloseByErrors(t *testing.T) {
	errorFunc := func() error { return errors.New("some error") }

	tasks := []UnitOfWork{
		errorFunc, errorFunc, errorFunc, errorFunc,
		errorFunc, errorFunc, errorFunc,
	}

	executor, err := NewUnbufferedExecutor(6, 6)

	if err != nil {
		t.Fatalf("Error initializing executor: %s", err)
	}

	err = executor.Execute(tasks)

	if err != nil {
		t.Fatalf("Error executing tasks: %s", err)
	}

	//Await 15 seconds
	<-time.After(time.Second * 15)

	if !executor.Closed() {
		t.Fatalf("Executor must be closed after 6 errors")
	}
}

func testDoWork(t *testing.T) {
	var wg sync.WaitGroup

	errorFunc := func() error {
		defer wg.Done()
		return errors.New("some error")
	}

	var lock sync.Mutex
	var i int

	workFunc := func() error {
		defer wg.Done()
		lock.Lock()
		i++
		lock.Unlock()
		return nil
	}

	tasks := []UnitOfWork{
		errorFunc, workFunc, workFunc, errorFunc,
		errorFunc, workFunc, workFunc, workFunc,
	}

	wg.Add(len(tasks))

	executor, err := NewBufferedExecutor(6, 6, len(tasks))

	if err != nil {
		t.Fatalf("Error initializing executor: %s", err)
	}

	err = executor.Execute(tasks)
	defer executor.Shutdown(false)

	waitChan := make(chan int)

	go func(result chan<- int, wg sync.WaitGroup) {
		wg.Wait()
		result <- 1
	}(waitChan, wg)

	var result bool

	select {
	case <-waitChan:
		result = true
	case <-time.After(time.Second * 30):
		result = false
	}

	if !result {
		t.Fatal("Execution error for 30 seconds")
	}

	lock.Lock()
	defer lock.Unlock()

	if i != 5 {
		t.Fatalf("Expected 5 works done. Actual: %d", i)
	}
}
