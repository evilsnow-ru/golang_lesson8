package otus_lesson8

import (
	"errors"
	"fmt"
	"sync"
)

var (
	ErrIncorrectErrorsSize  = errors.New("max errors size must be greater then zero")
	ErrIncorrectWorkersSize = errors.New("workers size must be greater then zero")
	ErrIncorrectBufferSize  = errors.New("buffer size must be greater then zero")
	ErrIncorrectState       = errors.New("executor is closed")
)

type UnitOfWork func() error

type Executor interface {
	Closed() bool
	Shutdown(awaitFinish bool) bool
	Execute(tasks []UnitOfWork) error
}

type errorsAwareExecutor struct {
	lock          sync.RWMutex
	workers       int
	maxErrors     int
	closed        bool
	tasksChannel  chan UnitOfWork
	errorsChannel chan error
	controlChan   chan struct{}
	wg            sync.WaitGroup
}

func (e *errorsAwareExecutor) Closed() bool {
	e.lock.RLock()
	result := e.closed
	e.lock.RUnlock()
	return result
}

func (e *errorsAwareExecutor) Shutdown(awaitFinish bool) bool {
	if e.Closed() {
		return false
	}

	e.lock.Lock()

	//Состояние могло измениться из другой горутины, поэтому второй раз проверяем, но уже во WriteLock
	if e.closed {
		e.lock.Unlock()
		return false
	}

	e.closed = true
	e.lock.Unlock()

	close(e.tasksChannel)
	close(e.controlChan)

	if awaitFinish {
		e.wg.Wait()
	}

	return true
}

func (e *errorsAwareExecutor) Execute(tasks []UnitOfWork) error {
	e.lock.Lock()
	defer e.lock.Unlock()

	if e.closed {
		return ErrIncorrectState
	}

	for i := range tasks {
		e.tasksChannel <- tasks[i]
	}

	return nil
}

func NewUnbufferedExecutor(maxErrors, maxWorkers int) (Executor, error) {
	return newExecutor(maxErrors, maxWorkers, maxWorkers)
}

func NewBufferedExecutor(maxErrors, maxWorkers, bufferSize int) (Executor, error) {
	return newExecutor(maxErrors, maxWorkers, bufferSize)
}

func newExecutor(maxErrors, maxWorkers, buffersSize int) (Executor, error) {
	if err := validateParams(maxErrors, maxWorkers, buffersSize); err != nil {
		return nil, err
	}

	tasksChan := make(chan UnitOfWork, buffersSize)
	errorsChan := make(chan error, maxErrors)

	executor := &errorsAwareExecutor{
		workers:       maxWorkers,
		maxErrors:     maxErrors,
		tasksChannel:  tasksChan,
		errorsChannel: errorsChan,
	}

	startErrorsController(executor)
	executor.wg.Add(maxWorkers)

	for i := 0; i < maxWorkers; i++ {
		//Выделено в отдельную функцию, чтобы можно было менять логику независимо от текущей функции
		runNewWorker(executor)
	}

	return executor, nil
}

func startErrorsController(executor *errorsAwareExecutor) {
	go func(errorsChan <-chan error, maxErrorsCount int, controlChan <-chan struct{}, executorStopCallback func()) {
		var currentErrors int

	loop:
		for {
			select {
			case err, isOpen := <-errorsChan:
				if isOpen {
					currentErrors++
					fmt.Printf("Received new error (total: %d/%d): %s.\n", currentErrors, maxErrorsCount, err)

					if currentErrors > maxErrorsCount {
						fmt.Printf("Errors limit exceeded: %d. Stopping executor...\n", maxErrorsCount)
						executorStopCallback()
						break loop
					}

					continue
				}

				//If channel is closed
				break loop

			case <-controlChan:
				break loop
			}
		}
	}(executor.errorsChannel, executor.maxErrors, executor.controlChan, func() { executor.Shutdown(false) })
}

func runNewWorker(executor *errorsAwareExecutor) {
	go func(tasksChan <-chan UnitOfWork, errorsChan chan<- error) {
		for {
			task, isAlive := <-tasksChan

			if !isAlive {
				break
			} else {
				err := task()

				if err != nil {
					errorsChan <- err
				}
			}
		}
	}(executor.tasksChannel, executor.errorsChannel)
}

func validateParams(maxErrors, maxWorkers, buffersSize int) error {
	if maxErrors <= 0 {
		return ErrIncorrectErrorsSize
	}

	if maxWorkers <= 0 {
		return ErrIncorrectWorkersSize
	}

	if buffersSize <= 0 {
		return ErrIncorrectBufferSize
	}

	return nil
}
