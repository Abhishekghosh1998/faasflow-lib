package goflow

import (
	"fmt"
)

var (
	BLANK_MODIFIER = func(data []byte) ([]byte, error) { return data, nil }
)

// FuncErrorHandler the error handler for OnFailure() options
type FuncErrorHandler func(error) error

// Modifier definition for Modify() call
type Modifier func([]byte, map[string][]string) ([]byte, error)

type ServiceOperation struct {
	Id      string              // ID
	Mod     Modifier            // Modifier
	Options map[string][]string // The option as a input to workload

	FailureHandler FuncErrorHandler // The Failure handler of the operation
}

// createWorkload Create a function with execution name
func createWorkload(id string, mod Modifier) *ServiceOperation {

	fmt.Printf("lib/goflow/operation.go::createWorkload start")
	operation := &ServiceOperation{}
	operation.Mod = mod
	operation.Id = id
	operation.Options = make(map[string][]string)
	fmt.Printf("lib/goflow/operation.go::createWorkload end")
	return operation
}

func (operation *ServiceOperation) addOptions(key string, value string) {

	fmt.Printf("lib/goflow/operation.go::addOptions start")
	array, ok := operation.Options[key]
	if !ok {
		operation.Options[key] = make([]string, 1)
		operation.Options[key][0] = value
	} else {
		operation.Options[key] = append(array, value)
	}
	fmt.Printf("lib/goflow/operation.go::addOptions end")
}

func (operation *ServiceOperation) addFailureHandler(handler FuncErrorHandler) {

	fmt.Printf("lib/goflow/operation.go::addFailureHandler start")
	operation.FailureHandler = handler
	fmt.Printf("lib/goflow/operation.go::addFailureHandler end")
}

func (operation *ServiceOperation) GetOptions() map[string][]string {

	fmt.Printf("lib/goflow/operation.go::GetOptions start")
	fmt.Printf("lib/goflow/operation.go::GetOptions end")
	return operation.Options

}

func (operation *ServiceOperation) GetId() string {

	fmt.Printf("lib/goflow/operation.go::GetId start")
	fmt.Printf("lib/goflow/operation.go::GetId end")
	return operation.Id
}

func (operation *ServiceOperation) Encode() []byte {

	fmt.Printf("lib/goflow/operation.go::Encode start")
	fmt.Printf("lib/goflow/operation.go::Encode end")
	return []byte("")
}

// executeWorkload executes a function call
func executeWorkload(operation *ServiceOperation, data []byte) ([]byte, error) {

	fmt.Printf("lib/goflow/operation.go::executeWorkload start")
	var err error
	var result []byte

	options := operation.GetOptions()
	result, err = operation.Mod(data, options)

	fmt.Printf("lib/goflow/operation.go::executeWorkload end")
	return result, err
}

func (operation *ServiceOperation) Execute(data []byte, option map[string]interface{}) ([]byte, error) {

	fmt.Printf("lib/goflow/operation.go::Execute start")
	var result []byte
	var err error

	if operation.Mod != nil {
		result, err = executeWorkload(operation, data)
		if err != nil {
			err = fmt.Errorf("function(%s), error: function execution failed, %v",
				operation.Id, err)
			if operation.FailureHandler != nil {
				err = operation.FailureHandler(err)
			}
			if err != nil {
				return nil, err
			}
		}
	}

	fmt.Printf("lib/goflow/operation.go::Execute end")
	return result, nil
}

func (operation *ServiceOperation) GetProperties() map[string][]string {

	fmt.Printf("lib/goflow/operation.go::GetProperties start")
	result := make(map[string][]string)

	isMod := "false"
	isFunction := "false"
	isHttpRequest := "false"
	hasFailureHandler := "false"

	if operation.Mod != nil {
		isFunction = "true"
	}
	if operation.FailureHandler != nil {
		hasFailureHandler = "true"
	}

	result["isMod"] = []string{isMod}
	result["isFunction"] = []string{isFunction}
	result["isHttpRequest"] = []string{isHttpRequest}
	result["hasFailureHandler"] = []string{hasFailureHandler}

	fmt.Printf("lib/goflow/operation.go::GetProperties end")
	return result
}

// Apply adds a new workload to the given vertex
func (node *Node) Apply(id string, workload Modifier, opts ...Option) *Node {

	fmt.Printf("lib/goflow/operation.go::Apply start")
	newWorkload := createWorkload(id, workload)

	o := &Options{}
	for _, opt := range opts {
		o.reset()
		opt(o)
		if len(o.option) != 0 {
			for key, array := range o.option {
				for _, value := range array {
					newWorkload.addOptions(key, value)
				}
			}
		}
		if o.failureHandler != nil {
			newWorkload.addFailureHandler(o.failureHandler)
		}
	}

	node.unode.AddOperation(newWorkload)
	fmt.Printf("lib/goflow/operation.go::Apply end")
	return node
}
