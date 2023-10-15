package openfaas

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"path"
	"strings"
)

var (
	BLANK_MODIFIER = func(data []byte) ([]byte, error) { return data, nil }
)

// FuncErrorHandler the error handler for OnFailure() options
type FuncErrorHandler func(error) error

// Modifier definition for Modify() call
type Modifier func([]byte) ([]byte, error)

// RespHandler definition for OnResponse() option on operation
type RespHandler func(*http.Response) ([]byte, error)

// Reqhandler definition for RequestHdlr() option on operation
type ReqHandler func(*http.Request)

type FaasOperation struct {
	// FaasOperations
	Function       string   // The name of the function
	HttpRequestUrl string   // HttpRequest Url
	Mod            Modifier // Modifier

	// Optional Options
	Header map[string]string   // The HTTP call header
	Param  map[string][]string // The Parameter in Query string

	FailureHandler FuncErrorHandler // The Failure handler of the operation
	Requesthandler ReqHandler       // The http request handler of the operation
	OnResphandler  RespHandler      // The http Resp handler of the operation
}

// createFunction Create a function with execution name
func createFunction(name string) *FaasOperation {

	fmt.Printf("lib/openfaas/faas_operation.go::createFunction start")
	operation := &FaasOperation{}
	operation.Function = name
	operation.Header = make(map[string]string)
	operation.Param = make(map[string][]string)
	fmt.Printf("lib/openfaas/faas_operation.go::createFunction end")
	return operation
}

// createModifier Create a modifier
func createModifier(mod Modifier) *FaasOperation {

	fmt.Printf("lib/openfaas/faas_operation.go::createModifier start")
	operation := &FaasOperation{}
	operation.Mod = mod
	fmt.Printf("lib/openfaas/faas_operation.go::createModifier end")
	return operation
}

// createHttpRequest Create a httpRequest
func createHttpRequest(url string) *FaasOperation {

	fmt.Printf("lib/openfaas/faas_operation.go::createHttpRequest start")
	operation := &FaasOperation{}
	operation.HttpRequestUrl = url
	operation.Header = make(map[string]string)
	operation.Param = make(map[string][]string)
	fmt.Printf("lib/openfaas/faas_operation.go::createHttpRequest end")
	return operation
}

func (operation *FaasOperation) addheader(key string, value string) {

	fmt.Printf("lib/openfaas/faas_operation.go::addheader start")
	lKey := strings.ToLower(key)
	operation.Header[lKey] = value
	fmt.Printf("lib/openfaas/faas_operation.go::addheader end")
}

func (operation *FaasOperation) addparam(key string, value string) {

	fmt.Printf("lib/openfaas/faas_operation.go::addparam start")
	array, ok := operation.Param[key]
	if !ok {
		operation.Param[key] = make([]string, 1)
		operation.Param[key][0] = value
	} else {
		operation.Param[key] = append(array, value)
	}
	fmt.Printf("lib/openfaas/faas_operation.go::addparam end")
}

func (operation *FaasOperation) addFailureHandler(handler FuncErrorHandler) {

	fmt.Printf("lib/openfaas/faas_operation.go::addFailureHandler start")
	operation.FailureHandler = handler
	fmt.Printf("lib/openfaas/faas_operation.go::addFailureHandler end")
}

func (operation *FaasOperation) addResponseHandler(handler RespHandler) {

	fmt.Printf("lib/openfaas/faas_operation.go::addResponseHandler start")
	operation.OnResphandler = handler
	fmt.Printf("lib/openfaas/faas_operation.go::addResponseHandler end")
}

func (operation *FaasOperation) addRequestHandler(handler ReqHandler) {

	fmt.Printf("lib/openfaas/faas_operation.go::addRequestHandler start")
	operation.Requesthandler = handler
	fmt.Printf("lib/openfaas/faas_operation.go::addRequestHandler end")
}

func (operation *FaasOperation) GetParams() map[string][]string {

	fmt.Printf("lib/openfaas/faas_operation.go::GetParams start")
	fmt.Printf("lib/openfaas/faas_operation.go::GetParams end")
	return operation.Param
}

func (operation *FaasOperation) GetHeaders() map[string]string {

	fmt.Printf("lib/openfaas/faas_operation.go::GetHeaders start")
	fmt.Printf("lib/openfaas/faas_operation.go::GetHeaders end")
	return operation.Header
}

func (operation *FaasOperation) GetId() string {

	fmt.Printf("lib/openfaas/faas_operation.go::GetId start")
	id := "modifier"
	switch {
	case operation.Function != "":
		id = operation.Function
	case operation.HttpRequestUrl != "":
		id = "http-req-" + operation.HttpRequestUrl[len(operation.HttpRequestUrl)-16:]
	}
	fmt.Printf("lib/openfaas/faas_operation.go::GetId end")
	return id
}

func (operation *FaasOperation) Encode() []byte {

	fmt.Printf("lib/openfaas/faas_operation.go::Encode start")
	fmt.Printf("lib/openfaas/faas_operation.go::Encode end")
	return []byte("")
}

// buildURL builds OpenFaaS function execution url for the flow
func buildURL(gateway, rPath, function string) string {

	fmt.Printf("lib/openfaas/faas_operation.go::buildURL start")
	u, _ := url.Parse(gateway)
	u.Path = path.Join(u.Path, rPath+"/"+function)
	fmt.Printf("lib/openfaas/faas_operation.go::buildURL end")
	return u.String()
}

// makeQueryStringFromParam create query string from provided query
func makeQueryStringFromParam(params map[string][]string) string {

	fmt.Printf("lib/openfaas/faas_operation.go::makeQueryStringFromParam start")
	if params == nil {
		return ""
	}
	result := ""
	for key, array := range params {
		for _, value := range array {
			keyVal := fmt.Sprintf("%s=%s", key, value)
			if result == "" {
				result = "?" + keyVal
			} else {
				result = result + "&" + keyVal
			}
		}
	}
	fmt.Printf("lib/openfaas/faas_operation.go::makeQueryStringFromParam end")
	return result
}

// buildHttpRequest build upstream request for function
func buildHttpRequest(url string, method string, data []byte, params map[string][]string,
	headers map[string]string) (*http.Request, error) {

	fmt.Printf("lib/openfaas/faas_operation.go::buildHttpRequest start")
	queryString := makeQueryStringFromParam(params)
	if queryString != "" {
		url = url + queryString
	}

	httpReq, err := http.NewRequest(method, url, bytes.NewReader(data))
	if err != nil {
		return nil, err
	}

	for key, value := range headers {
		httpReq.Header.Add(key, value)
	}

	fmt.Printf("lib/openfaas/faas_operation.go::buildHttpRequest end")
	return httpReq, nil
}

// executeFunction executes a function call
func executeFunction(gateway string, operation *FaasOperation, data []byte) ([]byte, error) {

	fmt.Printf("lib/openfaas/faas_operation.go::executeFunction start")
	var err error
	var result []byte

	name := operation.Function
	params := operation.GetParams()
	headers := operation.GetHeaders()

	funcUrl := buildURL("http://"+gateway, "function", name)

	method := os.Getenv("default-method")
	if method == "" {
		method = "POST"
	}

	if m, ok := headers["method"]; ok {
		method = m
	}

	httpReq, err := buildHttpRequest(funcUrl, method, data, params, headers)
	if err != nil {
		return []byte{}, fmt.Errorf("cannot connect to Function on URL: %s", funcUrl)
	}

	if operation.Requesthandler != nil {
		operation.Requesthandler(httpReq)
	}

	client := &http.Client{}
	resp, err := client.Do(httpReq)
	if err != nil {
		return []byte{}, err
	}

	defer resp.Body.Close()
	if operation.OnResphandler != nil {
		result, err = operation.OnResphandler(resp)
	} else {
		if resp.StatusCode < 200 || resp.StatusCode > 299 {
			err = fmt.Errorf("invalid return status %d while connecting %s", resp.StatusCode, funcUrl)
			result, _ = ioutil.ReadAll(resp.Body)
		} else {
			result, err = ioutil.ReadAll(resp.Body)
		}
	}
	fmt.Printf("lib/openfaas/faas_operation.go::executeFunction end")
	return result, err
}

// executeHttpRequest executes a httpRequest
func executeHttpRequest(operation *FaasOperation, data []byte) ([]byte, error) {

	fmt.Printf("lib/openfaas/faas_operation.go::executeHttpRequest start")
	var err error
	var result []byte

	httpUrl := operation.HttpRequestUrl
	params := operation.GetParams()
	headers := operation.GetHeaders()

	method := os.Getenv("default-method")
	if method == "" {
		method = "POST"
	}

	if m, ok := headers["method"]; ok {
		method = m
	}

	httpReq, err := buildHttpRequest(httpUrl, method, data, params, headers)
	if err != nil {
		return nil, fmt.Errorf("cannot connect to Function on URL: %s", httpUrl)
	}

	if operation.Requesthandler != nil {
		operation.Requesthandler(httpReq)
	}

	client := &http.Client{}
	resp, err := client.Do(httpReq)
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()
	if operation.OnResphandler != nil {
		_, err = operation.OnResphandler(resp)
	} else {
		if resp.StatusCode < 200 || resp.StatusCode > 299 {
			err = fmt.Errorf("invalid return status %d while connecting %s", resp.StatusCode, httpUrl)
			result, _ = ioutil.ReadAll(resp.Body)
		} else {
			result, err = ioutil.ReadAll(resp.Body)
		}
	}
	fmt.Printf("lib/openfaas/faas_operation.go::executeHttpRequest end")
	return result, err

}

func (operation *FaasOperation) Execute(data []byte, option map[string]interface{}) ([]byte, error) {

	fmt.Printf("lib/openfaas/faas_operation.go::Execute start")
	var result []byte
	var err error

	reqId := fmt.Sprintf("%v", option["request-id"])
	gateway := fmt.Sprintf("%v", option["gateway"])

	switch {
	// If function
	case operation.Function != "":
		fmt.Printf("[Request `%s`] Executing function `%s`\n",
			reqId, operation.Function)
		result, err = executeFunction(gateway, operation, data)
		if err != nil {
			err = fmt.Errorf("Function(%s), error: function execution failed, %v",
				operation.Function, err)
			if operation.FailureHandler != nil {
				err = operation.FailureHandler(err)
			}
			if err != nil {
				return nil, err
			}
		}

	// If httpRequest
	case operation.HttpRequestUrl != "":
		fmt.Printf("[Request `%s`] Executing httpRequest `%s`\n",
			reqId, operation.HttpRequestUrl)
		result, err = executeHttpRequest(operation, data)
		if err != nil {
			err = fmt.Errorf("HttpRequest(%s), error: httpRequest failed, %v",
				operation.HttpRequestUrl, err)
			if operation.FailureHandler != nil {
				err = operation.FailureHandler(err)
			}
			if err != nil {
				return nil, err
			}
		}
		if result == nil {
			result = []byte("")
		}

	// If modifier
	default:
		fmt.Printf("[Request `%s`] Executing modifier\n", reqId)
		result, err = operation.Mod(data)
		if err != nil {
			err = fmt.Errorf("error: Failed at modifier, %v", err)
			return nil, err
		}
		if result == nil {
			result = []byte("")
		}
	}
	fmt.Printf("lib/openfaas/faas_operation.go::Execute end")
	return result, nil
}

func (operation *FaasOperation) GetProperties() map[string][]string {

	fmt.Printf("lib/openfaas/faas_operation.go::GetProperties start")
	result := make(map[string][]string)

	isMod := "false"
	isFunction := "false"
	isHttpRequest := "false"
	hasFailureHandler := "false"
	hasResponseHandler := "false"

	if operation.Mod != nil {
		isMod = "true"
	}
	if operation.Function != "" {
		isFunction = "true"
	}
	if operation.HttpRequestUrl != "" {
		isHttpRequest = "true"
	}
	if operation.FailureHandler != nil {
		hasFailureHandler = "true"
	}
	if operation.OnResphandler != nil {
		hasResponseHandler = "true"
	}

	result["isMod"] = []string{isMod}
	result["isFunction"] = []string{isFunction}
	result["isHttpRequest"] = []string{isHttpRequest}
	result["hasFailureHandler"] = []string{hasFailureHandler}
	result["hasResponseHandler"] = []string{hasResponseHandler}

	fmt.Printf("lib/openfaas/faas_operation.go::GetProperties end")
	return result
}

// Faasflow faas operations

// Modify adds a new modifier to the given vertex
func (node *Node) Modify(mod Modifier) *Node {

	fmt.Printf("lib/openfaas/faas_operation.go::Modify start")
	newMod := createModifier(mod)
	node.unode.AddOperation(newMod)
	fmt.Printf("lib/openfaas/faas_operation.go::Modify end")
	return node
}

// Apply adds a new function to the given vertex
func (node *Node) Apply(function string, opts ...Option) *Node {

	fmt.Printf("lib/openfaas/faas_operation.go::Apply start")
	newfunc := createFunction(function)

	o := &Options{}
	for _, opt := range opts {
		o.reset()
		opt(o)
		if len(o.header) != 0 {
			for key, value := range o.header {
				newfunc.addheader(key, value)
			}
		}
		if len(o.query) != 0 {
			for key, array := range o.query {
				for _, value := range array {
					newfunc.addparam(key, value)
				}
			}
		}
		if o.failureHandler != nil {
			newfunc.addFailureHandler(o.failureHandler)
		}
		if o.responseHandler != nil {
			newfunc.addResponseHandler(o.responseHandler)
		}
		if o.requestHandler != nil {
			newfunc.addRequestHandler(o.requestHandler)
		}
	}

	node.unode.AddOperation(newfunc)
	fmt.Printf("lib/openfaas/faas_operation.go::Apply end")
	return node
}

// Request adds a new http Request to the given vertex
func (node *Node) Request(url string, opts ...Option) *Node {

	fmt.Printf("lib/openfaas/faas_operation.go::Request start")
	newHttpRequest := createHttpRequest(url)

	o := &Options{}
	for _, opt := range opts {
		o.reset()
		opt(o)
		if len(o.header) != 0 {
			for key, value := range o.header {
				newHttpRequest.addheader(key, value)
			}
		}
		if len(o.query) != 0 {
			for key, array := range o.query {
				for _, value := range array {
					newHttpRequest.addparam(key, value)
				}
			}
		}
		if o.failureHandler != nil {
			newHttpRequest.addFailureHandler(o.failureHandler)
		}
		if o.responseHandler != nil {
			newHttpRequest.addResponseHandler(o.responseHandler)
		}
		if o.requestHandler != nil {
			newHttpRequest.addRequestHandler(o.requestHandler)
		}
	}

	node.unode.AddOperation(newHttpRequest)
	fmt.Printf("lib/openfaas/faas_operation.go::Request end")
	return node
}
