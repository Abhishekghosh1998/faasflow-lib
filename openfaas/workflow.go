package openfaas

import (
	"fmt"

	sdk "github.com/Abhishekghosh1998/faasflow-sdk"
)

type Context sdk.Context
type StateStore sdk.StateStore
type DataStore sdk.DataStore

// Options options for operation execution
type Options struct {
	// Operation options
	header          map[string]string
	query           map[string][]string
	failureHandler  FuncErrorHandler
	requestHandler  ReqHandler
	responseHandler RespHandler
}

// BranchOptions options for branching in DAG
type BranchOptions struct {
	aggregator  sdk.Aggregator
	forwarder   sdk.Forwarder
	noforwarder bool
}

type Workflow struct {
	pipeline *sdk.Pipeline // underline pipeline definition object
}

type Dag struct {
	udag *sdk.Dag
}

type Node struct {
	unode *sdk.Node
}

type Option func(*Options)
type BranchOption func(*BranchOptions)

var (
	// Execution specify a edge doesn't forwards a data
	// but rather mention a execution direction
	Execution = InvokeEdge()
)

// reset reset the Options
func (o *Options) reset() {

	fmt.Println("lib/openfaas/workflow.go::Options::reset start")
	o.header = map[string]string{}
	o.query = map[string][]string{}
	o.failureHandler = nil
	o.requestHandler = nil
	o.responseHandler = nil
	fmt.Println("lib/openfaas/workflow.go::Options::reset end")
}

// reset reset the BranchOptions
func (o *BranchOptions) reset() {

	fmt.Println("lib/openfaas/workflow.go::BranchOptions::reset start")
	o.aggregator = nil
	o.noforwarder = false
	o.forwarder = nil
	fmt.Println("lib/openfaas/workflow.go::BranchOptions::reset end")
}

// Aggregator aggregates all outputs into one
func Aggregator(aggregator sdk.Aggregator) BranchOption {

	fmt.Println("lib/openfaas/workflow.go::Aggregator start")
	fmt.Println("lib/openfaas/workflow.go::Aggregator end")
	return func(o *BranchOptions) {
		o.aggregator = aggregator
	}
}

// InvokeEdge denotes a edge doesn't forwards a data,
// but rather provides only an execution flow
func InvokeEdge() BranchOption {

	fmt.Println("lib/openfaas/workflow.go::InvokeEdge start")
	fmt.Println("lib/openfaas/workflow.go::InvokeEdge end")
	return func(o *BranchOptions) {
		o.noforwarder = true
	}
}

// Forwarder encodes request based on need for children vertex
// by default the data gets forwarded as it is
func Forwarder(forwarder sdk.Forwarder) BranchOption {

	fmt.Println("lib/openfaas/workflow.go::Forwarder start")
	fmt.Println("lib/openfaas/workflow.go::Forwarder end")
	return func(o *BranchOptions) {
		o.forwarder = forwarder
	}
}

// Header Specify a header in a http call
func Header(key, value string) Option {

	fmt.Println("lib/openfaas/workflow.go::Header start")
	fmt.Println("lib/openfaas/workflow.go::Header end")
	return func(o *Options) {
		o.header[key] = value
	}
}

// Query Specify a query parameter in a http call
func Query(key string, value ...string) Option {

	fmt.Println("lib/openfaas/workflow.go::Query start")
	fmt.Println("lib/openfaas/workflow.go::Query end")
	return func(o *Options) {
		array := []string{}
		for _, val := range value {
			array = append(array, val)
		}
		o.query[key] = array
	}
}

// OnFailure Specify a function failure handler
func OnFailure(handler FuncErrorHandler) Option {

	fmt.Println("lib/openfaas/workflow.go::OnFailure start")
	fmt.Println("lib/openfaas/workflow.go::OnFailure end")
	return func(o *Options) {
		o.failureHandler = handler
	}
}

// RequestHandler Specify a request handler for function and callback request
func RequestHandler(handler ReqHandler) Option {

	fmt.Println("lib/openfaas/workflow.go::RequestHandler start")
	fmt.Println("lib/openfaas/workflow.go::RequestHandler end")
	return func(o *Options) {
		o.requestHandler = handler
	}
}

// OnResponse Specify a response handler for function and callback
func OnReponse(handler RespHandler) Option {

	fmt.Println("lib/openfaas/workflow.go::OnReponse start")
	fmt.Println("lib/openfaas/workflow.go::OnReponse end")
	return func(o *Options) {
		o.responseHandler = handler
	}
}

// GetWorkflow initiates a flow with a pipeline
func GetWorkflow(pipeline *sdk.Pipeline) *Workflow {

	fmt.Println("lib/openfaas/workflow.go::GetWorkflow start")
	workflow := &Workflow{}
	workflow.pipeline = pipeline
	fmt.Println("lib/openfaas/workflow.go::GetWorkflow end")
	return workflow
}

// OnFailure set a failure handler routine for the pipeline
func (flow *Workflow) OnFailure(handler sdk.PipelineErrorHandler) {

	fmt.Println("lib/openfaas/workflow.go::OnFailure start")
	flow.pipeline.FailureHandler = handler
	fmt.Println("lib/openfaas/workflow.go::OnFailure end")
}

// Finally sets an execution finish handler routine
// it will be called once the execution has finished with state either Success/Failure
func (flow *Workflow) Finally(handler sdk.PipelineHandler) {

	fmt.Println("lib/openfaas/workflow.go::Finally start")
	flow.pipeline.Finally = handler
	fmt.Println("lib/openfaas/workflow.go::Finally end")
}

// GetPipeline expose the underlying pipeline object
func (flow *Workflow) GetPipeline() *sdk.Pipeline {

	fmt.Println("lib/openfaas/workflow.go::GetPipeline start")
	fmt.Println("lib/openfaas/workflow.go::GetPipeline end")
	return flow.pipeline

}

// Dag provides the workflow dag object
func (flow *Workflow) Dag() *Dag {

	fmt.Println("lib/openfaas/workflow.go::Dag start")
	dag := &Dag{}
	dag.udag = flow.pipeline.Dag
	fmt.Println("lib/openfaas/workflow.go::Dag end")
	return dag
}

// SetDag apply a predefined dag, and override the default dag
func (flow *Workflow) SetDag(dag *Dag) {

	fmt.Println("lib/openfaas/workflow.go::SetDag start")
	pipeline := flow.pipeline
	pipeline.SetDag(dag.udag)
	fmt.Println("lib/openfaas/workflow.go::SetDag end")
}

// NewDag creates a new dag separately from pipeline
func NewDag() *Dag {

	fmt.Println("lib/openfaas/workflow.go::NewDag start")
	dag := &Dag{}
	dag.udag = sdk.NewDag()
	fmt.Println("lib/openfaas/workflow.go::NewDag end")
	return dag
}

// Append generalizes a seperate dag by appending its properties into current dag.
// Provided dag should be mutually exclusive
func (this *Dag) Append(dag *Dag) {

	fmt.Println("lib/openfaas/workflow.go::Append start")
	err := this.udag.Append(dag.udag)
	if err != nil {
		panic(fmt.Sprintf("Error at AppendDag, %v", err))
	}
	fmt.Println("lib/openfaas/workflow.go::Append end")
}

// Node adds a new vertex by id
func (this *Dag) Node(vertex string, options ...BranchOption) *Node {

	fmt.Println("lib/openfaas/workflow.go::Node start")
	node := this.udag.GetNode(vertex)
	if node == nil {
		node = this.udag.AddVertex(vertex, []sdk.Operation{})
	}
	o := &BranchOptions{}
	for _, opt := range options {
		o.reset()
		opt(o)
		if o.aggregator != nil {
			node.AddAggregator(o.aggregator)
		}
	}
	fmt.Println("lib/openfaas/workflow.go::Node end")
	return &Node{unode: node}
}

// Edge adds a directed edge between two vertex as <from>-><to>
func (this *Dag) Edge(from, to string, opts ...BranchOption) {

	fmt.Println("lib/openfaas/workflow.go::Edge start")
	err := this.udag.AddEdge(from, to)
	if err != nil {
		panic(fmt.Sprintf("Error at AddEdge for %s-%s, %v", from, to, err))
	}
	o := &BranchOptions{}
	for _, opt := range opts {
		o.reset()
		opt(o)
		if o.noforwarder == true {
			fromNode := this.udag.GetNode(from)
			// Add a nil forwarder overriding the default forwarder
			fromNode.AddForwarder(to, nil)
		}

		// in case there is a override
		if o.forwarder != nil {
			fromNode := this.udag.GetNode(from)
			fromNode.AddForwarder(to, o.forwarder)
		}
	}
	fmt.Println("lib/openfaas/workflow.go::Edge end")
}

// SubDag composites a seperate dag as a node.
func (this *Dag) SubDag(vertex string, dag *Dag) {

	fmt.Println("lib/openfaas/workflow.go::SubDag start")
	node := this.udag.AddVertex(vertex, []sdk.Operation{})
	err := node.AddSubDag(dag.udag)
	if err != nil {
		panic(fmt.Sprintf("Error at AddSubDag for %s, %v", vertex, err))
	}
	fmt.Println("lib/openfaas/workflow.go::SubDag end")
	return
}

// ForEachBranch composites a sub-dag which executes for each value
// It returns the sub-dag that will be executed for each value
func (this *Dag) ForEachBranch(vertex string, foreach sdk.ForEach, options ...BranchOption) (dag *Dag) {

	fmt.Println("lib/openfaas/workflow.go::ForEachBranch start")
	node := this.udag.AddVertex(vertex, []sdk.Operation{})
	if foreach == nil {
		panic(fmt.Sprintf("Error at AddForEachBranch for %s, foreach function not specified", vertex))
	}
	node.AddForEach(foreach)

	for _, option := range options {
		o := &BranchOptions{}
		o.reset()
		option(o)
		if o.aggregator != nil {
			node.AddSubAggregator(o.aggregator)
		}
		if o.noforwarder == true {
			node.AddForwarder("dynamic", nil)
		}
	}

	dag = NewDag()
	err := node.AddForEachDag(dag.udag)
	if err != nil {
		panic(fmt.Sprintf("Error at AddForEachBranch for %s, %v", vertex, err))
	}
	fmt.Println("lib/openfaas/workflow.go::ForEachBranch end")
	return
}

// ConditionalBranch composites multiple dags as a sub-dag which executes for a conditions matched
// and returns the set of dags based on the condition passed
func (this *Dag) ConditionalBranch(vertex string, conditions []string, condition sdk.Condition,
	options ...BranchOption) (conditiondags map[string]*Dag) {

	fmt.Println("lib/openfaas/workflow.go::ConditionalBranch start")
	node := this.udag.AddVertex(vertex, []sdk.Operation{})
	if condition == nil {
		panic(fmt.Sprintf("Error at AddConditionalBranch for %s, condition function not specified", vertex))
	}
	node.AddCondition(condition)

	for _, option := range options {
		o := &BranchOptions{}
		o.reset()
		option(o)
		if o.aggregator != nil {
			node.AddSubAggregator(o.aggregator)
		}
		if o.noforwarder == true {
			node.AddForwarder("dynamic", nil)
		}
	}
	conditiondags = make(map[string]*Dag)
	for _, conditionKey := range conditions {
		dag := NewDag()
		node.AddConditionalDag(conditionKey, dag.udag)
		conditiondags[conditionKey] = dag
	}
	fmt.Println("lib/openfaas/workflow.go::ConditionalBranch end")
	return
}

// AddOperation adds an Operation to the given vertex
func (node *Node) AddOperation(operation sdk.Operation) *Node {

	fmt.Println("lib/openfaas/workflow.go::AddOperation start")
	node.unode.AddOperation(operation)
	fmt.Println("lib/openfaas/workflow.go::AddOperation end")
	return node
}

// SyncNode adds a new vertex named Sync
func (flow *Workflow) SyncNode(options ...BranchOption) *Node {

	fmt.Println("lib/openfaas/workflow.go::SyncNode start")
	dag := flow.pipeline.Dag

	node := dag.GetNode("sync")
	if node == nil {
		node = dag.AddVertex("sync", []sdk.Operation{})
	}
	o := &BranchOptions{}
	for _, opt := range options {
		o.reset()
		opt(o)
		if o.aggregator != nil {
			node.AddAggregator(o.aggregator)
		}
	}
	fmt.Println("lib/openfaas/workflow.go::SyncNode end")
	return &Node{unode: node}
}
