package worker

type Worker interface {
	Run() error
	Stop() error
	Errors() chan error
	GetMetasOffset() []map[string]interface{}
	RegisterProcessor(fn func(row []string)) error
}
