package worker

type Worker interface {
	Run() error
	Stop() error
}
