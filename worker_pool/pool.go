package worker_pool

type workerPool struct {
	n  int
	fn func(interface{})
}

func NewPool(n int, fn func(interface{})) *workerPool {
	p := &workerPool{
		n:  n,
		fn: fn,
	}
	return p
}

func (p *workerPool) Run(c chan interface{}) {
	done := make(chan struct{})
	for w := 1; w < p.n; w++ {
		go p.runner(c, done)
	}
	for w := 1; w < p.n; w++ {
		<-done
	}
}

func (p *workerPool) runner(c chan interface{}, done chan struct{}) {
	for m := range c {
		p.fn(m)
	}
	done <- struct{}{}
}
