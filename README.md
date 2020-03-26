# Map-Reduce
A MapReduce framework with some simple applications in Go

## Overview
MapReduce is an abstraction technique for doing Big-data computation in
parellel, distributed and fault-tolerant fashion.

This framework implemented similar ideas in [MapReduce](https://pdos.csail.mit.edu/6.824/papers/mapreduce.pdf)

This project implements a simple MapReduce framework with some examples
applications such as word counter and document indexer.

## Usage

**Build the plugin applications using**
```go
go build -buildmode=plugin ../mrapps/wc.go
```

**Run master with given files as input**
```go
go run mrmaster.go pg-*.txt
```

**Run worker (one or more) with plugin app**
```go
go run mrworker.go wc.so
```

