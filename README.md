[![Qri](https://img.shields.io/badge/made%20by-qri-magenta.svg?style=flat-square)](https://qri.io)
[![GoDoc](https://godoc.org/github.com/qri-io/startf?status.svg)](http://godoc.org/github.com/qri-io/startf)
[![License](https://img.shields.io/github/license/qri-io/startf.svg?style=flat-square)](./LICENSE)
[![Codecov](https://img.shields.io/codecov/c/github/qri-io/startf.svg?style=flat-square)](https://codecov.io/gh/qri-io/startf)
[![CI](https://img.shields.io/circleci/project/github/qri-io/startf.svg?style=flat-square)](https://circleci.com/gh/qri-io/startf)
[![Go Report Card](https://goreportcard.com/badge/github.com/qri-io/startf)](https://goreportcard.com/report/github.com/qri-io/startf)

# Qri Starlark Transformation Syntax

Qri ("query") is about datasets. Transformions are repeatable scripts for generating a dataset. [Starlark](https://github.com/google/starlark-go/blob/master/doc/spec.md) is a scripting langauge from Google that feels a lot like python. This package implements starlark as a _transformation syntax_. Starlark tranformations are about as close as one can get to the full power of a programming language as a transformation syntax. Often you need this degree of control to generate a dataset.

Typical examples of a starlark transformation include:
* combining paginated calls to an API into a single dataset
* downloading unstructured structured data from the internet to extract
* re-shaping raw input data before saving a dataset

We're excited about starlark for a few reasons:
* **python syntax** - _many_ people working in data science these days write python, we like that, starlark likes that. dope.
* **deterministic subset of python** - unlike python, starlark removes properties that reduce introspection into code behaviour. things like `while` loops and recursive functions are ommitted, making it possible for qri to infer how a given transformation will behave.
* **parallel execution** - thanks to this deterministic requirement (and lack of global interpreter lock) starlark functions can be executed in parallel. Combined with peer-2-peer networking, we're hoping to advance tranformations toward peer-driven distribed computing. More on that in the coming months.


## Getting started
If you're mainly interested in learning how to write starlark transformations, our [documentation](https://qri.io/docs) is a better place to start. If you're interested in contributing to the way starlark transformations work, this is the place!

The easiest way to see starlark transformations in action is to use [qri](https://github.com/qri-io/qri). This `startf` package powers all the starlark stuff in qri. Assuming you have the [go programming language](https://golang.org/) the following should work from a terminal:
```shell
# get this package
$ go get github.com/qri-io/startf

# navigate to package
$ cd $GOPATH/src/github.com/qri-io/startf

# run tests
$ go test ./...
```

Often the next steps are to install [qri](https://github.com/qri-io/qri), mess with this `startf` package, then rebuild qri with your changes to see them in action within qri itself.

## Starlark Data Functions

Data Functions are the core of a starlark transform script. Here's an example of a simple data function that returns a constant result:

```python
def transform(qri):
  return ["hello","world"]
```

Here's something slightly more complicated that modifies a previous dataset by adding up the length of all of the elements:

```python
def transform(qri):
  body = qri.get_body()
  count = 0
  for entry in body:
    count += len(entry)
  return [{"total": count}]
```

Starlark transformations have a few rules on top of starlark itself:
* Data functions *always* return an array or dictionary/object, representing the new dataset body
* When you define a data function, qri calls it for you
* All transform functions are optional (you don't _need_ to define them), _but_
* A transformation must have at least one data function
* Data functions are always called in the same order
* Data functions often get a `qri` parameter that lets them do special things

More docs on the provide API is coming soon.


## Running a transform

Let's say the above function is saved as `transform.star`. First, create a configuration file (saved as `config.yaml`, for example) with at least the minimal structure:

```
name: dataset_name
transform:
  scriptpath: transform.star
  config:
    org: qri-io
    repo: frontend
```

Then invoke qri:

```
qri update --file=config.yaml me/dataset_name
```

If the script uses qri.get_body, there must be an existing version of the dataset already. Otherwise, if the dataset doesn't exist yet, and is being created from some other source, use `qri add` instead.

** **
