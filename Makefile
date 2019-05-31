

update-changelog:
	conventional-changelog -p angular -i CHANGELOG.md -s

test:
	go test ./... -v --coverprofile=coverage.txt --covermode=atomic