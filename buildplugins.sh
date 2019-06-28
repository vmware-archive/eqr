#!/bin/bash
go get -v ./plugins/dest/
go get -v ./plugins/operators/
cd plugins/dest
for f in *.go
do
  rm ${f%%.*}.so
  echo "Processing ${f%%.*} file..."
  # take action on each file. $f store current file name
  go build -buildmode=plugin -o ${f%%.*}.so ${f%%.*}.go
done

cd ../operators
for f in *.go
do
  rm ${f%%.*}.so
  echo "Processing ${f%%.*} file..."
  # take action on each file. $f store current file name
  go build -buildmode=plugin -o ${f%%.*}.so ${f%%.*}.go
done
