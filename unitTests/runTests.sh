#!/usr/bin/env bash
cp -a ../plugins ./
sh ./buildplugins.sh
go test -v
rm -r ./plugins
