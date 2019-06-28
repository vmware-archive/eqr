FROM golang:1.12
WORKDIR /go/src/app

COPY ./config ./config
COPY ./kinsumer /go/src/github.com/carbonblack/eqr/kinsumer
COPY ./logging /go/src/github.com/carbonblack/eqr/logging
COPY ./metrics /go/src/github.com/carbonblack/eqr/metrics
COPY ./ruleset /go/src/github.com/carbonblack/eqr/ruleset
COPY ./s3Batcher /go/src/github.com/carbonblack/eqr/s3Batcher
COPY ./checkpoint /go/src/github.com/carbonblack/eqr/checkpoint
COPY ./records /go/src/github.com/carbonblack/eqr/records
COPY ./main.go ./main.go
COPY ./buildplugins.sh ./buildplugins.sh
RUN go get -d -v ./...
RUN go build
RUN mkdir /tmp/eqr && chmod 777 /tmp/eqr

ENV AWS_REGION us-east-1
#ENV SFX_TOKEN <IF USING SFX, PUT TOKEN HERE>

CMD /go/src/app/app exampleRuleset ./config/example.json
