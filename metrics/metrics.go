/*
# The MIT License (MIT)
#
# Copyright (c) 2019  Carbon Black
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
*/
package metrics

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"sync"

	"github.com/signalfx/golib/datapoint"
	"github.com/signalfx/golib/sfxclient"
	"github.com/sirupsen/logrus"

	"github.com/carbonblack/eqr/logging"
)

type SfxClient struct {
	Client *sfxclient.HTTPSink
	m sync.Mutex
}

var ctx context.Context = context.Background()
var baseDimensions map[string]string
var prefix string = "example.prefix"
var sender MetricSender = &NoopMetricSender{}
var logger = logging.GetLogger()


func Init(useMetrics bool) {
	if useMetrics {
		sender = NewSfxClient()
		sender.(*SfxClient).Client.AuthToken = os.Getenv("SFX_TOKEN")
	}

	baseDimensions = make(map[string]string)
	baseDimensions["region"] = os.Getenv("AWS_REGION")
}

func NewSfxClient() MetricSender {
	return &SfxClient{
		Client: sfxclient.NewHTTPSink(),
	}
}

func GetSfxClient() MetricSender {
	return sender
}

func (s *SfxClient) SendGauge(name string, value interface{}, dims map[string]string) {
	s.m.Lock()
	defer s.m.Unlock()
	logger.WithFields(logrus.Fields{
	  "metricName": name,
	  "value": value,
	  "type": reflect.TypeOf(value).Name(),
	}).Debug("Send Gauge Invoked")
	metricName := BuildMetricName(name)
	dimensions := AddDimensions(dims)
	switch reflect.TypeOf(value).Name() {
	case "int":
		logger.WithFields(logrus.Fields{
			"metricName": name,
			"value": value,
			"type": reflect.TypeOf(value).Name(),
		}).Debug("Int64 metric send gauge case hit")
		s.Client.AddDatapoints(ctx, []*datapoint.Datapoint{
			sfxclient.Gauge(metricName, dimensions, int64(value.(int))),
		})
	case "int32":
		logger.WithFields(logrus.Fields{
		"metricName": name,
		"value": value,
		"type": reflect.TypeOf(value).Name(),
		}).Debug("Int64 metric send gauge case hit")
		s.Client.AddDatapoints(ctx, []*datapoint.Datapoint{
			sfxclient.Gauge(metricName, dimensions, int64(value.(int32))),
		})
	case "int64":
		logger.WithFields(logrus.Fields{
		  "metricName": name,
		  "value": value,
		  "type": reflect.TypeOf(value).Name(),
		}).Debug("Int64 metric send gauge case hit")
		s.Client.AddDatapoints(ctx, []*datapoint.Datapoint{
	        sfxclient.Gauge(metricName, dimensions, value.(int64)),
	    })
	case "float32":
		logger.WithFields(logrus.Fields{
			"metricName": name,
			"value": value,
			"type": reflect.TypeOf(value).Name(),
		}).Debug("Float64 metric send gauge case hit")
		s.Client.AddDatapoints(ctx, []*datapoint.Datapoint{
			sfxclient.GaugeF(metricName, dimensions, float64(value.(float32))),
		})
	case "float64":
		logger.WithFields(logrus.Fields{
			"metricName": name,
			"value": value,
			"type": reflect.TypeOf(value).Name(),
		}).Debug("Float64 metric send gauge case hit")
		s.Client.AddDatapoints(ctx, []*datapoint.Datapoint{
			sfxclient.GaugeF(metricName, dimensions, value.(float64)),
		})
	}
}

func (s *SfxClient) SendCounter(name string, value interface{}, dims map[string]string) {
	s.m.Lock()
	defer s.m.Unlock()
	logger.WithFields(logrus.Fields{
	  "metricName": name,
	  "value": value,
	  "type": reflect.TypeOf(value).Name(),
	}).Debug("Send Counter Invoked")
	metricName := BuildMetricName(name)
	dimensions := AddDimensions(dims)
	switch reflect.TypeOf(value).Name() {
	case "int":
		logger.WithFields(logrus.Fields{
			"metricName": name,
			"value": value,
			"type": reflect.TypeOf(value).Name(),
		}).Debug("Int64 metric send counter case hit")
		s.Client.AddDatapoints(ctx, []*datapoint.Datapoint{
			sfxclient.Cumulative(metricName, dimensions, int64(value.(int))),
		})
	case "int32":
		logger.WithFields(logrus.Fields{
			"metricName": name,
			"value": value,
			"type": reflect.TypeOf(value).Name(),
		}).Debug("Int64 metric send counter case hit")
		s.Client.AddDatapoints(ctx, []*datapoint.Datapoint{
			sfxclient.Cumulative(metricName, dimensions, int64(value.(int))),
		})
	case "int64":
		logger.WithFields(logrus.Fields{
		  "metricName": name,
		  "value": value,
		  "type": reflect.TypeOf(value).Name(),
		}).Debug("Int64 metric send counter case hit")
		s.Client.AddDatapoints(ctx, []*datapoint.Datapoint{
	        sfxclient.Cumulative(metricName, dimensions, value.(int64)),
	    })
	case "float32":
		logger.WithFields(logrus.Fields{
			"metricName": name,
			"value": value,
			"type": reflect.TypeOf(value).Name(),
		}).Debug("Float64 metric send counter case hit")
		s.Client.AddDatapoints(ctx, []*datapoint.Datapoint{
			sfxclient.CumulativeF(metricName, dimensions, float64(value.(float32))),
		})
	case "float64":
		logger.WithFields(logrus.Fields{
		  "metricName": name,
		  "value": value,
		  "type": reflect.TypeOf(value).Name(),
		}).Debug("Float64 metric send counter case hit")
		s.Client.AddDatapoints(ctx, []*datapoint.Datapoint{
	        sfxclient.CumulativeF(metricName, dimensions, value.(float64)),
	    })
	}
}

func BuildMetricName(name string) string {
	return fmt.Sprintf("%v%v", prefix, name)
}

func AddDimensions(extraDims map[string]string) map[string]string {
	tmp := make(map[string]string)
	for k, v := range baseDimensions {
		tmp[k] = v
	}
	for k, v := range extraDims {
		tmp[k] = v
	}
	return tmp
}