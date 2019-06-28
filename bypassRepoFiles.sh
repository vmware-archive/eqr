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

#!/usr/bin/env bash

#Github Location
LOCATION=github.com/carbonblack/eqr

mkdir -p ~/go/src/$LOCATION/logging/
cp -a ./logging/* ~/go/src/$LOCATION/logging/

mkdir -p ~/go/src/$LOCATION/metrics/
cp -a ./metrics/* ~/go/src/$LOCATION/metrics/

mkdir -p ~/go/src/$LOCATION/kinsumer/
cp -a ./kinsumer/* ~/go/src/$LOCATION/kinsumer/

mkdir -p ~/go/src/$LOCATION/s3Batcher/
cp -a ./s3Batcher/*.go ~/go/src/$LOCATION/s3Batcher/

mkdir -p ~/go/src/$LOCATION/checkpoint/
cp -a ./checkpoint/* ~/go/src/$LOCATION/checkpoint/

mkdir -p ~/go/src/$LOCATION/records/
cp -a ./records/* ~/go/src/$LOCATION/records/

mkdir -p ~/go/src/$LOCATION/ruleset/
cp -a ./ruleset/* ~/go/src/$LOCATION/ruleset/

mkdir -p ~/go/src/$LOCATION/ruleset/rulebase/
cp -a ./ruleset/rulebase/* ~/go/src/$LOCATION/ruleset/rulebase/

go build ./ruleset/ruleset.go
