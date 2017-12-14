#!/bin/sh

#==============================================================================
#   Copyright 2017 International Business Machines Corp.
#   
#   See the NOTICE file distributed with this work for additional information
#   regarding copyright ownership. Licensed under the Apache License, 
#   Version 2.0 (the "License"); you may not use this file except in compliance
#   with the License. You may obtain a copy of the License at
#   
#     http://www.apache.org/licenses/LICENSE-2.0
#   
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
#==============================================================================

echo 'Setting Variables'
JAVA="/shared/java/J8.0_64/"
LOCATION="/shared/zWebSphere/Liberty/V17001/bin/batchManager"
ACTION="submit"
SERVER="--batchManager=wg31.washington.ibm.com:10443"
USER="--user=Fred"
PW="--password=fredpwd"
APP="--applicationName=SleepyBatchletSample-1.0"
JOBXML="--jobXMLName=sleepy-batchlet.xml"
PARM1="--jobParameter=sleep.time.seconds=5"
PARM2="--getJobLog"
PARM3="--trustSslCertificates"
PARM4="--wait"
OUTLOC="/u/user1/jbatch/lab4"

echo 'Set Java Home'
export JAVA_HOME=$JAVA

echo 'Submitting Command'
$LOCATION $ACTION $SERVER $USER $PW $APP $JOBXML $PARM1 $PARM2 $PARM3 $PARM4 > $OUTLOC/out.txt 2> $OUTLOC/err.txt

rc=$?

if [ rc -eq 35 ]; then
  exitcode=0
  echo 'batchManager exit code=' $rc 'Status=Good: submitted and completed.'
  echo 'Will set shell script exit code to' $exitcode
  echo 'If BPXBATCH, then JES return code should be RC=0000'
  exit $exitcode

elif [ rc -eq 33 ]; then
  exitcode=4
  echo 'batchManager exit code=' $rc 'Status=Issue: someone stopped the job.'
  echo 'Will set shell script exit code to' $exitcode
  echo 'If BPXBATCH, then JES return code should be RC=1024'
  exit $exitcode

elif [ rc -eq 20 ] || [ rc -eq 21 ] || [ rc -eq 22 ]; then
  exitcode=8
  echo 'batchManager exit code=' $rc 'Status=Issue: argument problem -- required, unrecognized, invalid.'
  echo 'Will set shell script exit code to' $exitcode
  echo 'If BPXBATCH, then JES return code should be RC=2048'
  exit $exitcode

elif [ rc -eq 34 ] || [ rc -eq 36 ]; then
  exitcode=12
  echo 'batchManager exit code=' $rc 'Status=Problem: did not complete or was abandoned.'
  echo 'Will set shell script exit code to' $exitcode
  echo 'If BPXBATCH, then JES return code should be RC=3072'
  exit $exitcode

else
  exitcode=13
  echo 'batchManager exit code=' $rc 'Status=Problem: some other problem occurred.  See Knowledge Center.'
  echo 'Will set shell script exit code to' $exitcode
  echo 'If BPXBATCH, then JES return code should be RC=3328'
  exit $exitcode
fi

echo 'Done!'
