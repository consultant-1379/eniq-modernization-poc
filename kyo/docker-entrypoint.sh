#!/bin/bash

java -cp /root/ericsson/pm-stats-parser/pm-stats-parser.jar -Dlog4j.configuration=file:/usr/local/spark/conf/log4j.properties -Dhadoop.login=simple -Djava.security.auth.login.config=/opt/mapr/conf/mapr.login.conf -Djava.security.auth.login.config=/opt/mapr/conf/mapr.login.conf com.ericsson.kyo.KYO
