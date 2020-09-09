#!/usr/bin/env bash
set -e
curl -L -o /tmp/spark.tgz https://www.apache.org/dyn/closer.lua/spark/spark-3.0.0/spark-3.0.0-bin-hadoop2.7.tgz
tar -xvzf /tmp/spark.tgz -C /tmp