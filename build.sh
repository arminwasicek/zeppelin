#!/bin/bash

./mvnw clean package -Pspark-2.1 -Phadoop-2.4 -Pyarn -Ppyspark -Psparkr -Pscala-2.11 -DskipTests


