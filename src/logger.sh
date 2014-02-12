#!/bin/sh

mkdir -p bin
javac -d bin -sourcepath src/main -cp lib/snakeyaml-1.11.jar src/main/ds/lab/app/LoggerApp.java
java -cp bin:lib/snakeyaml-1.11.jar ds.lab.app.LoggerApp "$@"
