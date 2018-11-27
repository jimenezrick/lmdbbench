#!/bin/bash

rm -rf db && mkdir db

while true
do
	echo "==> New test run: $(date)"
	timeout --preserve-status -s KILL $((RANDOM % 10 + 1))s cargo run --release db
	if [[ $? != 137 ]]
	then
		echo "==> ERROR: non-killed exit status code reported"
		exit 1
	fi

	if [[ $(( RANDOM % 500)) == 0 ]]
	then
		echo "==> Cleaning up DB"
		rm -rf db && mkdir db
	fi
done |& tee -a harness.log
