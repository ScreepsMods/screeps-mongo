#!/bin/bash

docker pull node:6
for D in */
do
	echo Processing $D
	docker run --rm -v /screeps:/screeps -w /screeps/screeps-mongo/$D node:6 npm install
done
