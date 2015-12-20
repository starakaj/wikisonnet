#! /bin/bash

echo "Collecting poem writing python scripts"

mkdir -p ./wikibard
cp ../server/wikibard.py wikibard
cp ../server/dbconnect.py wikibard
cp ../server/dbhash.py wikibard
cp ../server/dbreader.py wikibard
cp ../server/benchmarking.py wikibard
cp ../server/poemform.py wikibard
cp ../server/dbconfig.yml wikibard
