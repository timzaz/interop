#!/bin/sh -e
set -e

DIR="$( dirname "${BASH_SOURCE[0]}" )"

# The goal is to get one directory above the scripts directory no matter
# where the script is called from. This assumes the script is not symlinked!
if [ $DIR == "." ]; then # Inside the scripts directory
  DIR=".${DIR}"
else  # DIR will end in .../scripts
  DIR="${DIR}/.."
fi

DIR="$( cd ${DIR} > /dev/null 2>&1 && pwd )"

isort "${DIR}"

autoflake \
  --exclude=__init__.py \
  --in-place \
  --recursive \
  --remove-all-unused-imports \
  --remove-unused-variables \
  "${DIR}"

black --line-length 79 --target-version py38 "${DIR}"
