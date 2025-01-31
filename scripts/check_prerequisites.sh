#!/usr/bin/env bash

set -eu

set +e
declare -A test_map
res=$?
if [[ $res -ne 0 ]]; then
  echo "✗ ERROR: declare -A is not supported. Do you have bash version 4.0 or higher installed?"
  exit 2
fi
set -e


declare -A python
python["name"]="Python"
python["version_command"]="python --version 2>&1 | awk '{print \$2}'"
python["required_version"]="3.9 3.10 3.11"

declare -A docker
docker["name"]="Docker"
docker["version_command"]="docker --version | awk '{print \$3}' | sed 's/,//'"
docker["required_version"]="20 21 22 23 24"

declare -A jq
jq["name"]="jq"
jq["version_command"]="jq --version | awk -F- '{print \$2}'"
jq["required_version"]="any"

declare -A antlr
antlr["name"]="ANTLR"
antlr["version_command"]="antlr4 | head -n1 | awk 'NF>1{print \$NF}'"
antlr["required_version"]="4.9"


code=0

function print_error() {
    >&2 echo "✗ ERROR: $1"
}

check_command_existence() {
    which "$1" >/dev/null 2>&1
    res=$?
    if [[ $res -ne 0 ]]; then
      print_error "$command is not installed."
      code=2
    fi
    echo $res
}

check_version() {
    local tool_name=$1
    local current=$2
    local required=$3
    IFS=' ' read -r -a required_versions <<< "$required"
    if [[ "$required" == "any" ]]; then
        echo "✓ $tool_name version $current is supported."
        return
    fi
    for v in "${required_versions[@]}"; do
        if [[ "$current" =~ $v.* ]]; then
            echo "✓ $tool_name version $version is supported."
            return
        fi
    done
    print_error "$tool_name version $version is not supported. Supported versions are: $required"
    code=1
}

declare -n dependency
for dependency in python docker jq antlr; do
    command=$(echo "${dependency["version_command"]}" | awk '{print $1}')
    if [[ $(check_command_existence "$command") -ne 0 ]]; then
        continue
    fi
    tool_name=${dependency["name"]}
    version=$(eval ${dependency["version_command"]})
    required_version=${dependency["required_version"]}
    check_version $tool_name "$version" "$required_version"
done
if [[ $code -eq 0 ]]; then
    echo "✓ All prerequisites are met."
else
    print_error "Some prerequisites are not met."
fi
exit $code
