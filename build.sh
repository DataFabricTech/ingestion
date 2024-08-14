#!/bin/bash

docker buildx build . --platform linux/amd64 --push -t repo.iris.tools/datafabric/ingestion:v1.1.7 -f Dockerfile
