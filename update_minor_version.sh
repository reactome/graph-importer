#!/bin/bash

# Get the current version from Maven
current_version=$(mvn help:evaluate -Dexpression=project.version -q -DforceStdout)
echo "Current version: $current_version"

# Extract major, minor, and patch versions
major=${current_version%%.*}            
rest=${current_version#*.}               
minor=${rest%%.*}                        
patch=${rest#*.}                         

# Increment the minor version
new_minor=$((minor + 1))
new_version="${major}.${new_minor}.0"

# Output the new version
echo "New version: $new_version"

# Set the new version in Maven
mvn versions:set -DnewVersion=$new_version
