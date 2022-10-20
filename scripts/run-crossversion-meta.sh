#!/bin/bash

set -ex

VERSIONS=""
for branch in "$@"
do
    git checkout "$branch"
    sha=`git rev-parse --short HEAD`

    # If the branch name has a "-<suffix>", pull off the suffix. With the
    # crl-release-{XX.X} release branch naming scheme, this will extract the
    # {XX.X}.
    version=`cut -d- -f3 <<< "$branch"`

    echo "Building $version ($sha)"
    go test -c -o "meta.$version.test" ./internal/metamorphic
    VERSIONS="$VERSIONS -version $version,$sha,$PWD/meta.$version.test"
done

# Always use master's crossversion package.
git checkout master

if [[ -z "${STRESS}" ]]; then
    go test ./internal/metamorphic/crossversion -test.v -test.timeout "${TIMEOUT:-30m}" -test.run 'TestMetaCrossVersion$' $(echo $VERSIONS)
else
    stress -p 1 go test ./internal/metamorphic/crossversion -test.v -test.timeout "${TIMEOUT:-30m}" -test.run 'TestMetaCrossVersion$' $(echo $VERSIONS)
fi
