#!/usr/bin/env bash

MANIFEST=`find puppet/ -name Modulefile | sed "s|^\./||"`
VERSION_REGEX="[0-9]+\.[0-9]+\.[0-9]"
VERSION_TEXT=`cat puppet/ft-organisations_rw_neo4j/Modulefile | grep -oEi "version\s+'[0-9]+\.[0-9]+\.[0-9]'"`
if [[ $VERSION_TEXT =~ $VERSION_REGEX ]]
then
	VERSION="${BASH_REMATCH}"
	echo "Current version: $VERSION"
	MAJOR=`echo "$VERSION" | grep -oEi '[0-9]+[.][0-9]+'`
	MINOR=`echo "$VERSION" | grep -oEi '[0-9]+$'`
	NEW_MINOR=`awk "BEGIN {print 1 + $MINOR}"`
	TAG=$MAJOR.$NEW_MINOR
	echo "New version is $TAG"
        sed -e s/^version\ '.*'$/version\ \'${TAG}\'/ ${MANIFEST} > Modulefile.tmp && mv Modulefile.tmp ${MANIFEST}
	git commit $MANIFEST -m "Updated version to $TAG"
	git tag $TAG
	git push --all origin
else
	echo "Could not find a valid version number. Version text was '$VERSION_TEXT'"
	exit 1
fi


