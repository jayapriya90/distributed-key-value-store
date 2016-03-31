#!/bin/bash -e

INSTALL_DIR=$PWD/install
MAVEN_DOWNLOAD_URL="http://www.us.apache.org/dist/maven/maven-3/3.3.9/binaries/apache-maven-3.3.9-bin.tar.gz"
MAVEN_BIN=$INSTALL_DIR/maven/bin

if [ -d "$INSTALL_DIR" ]; then
  echo "$INSTALL_DIR already exists.. Removing.."
  rm -rf $INSTALL_DIR
fi

if [ ! -d "$INSTALL_DIR" ]; then
  echo "Creating $INSTALL_DIR.."
  mkdir $INSTALL_DIR
  mkdir $INSTALL_DIR/maven
fi

echo "Downloading maven binaries.."
wget -c $MAVEN_DOWNLOAD_URL

echo "Extracting maven binaries.."
tar -xzvf apache-maven-3.3.9-bin.tar.gz -C $INSTALL_DIR/maven --strip-components=1

echo "Compiling source.."
$MAVEN_BIN/mvn clean install -DskipTests -q

echo "Running tests.."
$MAVEN_BIN/mvn test

echo "Compilation and tests ran successfully! Please refer README.md for running different components."

