#!/usr/bin/env bash
ZIP_NAME="dkozak.zip"

rm -f ${ZIP_NAME}
zip -r ${ZIP_NAME} ./src/ ./build.gradle ./settings.gradle ./execute.sh ./README.md ./Assignment.md ./Assignment.pdf