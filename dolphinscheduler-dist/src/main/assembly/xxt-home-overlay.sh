#!/usr/bin/env bash
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
set -euo pipefail

BIN_DIR=$1
PROJECT_ROOT=$2

find_single_artifact() {
  local artifact_dir=$1
  local artifact_pattern=$2
  local artifact_name=$3
  local artifact_list
  local artifact_count

  artifact_list=$(find "$artifact_dir" -maxdepth 1 -type f -name "$artifact_pattern" | sort)
  artifact_count=$(printf '%s\n' "$artifact_list" | sed '/^$/d' | wc -l | tr -d ' ')
  if [ "$artifact_count" -ne 1 ]; then
    echo "Expected exactly one $artifact_name artifact in $artifact_dir, found $artifact_count" >&2
    return 1
  fi
  printf '%s\n' "$artifact_list"
}

SHELL_JAR=$(find_single_artifact \
  "$PROJECT_ROOT/dolphinscheduler-task-plugin/dolphinscheduler-task-shell/target" \
  'dolphinscheduler-task-shell-*-shade.jar' \
  'Shell task plugin')
S3_JAR=$(find_single_artifact \
  "$PROJECT_ROOT/dolphinscheduler-storage-plugin/dolphinscheduler-storage-s3/target" \
  'dolphinscheduler-storage-s3-*-shade.jar' \
  'S3 storage plugin')
MYSQL_JAR=$(find_single_artifact \
  "$PROJECT_ROOT/dolphinscheduler-tools/target/tools/libs" \
  'mysql-connector-j-*.jar' \
  'MySQL connector')

mkdir -p "$BIN_DIR/plugins/task-plugins" "$BIN_DIR/plugins/storage-plugins"
chmod +x "$BIN_DIR/tools/bin/"*.sh
cp "$SHELL_JAR" "$BIN_DIR/plugins/task-plugins/"
cp "$S3_JAR" "$BIN_DIR/plugins/storage-plugins/"
cp "$MYSQL_JAR" "$BIN_DIR/libs/"

MYSQL_JAR_NAME=$(basename "$MYSQL_JAR")
for module in api-server master-server worker-server alert-server tools; do
  ln -sfn "../../libs/$MYSQL_JAR_NAME" "$BIN_DIR/$module/libs/$MYSQL_JAR_NAME"
done
