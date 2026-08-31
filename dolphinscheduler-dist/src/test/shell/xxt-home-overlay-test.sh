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

TEST_DIR=$(cd "$(dirname "$0")" && pwd)
OVERLAY_SCRIPT="$TEST_DIR/../../main/assembly/xxt-home-overlay.sh"
TEST_ROOT=$(mktemp -d)
trap 'rm -rf "$TEST_ROOT"' EXIT

PROJECT_ROOT="$TEST_ROOT/project"
BIN_DIR="$TEST_ROOT/bin"
MYSQL_JAR="$PROJECT_ROOT/dolphinscheduler-tools/target/tools/libs/mysql-connector-j-8.0.33.jar"

mkdir -p \
  "$PROJECT_ROOT/dolphinscheduler-task-plugin/dolphinscheduler-task-shell/target" \
  "$PROJECT_ROOT/dolphinscheduler-storage-plugin/dolphinscheduler-storage-s3/target" \
  "$PROJECT_ROOT/dolphinscheduler-tools/target/tools/libs" \
  "$BIN_DIR/libs"

for module in api-server master-server worker-server alert-server tools; do
  mkdir -p "$BIN_DIR/$module/libs"
done

touch "$PROJECT_ROOT/dolphinscheduler-task-plugin/dolphinscheduler-task-shell/target/dolphinscheduler-task-shell-3.4.2-shade.jar"
touch "$PROJECT_ROOT/dolphinscheduler-storage-plugin/dolphinscheduler-storage-s3/target/dolphinscheduler-storage-s3-3.4.2-shade.jar"
touch "$MYSQL_JAR"

bash "$OVERLAY_SCRIPT" "$BIN_DIR" "$PROJECT_ROOT"

test -f "$BIN_DIR/plugins/task-plugins/dolphinscheduler-task-shell-3.4.2-shade.jar"
test -f "$BIN_DIR/plugins/storage-plugins/dolphinscheduler-storage-s3-3.4.2-shade.jar"
test -f "$BIN_DIR/libs/mysql-connector-j-8.0.33.jar"

for module in api-server master-server worker-server alert-server tools; do
  test "$(readlink "$BIN_DIR/$module/libs/mysql-connector-j-8.0.33.jar")" = "../../libs/mysql-connector-j-8.0.33.jar"
done

echo "xxt-home overlay success case passed"

touch "$PROJECT_ROOT/dolphinscheduler-task-plugin/dolphinscheduler-task-shell/target/dolphinscheduler-task-shell-duplicate-shade.jar"
if bash "$OVERLAY_SCRIPT" "$BIN_DIR" "$PROJECT_ROOT" >/dev/null 2>&1; then
  echo "xxt-home overlay accepted duplicate shell artifacts" >&2
  exit 1
fi

echo "xxt-home overlay duplicate-artifact case passed"
