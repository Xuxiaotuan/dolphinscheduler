/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.dolphinscheduler.plugin.task.sparkoperator;

import org.apache.dolphinscheduler.plugin.task.api.model.ResourceInfo;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class SparkOperatorParametersTest {

    @Test
    public void testCheckParameters() {
        SparkOperatorParameters parameters = new SparkOperatorParameters();
        
        // Should fail - missing required fields
        Assertions.assertFalse(parameters.checkParameters());
        
        // Set required fields
        parameters.setAppName("test-app");
        parameters.setApplicationType(SparkApplicationType.Scala);
        parameters.setImage("spark:3.5.0");
        parameters.setNamespace("default");
        
        ResourceInfo mainJar = new ResourceInfo();
        mainJar.setResourceName("test.jar");
        parameters.setMainApplicationFile(mainJar);
        
        // Should pass
        Assertions.assertTrue(parameters.checkParameters());
    }

    @Test
    public void testDefaultValues() {
        SparkOperatorParameters parameters = new SparkOperatorParameters();
        
        Assertions.assertEquals("cluster", parameters.getMode());
        Assertions.assertEquals("IfNotPresent", parameters.getImagePullPolicy());
        Assertions.assertEquals("3.5.0", parameters.getSparkVersion());
        Assertions.assertEquals("default", parameters.getNamespace());
        Assertions.assertEquals("spark", parameters.getServiceAccount());
        Assertions.assertEquals(1, parameters.getExecutorInstances());
        Assertions.assertEquals("Never", parameters.getRestartPolicy());
    }
}
