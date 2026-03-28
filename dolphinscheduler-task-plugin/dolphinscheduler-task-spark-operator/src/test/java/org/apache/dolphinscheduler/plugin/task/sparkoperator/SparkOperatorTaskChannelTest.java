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

import org.apache.dolphinscheduler.common.utils.JSONUtils;
import org.apache.dolphinscheduler.plugin.task.api.TaskExecutionContext;
import org.apache.dolphinscheduler.plugin.task.api.model.ResourceInfo;
import org.apache.dolphinscheduler.plugin.task.api.parameters.AbstractParameters;
import org.apache.dolphinscheduler.plugin.task.api.parameters.ParametersNode;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class SparkOperatorTaskChannelTest {

    @Test
    public void testParseParameters() {
        SparkOperatorTaskChannel channel = new SparkOperatorTaskChannel();
        
        // Build test parameters
        SparkOperatorParameters params = new SparkOperatorParameters();
        params.setAppName("test-app");
        params.setApplicationType(SparkApplicationType.Scala);
        params.setImage("spark:3.5.0");
        params.setNamespace("default");
        params.setMainClass("com.example.TestApp");
        
        ResourceInfo mainJar = new ResourceInfo();
        mainJar.setResourceName("test.jar");
        params.setMainApplicationFile(mainJar);
        
        String jsonParams = JSONUtils.toJsonString(params);
        
        // Mock ParametersNode
        ParametersNode parametersNode = Mockito.mock(ParametersNode.class);
        Mockito.when(parametersNode.getTaskParams()).thenReturn(jsonParams);
        
        // Parse parameters
        AbstractParameters parsedParams = channel.parseParameters(parametersNode);
        
        Assertions.assertNotNull(parsedParams);
        Assertions.assertInstanceOf(SparkOperatorParameters.class, parsedParams);
        
        SparkOperatorParameters sparkParams = (SparkOperatorParameters) parsedParams;
        Assertions.assertEquals("test-app", sparkParams.getAppName());
        Assertions.assertEquals(SparkApplicationType.Scala, sparkParams.getApplicationType());
    }

    @Test
    public void testCreateTask() {
        SparkOperatorTaskChannel channel = new SparkOperatorTaskChannel();
        TaskExecutionContext taskRequest = Mockito.mock(TaskExecutionContext.class);
        
        var task = channel.createTask(taskRequest);
        
        Assertions.assertNotNull(task);
        Assertions.assertInstanceOf(SparkOperatorTask.class, task);
    }
}
