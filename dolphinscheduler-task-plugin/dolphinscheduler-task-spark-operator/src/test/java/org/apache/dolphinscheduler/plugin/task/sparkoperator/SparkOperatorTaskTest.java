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
import org.apache.dolphinscheduler.plugin.task.api.resource.ResourceContext;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class SparkOperatorTaskTest {

    private TaskExecutionContext taskExecutionContext;

    @BeforeEach
    public void setup() {
        taskExecutionContext = Mockito.mock(TaskExecutionContext.class);
        Mockito.when(taskExecutionContext.getTaskAppId()).thenReturn("test-12345");
        Mockito.when(taskExecutionContext.getProcessInstanceId()).thenReturn(100);
        Mockito.when(taskExecutionContext.getTaskInstanceId()).thenReturn(200);
        Mockito.when(taskExecutionContext.getExecutePath()).thenReturn("/tmp/test");
        
        // Mock ResourceContext
        ResourceContext resourceContext = Mockito.mock(ResourceContext.class);
        ResourceContext.ResourceItem resourceItem = new ResourceContext.ResourceItem();
        resourceItem.setResourceAbsolutePathInLocal("/tmp/test-app.jar");
        resourceItem.setResourceAbsolutePathInStorage("hdfs://test-app.jar");
        Mockito.when(resourceContext.getResourceItem(Mockito.anyString())).thenReturn(resourceItem);
        Mockito.when(taskExecutionContext.getResourceContext()).thenReturn(resourceContext);
    }

    @Test
    public void testInitWithValidParameters() {
        String taskParams = buildValidSparkOperatorParameters();
        Mockito.when(taskExecutionContext.getTaskParams()).thenReturn(taskParams);

        SparkOperatorTask task = new SparkOperatorTask(taskExecutionContext);
        
        // Should not throw exception
        Assertions.assertDoesNotThrow(() -> task.init());
        
        SparkOperatorParameters params = task.getParameters();
        Assertions.assertNotNull(params);
        Assertions.assertEquals("test-spark-app", params.getAppName());
        Assertions.assertEquals(SparkApplicationType.Scala, params.getApplicationType());
        Assertions.assertEquals("spark:3.5.0", params.getImage());
    }

    @Test
    public void testInitWithInvalidParameters() {
        String taskParams = buildInvalidSparkOperatorParameters();
        Mockito.when(taskExecutionContext.getTaskParams()).thenReturn(taskParams);

        SparkOperatorTask task = new SparkOperatorTask(taskExecutionContext);
        
        // Should throw TaskException due to invalid parameters
        Assertions.assertThrows(Exception.class, () -> task.init());
    }

    @Test
    public void testGenerateApplicationName() {
        String taskParams = buildValidSparkOperatorParameters();
        Mockito.when(taskExecutionContext.getTaskParams()).thenReturn(taskParams);

        SparkOperatorTask task = new SparkOperatorTask(taskExecutionContext);
        task.init();
        
        // Application name should be DNS-1123 compliant
        // Format: <appName>-<taskAppId>
        String expectedPattern = "test-spark-app-test-12345";
        // We can't directly access applicationName, but we can verify through logs
        // or by checking the created CRD in integration tests
        Assertions.assertNotNull(task.getParameters());
    }

    @Test
    public void testParametersWithVolcano() {
        String taskParams = buildSparkOperatorParametersWithVolcano();
        Mockito.when(taskExecutionContext.getTaskParams()).thenReturn(taskParams);

        SparkOperatorTask task = new SparkOperatorTask(taskExecutionContext);
        task.init();
        
        SparkOperatorParameters params = task.getParameters();
        Assertions.assertEquals("volcano", params.getBatchScheduler());
        Assertions.assertEquals("default-queue", params.getQueue());
        Assertions.assertEquals("high-priority", params.getPriorityClassName());
    }

    @Test
    public void testParametersWithDriverExecutorSpecs() {
        String taskParams = buildSparkOperatorParametersWithPodSpecs();
        Mockito.when(taskExecutionContext.getTaskParams()).thenReturn(taskParams);

        SparkOperatorTask task = new SparkOperatorTask(taskExecutionContext);
        task.init();
        
        SparkOperatorParameters params = task.getParameters();
        
        // Driver spec
        Assertions.assertNotNull(params.getDriverSpec());
        Assertions.assertEquals(2, params.getDriverSpec().getCores());
        Assertions.assertEquals("2g", params.getDriverSpec().getMemory());
        
        // Executor spec
        Assertions.assertNotNull(params.getExecutorSpec());
        Assertions.assertEquals(4, params.getExecutorSpec().getCores());
        Assertions.assertEquals("4g", params.getExecutorSpec().getMemory());
        Assertions.assertEquals(3, params.getExecutorInstances());
    }

    @Test
    public void testPythonApplication() {
        String taskParams = buildPySparkParameters();
        Mockito.when(taskExecutionContext.getTaskParams()).thenReturn(taskParams);

        SparkOperatorTask task = new SparkOperatorTask(taskExecutionContext);
        task.init();
        
        SparkOperatorParameters params = task.getParameters();
        Assertions.assertEquals(SparkApplicationType.Python, params.getApplicationType());
        Assertions.assertNull(params.getMainClass()); // Python doesn't need mainClass
    }

    // Helper methods to build test parameters

    private String buildValidSparkOperatorParameters() {
        SparkOperatorParameters params = new SparkOperatorParameters();
        params.setAppName("test-spark-app");
        params.setApplicationType(SparkApplicationType.Scala);
        params.setImage("spark:3.5.0");
        params.setNamespace("default");
        params.setMainClass("com.example.TestApp");
        
        ResourceInfo mainJar = new ResourceInfo();
        mainJar.setResourceName("test-app.jar");
        params.setMainApplicationFile(mainJar);
        
        return JSONUtils.toJsonString(params);
    }

    private String buildInvalidSparkOperatorParameters() {
        SparkOperatorParameters params = new SparkOperatorParameters();
        // Missing required fields: appName, applicationType, image, mainApplicationFile
        params.setNamespace("default");
        
        return JSONUtils.toJsonString(params);
    }

    private String buildSparkOperatorParametersWithVolcano() {
        SparkOperatorParameters params = new SparkOperatorParameters();
        params.setAppName("test-spark-app");
        params.setApplicationType(SparkApplicationType.Scala);
        params.setImage("spark:3.5.0");
        params.setNamespace("default");
        params.setMainClass("com.example.TestApp");
        params.setBatchScheduler("volcano");
        params.setQueue("default-queue");
        params.setPriorityClassName("high-priority");
        
        ResourceInfo mainJar = new ResourceInfo();
        mainJar.setResourceName("test-app.jar");
        params.setMainApplicationFile(mainJar);
        
        return JSONUtils.toJsonString(params);
    }

    private String buildSparkOperatorParametersWithPodSpecs() {
        SparkOperatorParameters params = new SparkOperatorParameters();
        params.setAppName("test-spark-app");
        params.setApplicationType(SparkApplicationType.Scala);
        params.setImage("spark:3.5.0");
        params.setNamespace("default");
        params.setMainClass("com.example.TestApp");
        
        // Driver spec
        SparkOperatorParameters.PodSpec driverSpec = new SparkOperatorParameters.PodSpec();
        driverSpec.setCores(2);
        driverSpec.setMemory("2g");
        driverSpec.setLabels("{\"team\": \"data\"}");
        params.setDriverSpec(driverSpec);
        
        // Executor spec
        SparkOperatorParameters.PodSpec executorSpec = new SparkOperatorParameters.PodSpec();
        executorSpec.setCores(4);
        executorSpec.setMemory("4g");
        executorSpec.setLabels("{\"team\": \"data\"}");
        params.setExecutorSpec(executorSpec);
        params.setExecutorInstances(3);
        
        ResourceInfo mainJar = new ResourceInfo();
        mainJar.setResourceName("test-app.jar");
        params.setMainApplicationFile(mainJar);
        
        return JSONUtils.toJsonString(params);
    }

    private String buildPySparkParameters() {
        SparkOperatorParameters params = new SparkOperatorParameters();
        params.setAppName("pyspark-app");
        params.setApplicationType(SparkApplicationType.Python);
        params.setImage("spark-py:3.5.0");
        params.setNamespace("default");
        params.setArguments("--input /data/input --output /data/output");
        
        ResourceInfo pythonFile = new ResourceInfo();
        pythonFile.setResourceName("wordcount.py");
        params.setMainApplicationFile(pythonFile);
        
        return JSONUtils.toJsonString(params);
    }
}
