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
import org.apache.dolphinscheduler.plugin.task.api.AbstractRemoteTask;
import org.apache.dolphinscheduler.plugin.task.api.TaskException;
import org.apache.dolphinscheduler.plugin.task.api.TaskExecutionContext;
import org.apache.dolphinscheduler.plugin.task.api.model.TaskResponse;
import org.apache.dolphinscheduler.plugin.task.api.parameters.AbstractParameters;
import org.apache.dolphinscheduler.plugin.task.api.resource.ResourceContext;

import org.apache.commons.lang3.StringUtils;

import java.util.HashMap;
import java.util.Map;

import lombok.extern.slf4j.Slf4j;
import io.fabric8.kubernetes.api.model.GenericKubernetesResource;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import io.fabric8.kubernetes.client.dsl.base.CustomResourceDefinitionContext;

import static org.apache.dolphinscheduler.plugin.task.sparkoperator.SparkOperatorConstants.*;

@Slf4j
public class SparkOperatorTask extends AbstractRemoteTask {

    private final TaskExecutionContext taskExecutionContext;
    private SparkOperatorParameters parameters;
    private KubernetesClient kubernetesClient;
    private String applicationName;

    public SparkOperatorTask(TaskExecutionContext taskExecutionContext) {
        super(taskExecutionContext);
        this.taskExecutionContext = taskExecutionContext;
    }

    @Override
    public void init() {
        log.info("Initializing Spark Operator task...");
        
        parameters = JSONUtils.parseObject(taskExecutionContext.getTaskParams(), SparkOperatorParameters.class);
        
        if (parameters == null) {
            throw new TaskException("Spark Operator task parameters is null");
        }
        
        if (!parameters.checkParameters()) {
            throw new TaskException("Spark Operator task parameters is invalid");
        }
        
        // Generate application name
        applicationName = generateApplicationName();
        
        // Initialize Kubernetes client
        try {
            String kubeconfig = taskExecutionContext.getK8sTaskExecutionContext() != null 
                ? taskExecutionContext.getK8sTaskExecutionContext().getConfigYaml()
                : null;
            
            if (StringUtils.isNotEmpty(kubeconfig)) {
                kubernetesClient = new KubernetesClientBuilder()
                    .withConfig(io.fabric8.kubernetes.client.Config.fromKubeconfig(kubeconfig))
                    .build();
            } else {
                kubernetesClient = new KubernetesClientBuilder().build();
            }
            
            log.info("Kubernetes client initialized successfully");
        } catch (Exception e) {
            throw new TaskException("Failed to initialize Kubernetes client", e);
        }
        
        log.info("Spark Operator task initialized: {}", JSONUtils.toPrettyJsonString(parameters));
    }

    @Override
    public void submitApplication() throws TaskException {
        log.info("Submitting SparkApplication: {}", applicationName);
        
        try {
            // Build SparkApplication CRD
            GenericKubernetesResource sparkApp = buildSparkApplication();
            
            // Create CRD definition context
            CustomResourceDefinitionContext crdContext = new CustomResourceDefinitionContext.Builder()
                .withGroup(API_GROUP)
                .withVersion(API_VERSION)
                .withPlural("sparkapplications")
                .withKind(KIND_SPARK_APPLICATION)
                .withScope("Namespaced")
                .build();
            
            // Create SparkApplication
            GenericKubernetesResource created = kubernetesClient
                .genericKubernetesResources(crdContext)
                .inNamespace(parameters.getNamespace())
                .create(sparkApp);
            
            log.info("SparkApplication created successfully: {}", created.getMetadata().getName());
            setAppIds(applicationName);
            
        } catch (Exception e) {
            log.error("Failed to submit SparkApplication", e);
            throw new TaskException("Failed to submit SparkApplication: " + e.getMessage(), e);
        }
    }

    @Override
    public void trackApplicationStatus() throws TaskException {
        try {
            CustomResourceDefinitionContext crdContext = new CustomResourceDefinitionContext.Builder()
                .withGroup(API_GROUP)
                .withVersion(API_VERSION)
                .withPlural("sparkapplications")
                .withKind(KIND_SPARK_APPLICATION)
                .withScope("Namespaced")
                .build();
            
            GenericKubernetesResource sparkApp = kubernetesClient
                .genericKubernetesResources(crdContext)
                .inNamespace(parameters.getNamespace())
                .withName(applicationName)
                .get();
            
            if (sparkApp == null) {
                log.warn("SparkApplication not found: {}", applicationName);
                return;
            }
            
            // Extract application state
            Map<String, Object> status = sparkApp.getAdditionalProperties();
            String state = extractState(status);
            
            log.info("SparkApplication {} state: {}", applicationName, state);
            
            // Map state to TaskResponse
            TaskResponse taskResponse = mapStateToTaskResponse(state);
            setTaskResponse(taskResponse);
            
        } catch (Exception e) {
            log.error("Failed to track SparkApplication status", e);
            throw new TaskException("Failed to track application status: " + e.getMessage(), e);
        }
    }

    @Override
    public void cancelApplication() throws TaskException {
        log.info("Cancelling SparkApplication: {}", applicationName);
        
        try {
            CustomResourceDefinitionContext crdContext = new CustomResourceDefinitionContext.Builder()
                .withGroup(API_GROUP)
                .withVersion(API_VERSION)
                .withPlural("sparkapplications")
                .withKind(KIND_SPARK_APPLICATION)
                .withScope("Namespaced")
                .build();
            
            kubernetesClient
                .genericKubernetesResources(crdContext)
                .inNamespace(parameters.getNamespace())
                .withName(applicationName)
                .delete();
            
            log.info("SparkApplication cancelled successfully: {}", applicationName);
            
        } catch (Exception e) {
            log.error("Failed to cancel SparkApplication", e);
            throw new TaskException("Failed to cancel application: " + e.getMessage(), e);
        }
    }

    @Override
    public AbstractParameters getParameters() {
        return parameters;
    }

    /**
     * Build SparkApplication CRD resource
     */
    private GenericKubernetesResource buildSparkApplication() {
        GenericKubernetesResource sparkApp = new GenericKubernetesResource();
        sparkApp.setApiVersion(API_GROUP + "/" + API_VERSION);
        sparkApp.setKind(KIND_SPARK_APPLICATION);
        
        // Metadata
        Map<String, String> labels = new HashMap<>();
        labels.put(LABEL_DOLPHINSCHEDULER_TASK_ID, taskExecutionContext.getTaskAppId());
        labels.put(LABEL_DOLPHINSCHEDULER_WORKFLOW_INSTANCE_ID, String.valueOf(taskExecutionContext.getProcessInstanceId()));
        labels.put(LABEL_DOLPHINSCHEDULER_TASK_INSTANCE_ID, String.valueOf(taskExecutionContext.getTaskInstanceId()));
        
        sparkApp.setMetadata(new ObjectMetaBuilder()
            .withName(applicationName)
            .withNamespace(parameters.getNamespace())
            .withLabels(labels)
            .build());
        
        // Spec
        Map<String, Object> spec = new HashMap<>();
        spec.put("type", parameters.getApplicationType().name());
        spec.put("mode", parameters.getMode());
        spec.put("image", parameters.getImage());
        spec.put("imagePullPolicy", parameters.getImagePullPolicy());
        spec.put("sparkVersion", parameters.getSparkVersion());
        
        // Main application file
        ResourceContext resourceContext = taskExecutionContext.getResourceContext();
        String mainAppFile = resourceContext.getResourceItem(parameters.getMainApplicationFile().getResourceName())
            .getResourceAbsolutePathInLocal();
        spec.put("mainApplicationFile", "local://" + mainAppFile);
        
        // Main class
        if (StringUtils.isNotEmpty(parameters.getMainClass())) {
            spec.put("mainClass", parameters.getMainClass());
        }
        
        // Arguments
        if (StringUtils.isNotEmpty(parameters.getArguments())) {
            spec.put("arguments", parameters.getArguments().split("\\s+"));
        }
        
        // Driver spec
        if (parameters.getDriverSpec() != null) {
            spec.put("driver", buildPodSpec(parameters.getDriverSpec()));
        }
        
        // Executor spec
        if (parameters.getExecutorSpec() != null) {
            Map<String, Object> executor = buildPodSpec(parameters.getExecutorSpec());
            executor.put("instances", parameters.getExecutorInstances());
            spec.put("executor", executor);
        }
        
        // Batch scheduler (Volcano/YuniKorn)
        if (StringUtils.isNotEmpty(parameters.getBatchScheduler())) {
            spec.put("batchScheduler", parameters.getBatchScheduler());
            
            Map<String, Object> batchOptions = new HashMap<>();
            if (StringUtils.isNotEmpty(parameters.getQueue())) {
                batchOptions.put("queue", parameters.getQueue());
            }
            if (StringUtils.isNotEmpty(parameters.getPriorityClassName())) {
                batchOptions.put("priorityClassName", parameters.getPriorityClassName());
            }
            if (!batchOptions.isEmpty()) {
                spec.put("batchSchedulerOptions", batchOptions);
            }
        }
        
        // Restart policy
        Map<String, Object> restartPolicy = new HashMap<>();
        restartPolicy.put("type", parameters.getRestartPolicy());
        spec.put("restartPolicy", restartPolicy);
        
        // TTL
        if (parameters.getTtlSecondsAfterFinished() != null) {
            spec.put("timeToLiveSeconds", parameters.getTtlSecondsAfterFinished());
        }
        
        sparkApp.setAdditionalProperty("spec", spec);
        
        return sparkApp;
    }

    /**
     * Build pod spec
     */
    private Map<String, Object> buildPodSpec(SparkOperatorParameters.PodSpec podSpec) {
        Map<String, Object> spec = new HashMap<>();
        
        if (podSpec.getCores() != null) {
            spec.put("cores", podSpec.getCores());
        }
        if (StringUtils.isNotEmpty(podSpec.getCoreLimit())) {
            spec.put("coreLimit", podSpec.getCoreLimit());
        }
        if (StringUtils.isNotEmpty(podSpec.getMemory())) {
            spec.put("memory", podSpec.getMemory());
        }
        if (StringUtils.isNotEmpty(podSpec.getLabels())) {
            spec.put("labels", JSONUtils.toMap(podSpec.getLabels()));
        }
        if (StringUtils.isNotEmpty(podSpec.getAnnotations())) {
            spec.put("annotations", JSONUtils.toMap(podSpec.getAnnotations()));
        }
        if (StringUtils.isNotEmpty(podSpec.getNodeSelector())) {
            spec.put("nodeSelector", JSONUtils.toMap(podSpec.getNodeSelector()));
        }
        
        return spec;
    }

    /**
     * Extract state from status
     */
    @SuppressWarnings("unchecked")
    private String extractState(Map<String, Object> status) {
        if (status == null || !status.containsKey("status")) {
            return STATE_UNKNOWN;
        }
        
        Map<String, Object> statusObj = (Map<String, Object>) status.get("status");
        if (statusObj == null || !statusObj.containsKey("applicationState")) {
            return STATE_UNKNOWN;
        }
        
        Map<String, Object> appState = (Map<String, Object>) statusObj.get("applicationState");
        if (appState == null || !appState.containsKey("state")) {
            return STATE_UNKNOWN;
        }
        
        return (String) appState.get("state");
    }

    /**
     * Map Spark application state to TaskResponse
     */
    private TaskResponse mapStateToTaskResponse(String state) {
        switch (state) {
            case STATE_COMPLETED:
                return TaskResponse.success();
            case STATE_FAILED:
            case STATE_SUBMISSION_FAILED:
                return TaskResponse.fail("SparkApplication failed");
            case STATE_RUNNING:
            case STATE_SUBMITTED:
            case STATE_SUCCEEDING:
                return TaskResponse.running();
            default:
                return TaskResponse.running();
        }
    }

    /**
     * Generate application name
     */
    private String generateApplicationName() {
        String baseName = parameters.getAppName();
        if (StringUtils.isEmpty(baseName)) {
            baseName = "spark-app";
        }
        // Make it DNS-1123 compliant
        baseName = baseName.toLowerCase().replaceAll("[^a-z0-9-]", "-");
        return baseName + "-" + taskExecutionContext.getTaskAppId();
    }

    @Override
    public void close() {
        if (kubernetesClient != null) {
            kubernetesClient.close();
        }
    }
}
