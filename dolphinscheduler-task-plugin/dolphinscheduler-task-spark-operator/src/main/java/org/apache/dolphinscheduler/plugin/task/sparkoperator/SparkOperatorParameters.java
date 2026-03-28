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
import org.apache.dolphinscheduler.plugin.task.api.parameters.AbstractParameters;

import java.util.ArrayList;
import java.util.List;

import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = true)
public class SparkOperatorParameters extends AbstractParameters {

    /**
     * Spark application name
     */
    private String appName;

    /**
     * Spark application type: Java, Scala, Python, R
     */
    private SparkApplicationType applicationType;

    /**
     * Spark mode: cluster
     */
    private String mode = "cluster";

    /**
     * Docker image for Spark application
     */
    private String image;

    /**
     * Image pull policy: Always, IfNotPresent, Never
     */
    private String imagePullPolicy = "IfNotPresent";

    /**
     * Main application file (local:// or resource path)
     */
    private ResourceInfo mainApplicationFile;

    /**
     * Main class for Java/Scala applications
     */
    private String mainClass;

    /**
     * Application arguments
     */
    private String arguments;

    /**
     * Spark version
     */
    private String sparkVersion = "3.5.0";

    /**
     * Kubernetes namespace
     */
    private String namespace = "default";

    /**
     * Service account for Spark pods
     */
    private String serviceAccount = "spark";

    /**
     * Driver pod specifications
     */
    private PodSpec driverSpec;

    /**
     * Executor pod specifications
     */
    private PodSpec executorSpec;

    /**
     * Number of executor instances
     */
    private int executorInstances = 1;

    /**
     * Batch scheduler: volcano, yunikorn, none
     */
    private String batchScheduler;

    /**
     * Queue name for batch scheduler
     */
    private String queue;

    /**
     * Priority class name
     */
    private String priorityClassName;

    /**
     * Spark configuration properties
     */
    private String sparkConf;

    /**
     * Hadoop configuration properties
     */
    private String hadoopConf;

    /**
     * Dependencies: jars
     */
    private List<String> jars = new ArrayList<>();

    /**
     * Dependencies: files
     */
    private List<String> files = new ArrayList<>();

    /**
     * Resource files list
     */
    private List<ResourceInfo> resourceList = new ArrayList<>();

    /**
     * Restart policy: Never, OnFailure, Always
     */
    private String restartPolicy = "Never";

    /**
     * Time to live seconds after finish
     */
    private Long ttlSecondsAfterFinished;

    @Override
    public boolean checkParameters() {
        return appName != null && !appName.isEmpty()
                && applicationType != null
                && image != null && !image.isEmpty()
                && mainApplicationFile != null
                && namespace != null && !namespace.isEmpty();
    }

    @Override
    public List<ResourceInfo> getResourceFilesList() {
        if (mainApplicationFile != null && !resourceList.contains(mainApplicationFile)) {
            resourceList.add(mainApplicationFile);
        }
        return resourceList;
    }

    @Data
    public static class PodSpec {
        private Integer cores;
        private String coreLimit;
        private String memory;
        private String labels;
        private String annotations;
        private String nodeSelector;
        private String tolerations;
    }
}
