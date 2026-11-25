/*
 * Copyright 2026 Huawei Technologies Co., Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.full.migration.datax;

/**
 * DataXConfigResult
 * Result of DataX configuration generation
 * Contains the generated config file path and JVM parameters
 *
 * @since 2026-04-11
 */
public class DataXConfigResult {
    private final String configFile;
    private final String jvmParameters;
    
    /**
     * Constructor
     * @param configFile Path to the generated DataX configuration file
     * @param jvmParameters JVM parameters required by the strategy
     */
    public DataXConfigResult(String configFile, String jvmParameters) {
        this.configFile = configFile;
        this.jvmParameters = jvmParameters;
    }
    
    /**
     * Get the path to the generated DataX configuration file
     * @return Config file path
     */
    public String getConfigFile() {
        return configFile;
    }
    
    /**
     * Get the JVM parameters required by the strategy
     * @return JVM parameters
     */
    public String getJvmParameters() {
        return jvmParameters;
    }
}
