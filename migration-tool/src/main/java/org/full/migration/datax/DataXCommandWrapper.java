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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * DataXCommandWrapper
 * Wrapper for DataX commands, replacing the datax.py script
 * Directly constructs and executes Java commands for DataX
 *
 * @since 2026-04-10
 */
public class DataXCommandWrapper {
    private static final Logger LOGGER = LoggerFactory.getLogger(DataXCommandWrapper.class);
    
    private static final String DATAX_HOME = System.getProperty("user.dir") + File.separator + "datax";
    private static final String CLASS_PATH;
    private static final String LOGBACK_FILE = DATAX_HOME + File.separator + "conf" + File.separator + "logback.xml";
    private static final String DEFAULT_JVM = "-Xms256m -Xmx256m -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=" + DATAX_HOME + File.separator + "log";
    private static final String DEFAULT_PROPERTY_CONF;
    private static final String CLASS_PATH_SEPARATOR = System.getProperty("os.name").toLowerCase().contains("win") ? ";" : ":";
    
    static {
        // Set classpath based on OS
        if (System.getProperty("os.name").toLowerCase().contains("win")) {
            CLASS_PATH = DATAX_HOME + File.separator + "lib" + File.separator + "*";
        } else {
            CLASS_PATH = DATAX_HOME + File.separator + "lib" + File.separator + "*:.";
        }
        
        // Set default properties
        DEFAULT_PROPERTY_CONF = "-Dfile.encoding=UTF-8 " +
                "-Dlogback.statusListenerClass=ch.qos.logback.core.status.NopStatusListener " +
                "-Djava.security.egd=file:///dev/urandom " +
                "-Ddatax.home=" + DATAX_HOME + " " +
                "-Dlogback.configurationFile=" + LOGBACK_FILE;
    }
    
    /**
     * Build DataX command list
     * Add Java executable
     * Add server flag
     * Add JVM parameters
     * Add default properties
     * Add classpath - include both DataX libs and our project's classes
     * Add log file name property
     * Add DataX process monitor class as the main class
     * Add mode ,jobid,job configuration file
     * @param configResult DataXConfigResult containing configuration file path and JVM parameters
     * @param jobId Job ID
     * @return Command list for ProcessBuilder
     */
    public static List<String> buildCommandList(DataXConfigResult configResult, int jobId) {
        List<String> commandList = new ArrayList<>();
        commandList.add(getJavaExecutable());
        commandList.add("-server");
        addJvmParameters(commandList, configResult.getJvmParameters());
        addDefaultProperties(commandList);
        addClasspath(commandList);
        addLogFileNameProperty(commandList, configResult.getConfigFile());
        commandList.add("org.full.migration.datax.DataXProcessMonitor");
        commandList.add("-mode");
        commandList.add( "standalone");
        commandList.add("-jobid");
        commandList.add(String.valueOf(jobId));
        commandList.add("-job");
        commandList.add(configResult.getConfigFile());
        LOGGER.debug("Built DataX command: {}", String.join(" ", commandList));
        return commandList;
    }
    
    /**
     * Add JVM parameters to the command list
     * @param commandList Command list
     * @param jvmParams JVM parameters
     */
    private static void addJvmParameters(List<String> commandList, String jvmParams) {
        String paramsToUse = (jvmParams != null && !jvmParams.isEmpty()) ? jvmParams : DEFAULT_JVM;
        for (String param : paramsToUse.split("\\s+")) {
            if (!param.isEmpty()) {
                commandList.add(param);
            }
        }
    }
    
    /**
     * Add default properties to the command list
     * @param commandList Command list
     */
    private static void addDefaultProperties(List<String> commandList) {
        for (String param : DEFAULT_PROPERTY_CONF.split("\\s+")) {
            if (!param.isEmpty()) {
                commandList.add(param);
            }
        }
    }
    
    /**
     * Add classpath to the command list
     * @param commandList Command list
     */
    private static void addClasspath(List<String> commandList) {
        commandList.add("-classpath");
        String projectClasspath = System.getProperty("java.class.path");
        String fullClasspath = CLASS_PATH + CLASS_PATH_SEPARATOR + projectClasspath;
        commandList.add(fullClasspath);
    }
    
    /**
     * Add log file name property to the command list
     * @param commandList Command list
     * @param configFile Configuration file path
     */
    private static void addLogFileNameProperty(List<String> commandList, String configFile) {
        String logFileName = configFile.substring(configFile.lastIndexOf(File.separator) + 1);
        logFileName = logFileName.replace('.', '_').replace('/', '_');
        commandList.add("-Dlog.file.name=" + logFileName);
    }
    
    /**
     * Get Java executable path
     * @return Java executable path
     */
    private static String getJavaExecutable() {
        String javaHome = System.getenv("JAVA_HOME");
        if (javaHome != null && !javaHome.isEmpty()) {
            return javaHome + File.separator + "bin" + File.separator + (System.getProperty("os.name").toLowerCase().contains("win") ? "java.exe" : "java");
        }
        // Fallback to java in PATH
        return System.getProperty("os.name").toLowerCase().contains("win") ? "java.exe" : "java";
    }
    
    /**
     * Execute DataX command
     * @param commandList Command list
     * @return Process object
     * @throws IOException If an I/O error occurs
     */
    public static Process executeCommand(List<String> commandList) throws IOException {
        ProcessBuilder pb = new ProcessBuilder();
        pb.command(commandList);
        pb.redirectErrorStream(true);
        
        // Set environment variables
        pb.environment().put("JAVA_OPTS", "-Doracle.jdbc.fanEnabled=false -Doracle.jdbc.defaultNChar=true");
        
        LOGGER.debug("Executing DataX command: {}", String.join(" ", commandList));
        return pb.start();
    }
}
