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

package org.full.migration.test.cli;

import org.apache.commons.cli.*;
import org.full.migration.test.command.TestCommand;
import org.full.migration.test.command.CleanupCommand;
import org.full.migration.test.command.PrepareCommand;
import org.full.migration.test.command.CheckDataCommand;

import java.util.HashMap;
import java.util.Map;

/**
 * TestCLI
 * Command line interface for test environment management
 * Provides command line interface for cleaning up and preparing test environments
 * for oracle2ograc migration testing
 */
public class TestCLI {
    private static final Map<String, TestCommand> commands = new HashMap<>();

    static {
        commands.put("cleanup", new CleanupCommand());
        commands.put("prepare", new PrepareCommand());
        commands.put("check", new CheckDataCommand());
    }

    /**
     * Main entry point for the command line interface
     * @param args Command line arguments
     */
    public static void main(String[] args) {
        Options options = new Options();

        options.addOption(Option.builder("a").longOpt("action").hasArg().desc("Action to perform: cleanup, prepare").build());
        options.addOption(Option.builder("c").longOpt("config").hasArg().desc("Configuration file path").build());
        options.addOption(Option.builder("s").longOpt("scenario").hasArg().desc("Test scenario name").build());

        CommandLineParser parser = new DefaultParser();
        try {
            CommandLine cmd = parser.parse(options, args);

            String action = cmd.getOptionValue("action");
            if (action == null) {
                System.err.println("Error: Action is required");
                printHelp(options);
                return;
            }

            String configPath = cmd.getOptionValue("config");
            if (configPath == null) {
                System.err.println("Error: Configuration file is required");
                printHelp(options);
                return;
            }

            TestCommand command = commands.get(action.toLowerCase());
            if (command == null) {
                System.err.println("Error: Invalid action. Supported actions: cleanup, prepare, check");
                printHelp(options);
                return;
            }

            command.execute(cmd);
        } catch (ParseException e) {
            System.err.println("Error parsing command line: " + e.getMessage());
            printHelp(options);
        } catch (Exception e) {
            System.err.println("Error executing command: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * Print help information
     * @param options Command line options
     */
    private static void printHelp(Options options) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("java -jar migration-tool-test.jar", options);
    }
}