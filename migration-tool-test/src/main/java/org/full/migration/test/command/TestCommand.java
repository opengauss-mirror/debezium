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

package org.full.migration.test.command;

import org.apache.commons.cli.CommandLine;

/**
 * TestCommand
 * Interface for test commands
 * Defines the contract for implementing command classes that can be executed
 */
public interface TestCommand {
    /**
     * Execute the command
     * @param cmd Parsed command line arguments
     * @throws Exception if an error occurs during execution
     */
    void execute(CommandLine cmd) throws Exception;
}