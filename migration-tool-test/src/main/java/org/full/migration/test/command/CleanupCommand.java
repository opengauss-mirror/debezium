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
import org.full.migration.test.manager.TestEnvironmentManager;

/**
 * CleanupCommand
 * Command to clean up test environment
 * Implements the TestCommand interface to provide cleanup functionality
 */
public class CleanupCommand implements TestCommand {
    @Override
    public void execute(CommandLine cmd) throws Exception {
        String configPath = cmd.getOptionValue("--config");
        TestEnvironmentManager manager = new TestEnvironmentManager(configPath);
        manager.cleanup();
    }
}