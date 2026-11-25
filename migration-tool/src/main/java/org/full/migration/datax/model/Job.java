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

package org.full.migration.datax.model;

import lombok.Data;

import java.util.ArrayList;
import java.util.List;

/**
 * Job
 * Represents the job section in DataX configuration.
 * A job contains the global settings and a list of content configurations
 * that define the data migration tasks.
 *
 * @since 2025-04-18
 */
@Data
public class Job {
    private Setting setting;
    private List<Content> content;

    /**
     * Default constructor that initializes with default settings and empty content list.
     */
    public Job() {
        this.setting = new Setting();
        this.content = new ArrayList<>();
    }

    /**
     * Constructor with specified settings and content.
     *
     * @param setting The job settings
     * @param content The list of content configurations
     */
    public Job(Setting setting, List<Content> content) {
        this.setting = setting;
        this.content = content;
    }

    /**
     * Add a content configuration to the job.
     *
     * @param contentItem The content configuration to add
     */
    public void addContent(Content contentItem) {
        if (this.content == null) {
            this.content = new ArrayList<>();
        }
        this.content.add(contentItem);
    }

    @Override
    public String toString() {
        return "Job{setting=" + setting + ", content=" + content + '}';
    }
}