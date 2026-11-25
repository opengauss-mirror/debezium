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

/**
 * Speed
 * Represents the speed section in DataX configuration.
 * Controls the concurrency level of data transfer through channel settings.
 *
 * @since 2025-04-18
 */
@Data
public class Speed {
    private int channel;

    /**
     * Default constructor that initializes with 3 channels.
     */
    public Speed() {
        this.channel = 3;
    }

    /**
     * Constructor with specified channel count.
     *
     * @param channel The number of concurrent channels for data transfer
     */
    public Speed(int channel) {
        this.channel = channel;
    }

    @Override
    public String toString() {
        return "Speed{channel=" + channel + '}';
    }
}