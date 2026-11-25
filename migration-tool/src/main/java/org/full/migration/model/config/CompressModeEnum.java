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

package org.full.migration.model.config;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * CompressModeEnum
 *
 * @since 2025-04-18
 */
@Getter
@AllArgsConstructor
public enum CompressModeEnum {
    DELTA(0),
    PREFIX(1),
    DICTIONARY(2),
    NUMSTR(3),
    NOCOMPRESS(4);

    private int type;
}
