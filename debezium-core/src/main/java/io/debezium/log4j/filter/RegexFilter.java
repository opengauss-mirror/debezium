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

package io.debezium.log4j.filter;

import org.apache.log4j.spi.Filter;
import org.apache.log4j.spi.LoggingEvent;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * regex filter
 *
 * @since 2024/12/11
 */
public class RegexFilter extends Filter {
    private Pattern pattern;

    public void setRegex(String regex) {
        this.pattern = Pattern.compile(regex);
    }

    @Override
    public int decide(LoggingEvent loggingEvent) {
        if (pattern == null) {
            return Filter.NEUTRAL;
        }

        Matcher matcher = pattern.matcher(loggingEvent.getRenderedMessage());
        if (matcher.matches()) {
            return Filter.ACCEPT;
        } else {
            return Filter.DENY;
        }
    }
}
