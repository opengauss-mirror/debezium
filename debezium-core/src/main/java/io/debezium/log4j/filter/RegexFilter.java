/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2024. All rights reserved.
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
