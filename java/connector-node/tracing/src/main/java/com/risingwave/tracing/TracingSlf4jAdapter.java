// Copyright 2024 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Ported from https://github.com/MrFriendly-B-V/tracing-slf4j,
// which is licensed under the Apache License, Version 2.0.

package com.risingwave.tracing;

// Import log4j's ParameterizedMessage, so that we can format the messages
// with the same interpolation as log4j (i.e. "{}" instead of "%s").
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.slf4j.Logger;
import org.slf4j.Marker;

public class TracingSlf4jAdapter implements Logger {

    private final String name;

    private void logIfEnabled(int level, String msg) {
        if (TracingSlf4jImpl.isEnabled(level)) {
            TracingSlf4jImpl.event(name, level, msg);
        }
    }

    private void logIfEnabled(int level, String format, Object arg) {
        if (TracingSlf4jImpl.isEnabled(level)) {
            var pm = new ParameterizedMessage(format, arg);
            if (null != pm.getThrowable()) {
                logIfEnabled(level, pm.getFormattedMessage(), pm.getThrowable());
            } else {
                TracingSlf4jImpl.event(name, level, pm.getFormattedMessage());
            }
        }
    }

    private void logIfEnabled(int level, String format, Object arg1, Object arg2) {
        if (TracingSlf4jImpl.isEnabled(level)) {
            var pm = new ParameterizedMessage(format, arg1, arg2);
            if (null != pm.getThrowable()) {
                logIfEnabled(level, pm.getFormattedMessage(), pm.getThrowable());
            } else {
                TracingSlf4jImpl.event(name, level, pm.getFormattedMessage());
            }
        }
    }

    private void logIfEnabled(int level, String format, Object... arguments) {
        if (TracingSlf4jImpl.isEnabled(level)) {
            var pm = new ParameterizedMessage(format, arguments);
            if (null != pm.getThrowable()) {
                logIfEnabled(level, pm.getFormattedMessage(), pm.getThrowable());
            } else {
                TracingSlf4jImpl.event(name, level, pm.getFormattedMessage());
            }
        }
    }

    private void logIfEnabled(int level, String msg, Throwable t) {
        if (TracingSlf4jImpl.isEnabled(level)) {
            String sStackTrace = ExceptionUtils.getStackTrace(t);
            TracingSlf4jImpl.event(name, level, String.format("%s: %s", msg, sStackTrace));
        }
    }

    public TracingSlf4jAdapter(String name) {
        this.name = name;
    }

    @Override
    public String getName() {
        return this.name;
    }

    @Override
    public boolean isTraceEnabled() {
        return TracingSlf4jImpl.isEnabled(TracingSlf4jImpl.TRACE);
    }

    @Override
    public void trace(String msg) {
        logIfEnabled(TracingSlf4jImpl.TRACE, msg);
    }

    @Override
    public void trace(String format, Object arg) {
        logIfEnabled(TracingSlf4jImpl.TRACE, format, arg);
    }

    @Override
    public void trace(String format, Object arg1, Object arg2) {
        logIfEnabled(TracingSlf4jImpl.TRACE, format, arg1, arg2);
    }

    @Override
    public void trace(String format, Object... arguments) {
        logIfEnabled(TracingSlf4jImpl.TRACE, format, arguments);
    }

    @Override
    public void trace(String msg, Throwable t) {
        logIfEnabled(TracingSlf4jImpl.TRACE, msg, t);
    }

    @Override
    public boolean isTraceEnabled(Marker marker) {
        return isTraceEnabled();
    }

    @Override
    public void trace(Marker marker, String msg) {
        trace(msg);
    }

    @Override
    public void trace(Marker marker, String format, Object arg) {
        trace(format, arg);
    }

    @Override
    public void trace(Marker marker, String format, Object arg1, Object arg2) {
        trace(format, arg1, arg2);
    }

    @Override
    public void trace(Marker marker, String format, Object... arguments) {
        trace(format, arguments);
    }

    @Override
    public void trace(Marker marker, String msg, Throwable t) {
        trace(msg, t);
    }

    @Override
    public boolean isDebugEnabled() {
        return TracingSlf4jImpl.isEnabled(TracingSlf4jImpl.DEBUG);
    }

    @Override
    public void debug(String msg) {
        logIfEnabled(TracingSlf4jImpl.DEBUG, msg);
    }

    @Override
    public void debug(String format, Object arg) {
        logIfEnabled(TracingSlf4jImpl.DEBUG, format, arg);
    }

    @Override
    public void debug(String format, Object arg1, Object arg2) {
        logIfEnabled(TracingSlf4jImpl.DEBUG, format, arg1, arg2);
    }

    @Override
    public void debug(String format, Object... arguments) {
        logIfEnabled(TracingSlf4jImpl.DEBUG, format, arguments);
    }

    @Override
    public void debug(String msg, Throwable t) {
        logIfEnabled(TracingSlf4jImpl.DEBUG, msg, t);
    }

    @Override
    public boolean isDebugEnabled(Marker marker) {
        return isDebugEnabled();
    }

    @Override
    public void debug(Marker marker, String msg) {
        debug(msg);
    }

    @Override
    public void debug(Marker marker, String format, Object arg) {
        debug(format, arg);
    }

    @Override
    public void debug(Marker marker, String format, Object arg1, Object arg2) {
        debug(format, arg1, arg2);
    }

    @Override
    public void debug(Marker marker, String format, Object... arguments) {
        debug(format, arguments);
    }

    @Override
    public void debug(Marker marker, String msg, Throwable t) {
        debug(msg, t);
    }

    @Override
    public boolean isInfoEnabled() {
        return TracingSlf4jImpl.isEnabled(TracingSlf4jImpl.INFO);
    }

    @Override
    public void info(String msg) {
        logIfEnabled(TracingSlf4jImpl.INFO, msg);
    }

    @Override
    public void info(String format, Object arg) {
        logIfEnabled(TracingSlf4jImpl.INFO, format, arg);
    }

    @Override
    public void info(String format, Object arg1, Object arg2) {
        logIfEnabled(TracingSlf4jImpl.INFO, format, arg1, arg2);
    }

    @Override
    public void info(String format, Object... arguments) {
        logIfEnabled(TracingSlf4jImpl.INFO, format, arguments);
    }

    @Override
    public void info(String msg, Throwable t) {
        logIfEnabled(TracingSlf4jImpl.INFO, msg, t);
    }

    @Override
    public boolean isInfoEnabled(Marker marker) {
        return isInfoEnabled();
    }

    @Override
    public void info(Marker marker, String msg) {
        info(msg);
    }

    @Override
    public void info(Marker marker, String format, Object arg) {
        info(format, arg);
    }

    @Override
    public void info(Marker marker, String format, Object arg1, Object arg2) {
        info(format, arg1, arg2);
    }

    @Override
    public void info(Marker marker, String format, Object... arguments) {
        info(format, arguments);
    }

    @Override
    public void info(Marker marker, String msg, Throwable t) {
        info(msg, t);
    }

    @Override
    public boolean isWarnEnabled() {
        return TracingSlf4jImpl.isEnabled(TracingSlf4jImpl.WARN);
    }

    @Override
    public void warn(String msg) {
        logIfEnabled(TracingSlf4jImpl.WARN, msg);
    }

    @Override
    public void warn(String format, Object arg) {
        logIfEnabled(TracingSlf4jImpl.WARN, format, arg);
    }

    @Override
    public void warn(String format, Object arg1, Object arg2) {
        logIfEnabled(TracingSlf4jImpl.WARN, format, arg1, arg2);
    }

    @Override
    public void warn(String format, Object... arguments) {
        logIfEnabled(TracingSlf4jImpl.WARN, format, arguments);
    }

    @Override
    public void warn(String msg, Throwable t) {
        logIfEnabled(TracingSlf4jImpl.WARN, msg, t);
    }

    @Override
    public boolean isWarnEnabled(Marker marker) {
        return isWarnEnabled();
    }

    @Override
    public void warn(Marker marker, String msg) {
        warn(msg);
    }

    @Override
    public void warn(Marker marker, String format, Object arg) {
        warn(format, arg);
    }

    @Override
    public void warn(Marker marker, String format, Object arg1, Object arg2) {
        warn(format, arg1, arg2);
    }

    @Override
    public void warn(Marker marker, String format, Object... arguments) {
        warn(format, arguments);
    }

    @Override
    public void warn(Marker marker, String msg, Throwable t) {
        warn(msg, t);
    }

    @Override
    public boolean isErrorEnabled() {
        return TracingSlf4jImpl.isEnabled(TracingSlf4jImpl.ERROR);
    }

    @Override
    public void error(String msg) {
        logIfEnabled(TracingSlf4jImpl.ERROR, msg);
    }

    @Override
    public void error(String format, Object arg) {
        logIfEnabled(TracingSlf4jImpl.ERROR, format, arg);
    }

    @Override
    public void error(String format, Object arg1, Object arg2) {
        logIfEnabled(TracingSlf4jImpl.ERROR, format, arg1, arg2);
    }

    @Override
    public void error(String format, Object... arguments) {
        logIfEnabled(TracingSlf4jImpl.ERROR, format, arguments);
    }

    @Override
    public void error(String msg, Throwable t) {
        logIfEnabled(TracingSlf4jImpl.ERROR, msg, t);
    }

    @Override
    public boolean isErrorEnabled(Marker marker) {
        return isErrorEnabled();
    }

    @Override
    public void error(Marker marker, String msg) {
        error(msg);
    }

    @Override
    public void error(Marker marker, String format, Object arg) {
        error(format, arg);
    }

    @Override
    public void error(Marker marker, String format, Object arg1, Object arg2) {
        error(format, arg1, arg2);
    }

    @Override
    public void error(Marker marker, String format, Object... arguments) {
        error(format, arguments);
    }

    @Override
    public void error(Marker marker, String msg, Throwable t) {
        error(msg, t);
    }
}
