/*
 * Copyright (C) 2012 Facebook, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License. You may obtain
 * a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package io.airlift.drift.transport.netty.scribe.drift;

import io.airlift.drift.annotations.ThriftConstructor;
import io.airlift.drift.annotations.ThriftField;
import io.airlift.drift.annotations.ThriftStruct;

@ThriftStruct
public final class LogEntry
{
    private final String category;
    private final String message;

    @ThriftConstructor
    public LogEntry(
            @ThriftField(name = "category") String category,
            @ThriftField(name = "message") String message)
    {
        this.category = category;
        this.message = message;
    }

    @ThriftField(1)
    public String getCategory()
    {
        return category;
    }

    @ThriftField(2)
    public String getMessage()
    {
        return message;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        LogEntry logEntry = (LogEntry) o;

        if (category != null ? !category.equals(logEntry.category) : logEntry.category != null) {
            return false;
        }
        if (message != null ? !message.equals(logEntry.message) : logEntry.message != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = category != null ? category.hashCode() : 0;
        result = 31 * result + (message != null ? message.hashCode() : 0);
        return result;
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append("LogEntryStruct");
        sb.append("{category='").append(category).append('\'');
        sb.append(", message='").append(message).append('\'');
        sb.append('}');
        return sb.toString();
    }
}