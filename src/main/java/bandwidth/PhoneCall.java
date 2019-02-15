/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package bandwidth;

import java.nio.ByteBuffer;

public class PhoneCall {
    private long id;
    private long startTime;
    private long endTime;

    private PhoneCall() {
    }

    public PhoneCall(long id, long startTime) {
        this.id = id;
        this.startTime = startTime;

        endTime = -1;
    }

    public void setEndTime(long endTime) {
        this.endTime = endTime;
    }

    public long getId() {
        return id;
    }

    public long getStartTime() {
        return startTime;
    }

    public long getEndTime() {
        return endTime;
    }

    public byte[] toBytes() {
        ByteBuffer buf = ByteBuffer.allocate(Long.BYTES * 3);

        buf.putLong(id);
        buf.putLong(startTime);
        buf.putLong(endTime);

        return buf.array();
    }

    public static PhoneCall fromBytes(byte[] bytes) {
        ByteBuffer buf = ByteBuffer.wrap(bytes);

        PhoneCall call = new PhoneCall();

        call.id = buf.getLong();
        call.startTime = buf.getLong();
        call.endTime = buf.getLong();

        return call;
    }

    @Override public String toString() {
        return "PhoneCall [" +
            "id=" + id +
            ", startTime=" + startTime +
            ", endTime=" + endTime +
            ']';
    }
}
