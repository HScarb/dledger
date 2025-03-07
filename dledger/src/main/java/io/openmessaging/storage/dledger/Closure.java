/*
 * Copyright 2017-2022 The DLedger Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.openmessaging.storage.dledger;

public abstract class Closure {

    protected long createTime = System.currentTimeMillis();

    protected long timeoutMs = 2000;

    public Closure() {

    }

    public Closure(long timeoutMs) {
        this.timeoutMs = timeoutMs;
    }

    public boolean isTimeOut() {
        return System.currentTimeMillis() - createTime >= timeoutMs;
    }

    abstract void done(Status status);
}
