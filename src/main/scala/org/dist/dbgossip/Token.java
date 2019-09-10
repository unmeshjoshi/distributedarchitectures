/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.dist.dbgossip;

import java.io.Serializable;
import java.nio.ByteBuffer;

public abstract class Token implements RingPosition<Token>, Serializable
{
    private static final long serialVersionUID = 1L;

    public static abstract class TokenFactory
    {
        public abstract ByteBuffer toByteArray(Token token);
        public abstract Token fromByteArray(ByteBuffer bytes);
        public abstract String toString(Token token); // serialize as string, not necessarily human-readable
        public abstract Token fromString(String string); // deserialize

        public abstract void validate(String token);
    }

    abstract public long getHeapSize();
    abstract public Object getTokenValue();

    /**
     * Returns a measure for the token space covered between this token and next.
     * Used by the token allocation algorithm (see CASSANDRA-7032).
     */
    abstract public double size(Token next);
    /**
     * Returns a token that is slightly greater than this. Used to avoid clashes
     * between nodes in separate datacentres trying to use the same token via
     * the token allocation algorithm.
     */
    abstract public Token increaseSlightly();

    public Token getToken()
    {
        return this;
    }

}
