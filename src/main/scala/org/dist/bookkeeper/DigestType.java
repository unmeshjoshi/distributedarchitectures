/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.dist.bookkeeper;

/**
 * Digest type.
 *
 * @since 4.6
 */
public enum DigestType {

    /**
     * Entries are verified by applied CRC32 algorithm.
     */
    CRC32,
    /**
     * Entries are verified by applied MAC algorithm.
     */
    MAC,
    /**
     * Entries are verified by applied CRC32C algorithm.
     */
    CRC32C,
    /**
     * Entries are not verified.
     */
    DUMMY,
    ;

    public DigestType toApiDigestType() {
        switch (this) {
            case MAC:
                return DigestType.MAC;
            case CRC32:
                return DigestType.CRC32;
            case CRC32C:
                return DigestType.CRC32C;
            case DUMMY:
                return DigestType.DUMMY;
            default:
                throw new IllegalArgumentException("Unable to convert digest type " + this);
        }
    }
}
