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

package org.dist.patterns.ignite.cluster;


import java.util.Collection;
import java.util.Map;
import java.util.UUID;

public interface ClusterNode {
    public UUID id();

    public Object consistentId();
    public <T> T attribute(String name);

    public Map<String, Object> attributes();
    public Collection<String> addresses();
    public Collection<String> hostNames();
    public long order();
    default public Integer version() {return 0;};
    public boolean isLocal();
    public boolean isDaemon();
    public boolean isClient();
}