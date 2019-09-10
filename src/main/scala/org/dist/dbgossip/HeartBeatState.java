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

/**
 * HeartBeat State associated with any given endpoint.
 */
public class HeartBeatState
{
    private volatile int generation;
    private volatile int version;

    HeartBeatState(int gen)
    {
        this(gen, 0);
    }

    public HeartBeatState(int gen, int ver)
    {
        generation = gen;
        version = ver;
    }

    int getGeneration()
    {
        return generation;
    }

    void updateHeartBeat()
    {
        version = VersionGenerator.getNextVersion();
    }

    int getHeartBeatVersion()
    {
        return version;
    }

    void forceNewerGenerationUnsafe()
    {
        generation += 1;
    }

    void forceHighestPossibleVersionUnsafe()
    {
        version = Integer.MAX_VALUE;
    }

    public String toString()
    {
        return String.format("HeartBeat: generation = %d, version = %d", generation, version);
    }
}

