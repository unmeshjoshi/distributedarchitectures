package org.dist.patterns.singularupdatequeue;

import org.dist.utils.JTestUtils;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 5, timeUnit = TimeUnit.MICROSECONDS)
@Measurement(iterations = 5, timeUnit = TimeUnit.MICROSECONDS)
@Fork(value = 3, warmups = 1, jvmArgsAppend = { "-XX:-UseBiasedLocking", "-XX:+UseHeavyMonitors"})
public class SynchronizedAccountContention {

    @State(Scope.Group)
    public static class Contended
    {
        final SynchronizedAccount account = new SynchronizedAccount(100, JTestUtils.tmpDir("perf"));
    }

    public void main() throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(SynchronizedAccountContention.class.getName())
                .verbosity(VerboseMode.SILENT)
                .build();
        new Runner(opt).run();
    }

    @Benchmark
    @Group("SynchronizedAccountContention")
    @GroupThreads(100)
    public long contended(Contended state) {
        return state.account.credit(1);
    }
}