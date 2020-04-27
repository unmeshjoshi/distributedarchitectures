package org.dist.patterns.singularupdatequeue;

import org.dist.utils.JTestUtils;
import org.junit.Test;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;


@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 5, timeUnit = TimeUnit.MICROSECONDS)
@Measurement(iterations = 5, timeUnit = TimeUnit.MICROSECONDS)
public class SingularUpdateQueueAccountContention {
    private static final int NUMBER_OF_THREADS = 4;

    @State(Scope.Group)
    public static class Contended
    {
        final SingleThreadedAccount account = new SingleThreadedAccount(100, JTestUtils.tmpDir("perf"));
    }

    public void main() throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(SingularUpdateQueueAccountContention.class.getName())
                .verbosity(VerboseMode.SILENT)
                .build();
        new Runner(opt).run();
    }


    @Benchmark
    @Group("SingularUpdateQueueAccountContention")
    @GroupThreads(value = NUMBER_OF_THREADS)
    public void contended(SingularUpdateQueueAccountContention.Contended state) {
        CompletableFuture<Response> credit = state.account.credit(1);
//        try {
//            Response response = credit.get();
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        } catch (ExecutionException e) {
//            e.printStackTrace();
//        }
    }
}