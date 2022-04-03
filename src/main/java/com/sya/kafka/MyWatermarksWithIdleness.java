package com.sya.kafka;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.clock.Clock;
import org.apache.flink.util.clock.SystemClock;

import java.time.Duration;

public class MyWatermarksWithIdleness<T> implements WatermarkGenerator<T>{

    private final WatermarkGenerator<T> watermarks;
    private final MyWatermarksWithIdleness.IdlenessTimer idlenessTimer;

    public MyWatermarksWithIdleness(WatermarkGenerator<T> watermarks, Duration idleTimeout) {
        this(watermarks, idleTimeout, SystemClock.getInstance());
    }

    @VisibleForTesting
    MyWatermarksWithIdleness(WatermarkGenerator<T> watermarks, Duration idleTimeout, Clock clock) {
        Preconditions.checkNotNull(idleTimeout, "idleTimeout");
        Preconditions.checkArgument(!idleTimeout.isZero() && !idleTimeout.isNegative(), "idleTimeout must be greater than zero");
        this.watermarks = (WatermarkGenerator)Preconditions.checkNotNull(watermarks, "watermarks");
        this.idlenessTimer = new MyWatermarksWithIdleness.IdlenessTimer(clock, idleTimeout);
    }

    public void onEvent(T event, long eventTimestamp, WatermarkOutput output) {
        this.watermarks.onEvent(event, eventTimestamp, output);
        this.idlenessTimer.activity();
    }

    public void onPeriodicEmit(WatermarkOutput output) {
        if (this.idlenessTimer.checkIfIdle()) {
            output.markIdle();
        } else {
            this.watermarks.onPeriodicEmit(output);
        }

    }

    @VisibleForTesting
    static final class IdlenessTimer {
        private final Clock clock;
        private long counter;
        private long lastCounter;
        private long startOfInactivityNanos;
        private final long maxIdleTimeNanos;

        IdlenessTimer(Clock clock, Duration idleTimeout) {
            this.clock = clock;

            long idleNanos;
            try {
                idleNanos = idleTimeout.toNanos();
            } catch (ArithmeticException var6) {
                idleNanos = 9223372036854775807L;
            }

            this.maxIdleTimeNanos = idleNanos;
        }

        public void activity() {
            ++this.counter;
        }

        public boolean checkIfIdle() {
            if (this.counter != this.lastCounter) {
                this.lastCounter = this.counter;
                this.startOfInactivityNanos = 0L;
                return false;
            } else if (this.startOfInactivityNanos == 0L) {
                this.startOfInactivityNanos = this.clock.relativeTimeNanos();
                return false;
            } else {
                return this.clock.relativeTimeNanos() - this.startOfInactivityNanos > this.maxIdleTimeNanos;
            }
        }
    }
}
