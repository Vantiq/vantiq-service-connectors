package io.vantiq.common;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.val;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * Namespace for helper routines for handling interval durations.
 * <p></p>
 * Created by sfitts on 7/26/16.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class DurationHelper {

    /**
     * Parse a duration string and return the value in milliseconds.
     * 
     * @param durationStr the VAIL "interval constant" to parse
     * @return milliseconds in specified interval
     */
    public static long parseDurationString(String durationStr) {
        // Make sure we have a legal string
        Objects.requireNonNull(durationStr);
        String[] parts = durationStr.split(" ");
        if (parts.length != 2) {
            val msg = String.format("Unable to parse duration string '%s'.  Expected number followed by time unit.",
                    durationStr);
            throw new IllegalArgumentException(msg);
        }

        // Parse duration
        long duration = Long.parseLong(parts[0]);
        return DurationHelper.convert(parts[1], duration);
    }

    /**
     * Convert the given interval into milliseconds.
     *
     * @param sourceUnits time unit for source value
     * @param sourceVal the source value
     * @return the converted value
     */
    public static long convert(String sourceUnits, long sourceVal) {
        return convert(sourceUnits, sourceVal, null);
    }

    /**
     * Convert the given interval into the specified target units.  If no target units are provided then the
     * conversion is to milliseconds.
     * <p></p>
     * For conversion purposes assume a year is 365 days and a month is 30 days.
     * <p></p>
     * Time units are now CASE INSENSITIVE.
     *
     * @param sourceUnits time unit for source value
     * @param sourceVal the source value
     * @param targetUnits time unit for conversion target
     * @return the converted value
     */
    public static long convert(String sourceUnits, long sourceVal, String targetUnits) {
        // Start by converting to milliseconds
        long src;
        switch (sourceUnits.toLowerCase()) {
            case "year":
            case "years":
            case "y":
                src = TimeUnit.DAYS.toMillis(sourceVal) * 365;
                break;

            case "month":
            case "months":
                src = TimeUnit.DAYS.toMillis(sourceVal) * 30;
                break;

            case "week":
            case "weeks":
            case "w":
                src = TimeUnit.DAYS.toMillis(sourceVal) * 7;
                break;

            case "day":
            case "days":
            case "d":
            case "epochdays":
                src = TimeUnit.DAYS.toMillis(sourceVal);
                break;

            case "hour":
            case "hours":
            case "h":
            case "epochhours":
                src = TimeUnit.HOURS.toMillis(sourceVal);
                break;

            case "minute":
            case "minutes":
            case "m":
            case "epochminutes":
                src = TimeUnit.MINUTES.toMillis(sourceVal);
                break;

            case "second":
            case "seconds":
            case "s":
            case "epochseconds":
                src = TimeUnit.SECONDS.toMillis(sourceVal);
                break;

            case "millisecond":
            case "milliseconds":
            case "ms":
            case "epochmilliseconds":
                src = TimeUnit.MILLISECONDS.toMillis(sourceVal);
                break;

            case "microsecond":
            case "microseconds":
            case "us":
            case "u":
            case "epochmicroseconds":
                src = TimeUnit.MICROSECONDS.toMillis(sourceVal);
                break;

            case "nanosecond":
            case "nanoseconds":
            case "ns":
                src = TimeUnit.NANOSECONDS.toMillis(sourceVal);
                break;

            default:
                throw new IllegalArgumentException("Unrecognized time unit " + sourceUnits);
        }

        if (targetUnits == null) {
            return src;
        }
        //
        // Convert to target units
        //
        long rslt;
        switch (targetUnits.toLowerCase()) {
            case "year":
            case "years":
                rslt = (TimeUnit.MILLISECONDS.toDays(src)/365);
                break;

            case "month":
            case "months":
                rslt = (TimeUnit.MILLISECONDS.toDays(src)/30);
                break;

            case "week":
            case "weeks":
                rslt = (TimeUnit.MILLISECONDS.toDays(src)/7);
                break;

            case "day":
            case "days":
            case "epochdays":
                rslt = TimeUnit.MILLISECONDS.toDays(src);
                break;

            case "hour":
            case "hours":
            case "epochhours":
                rslt = TimeUnit.MILLISECONDS.toHours(src);
                break;

            case "minute":
            case "minutes":
            case "epochminutes":
                rslt = TimeUnit.MILLISECONDS.toMinutes(src);
                break;

            case "s":
            case "second":
            case "seconds":
            case "epochseconds":
                rslt = TimeUnit.MILLISECONDS.toSeconds(src);
                break;

            case "ms":
            case "millisecond":
            case "milliseconds":
            case "epochmilliseconds":
                rslt = TimeUnit.MILLISECONDS.toMillis(src);
                break;

            case "us":
            case "microsecond":
            case "microseconds":
            case "epochmicroseconds":
                rslt = TimeUnit.MILLISECONDS.toMicros(src);
                break;

            case "ns":
            case "nanosecond":
            case "nanoseconds":
            case "epochnanoseconds":
                rslt = TimeUnit.MILLISECONDS.toNanos(src);
                break;
            default:
                throw new IllegalArgumentException("Unrecognized target time unit " + targetUnits);
        }
        return rslt;
    }
}
