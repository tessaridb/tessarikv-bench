//! Process CPU over a phase, from `getrusage`: CPU and system time per operation, minor page
//! faults and context switches per operation. Printed beside each read run on both engines.

use crate::compression::float;

#[repr(C)]
#[derive(Default)]
struct TimeVal {
    seconds: i64,
    micros: i32,
}

#[repr(C)]
#[derive(Default)]
struct Usage {
    user: TimeVal,
    system: TimeVal,
    rest: [i64; 14],
}

unsafe extern "C" {
    fn getrusage(who: i32, usage: *mut Usage) -> i32;
}

/// Field indexes into `Usage::rest`: minor faults, voluntary and involuntary context switches.
const MINOR_FAULTS: usize = 4;
const VOLUNTARY_SWITCHES: usize = 12;
const INVOLUNTARY_SWITCHES: usize = 13;

/// What the process has used so far.
pub struct Sample {
    user: f64,
    system: f64,
    minor_faults: i64,
    voluntary_switches: i64,
    involuntary_switches: i64,
}

fn seconds(time: &TimeVal) -> f64 {
    let whole = u64::try_from(time.seconds).unwrap_or(0);
    float(whole) + f64::from(time.micros) / 1e6
}

/// Takes a sample of this process's usage.
pub fn sample() -> Sample {
    let mut usage = Usage::default();
    // SAFETY: `usage` is a valid, writable rusage of the platform's layout (RUSAGE_SELF = 0).
    let _ = unsafe { getrusage(0, &mut usage) };
    Sample {
        user: seconds(&usage.user),
        system: seconds(&usage.system),
        minor_faults: usage.rest[MINOR_FAULTS],
        voluntary_switches: usage.rest[VOLUNTARY_SWITCHES],
        involuntary_switches: usage.rest[INVOLUNTARY_SWITCHES],
    }
}

/// Prints the usage between `started` and now, per one of `operations`.
pub fn report(started: &Sample, operations: u64) {
    let ended = sample();
    let per = |delta: f64| delta / float(operations);
    let count = |end: i64, start: i64| float(u64::try_from(end.saturating_sub(start)).unwrap_or(0));
    eprintln!(
        "cpu us per get {:.3}, sys us per get {:.3}, minflt per get {:.4}, vcsw per get {:.4}, \
         ivcsw per get {:.4}",
        per(ended.user + ended.system - started.user - started.system) * 1e6,
        per(ended.system - started.system) * 1e6,
        per(count(ended.minor_faults, started.minor_faults)),
        per(count(ended.voluntary_switches, started.voluntary_switches)),
        per(count(
            ended.involuntary_switches,
            started.involuntary_switches
        )),
    );
}
