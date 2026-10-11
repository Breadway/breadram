//! Parsers for the procfs files the inventory reads.
//!
//! All parsing is pure and takes `&str`, so it is testable from fixed fixtures
//! regardless of what is running on the host.

/// Index of `starttime` within the fields that follow the command field.
///
/// `/proc/<pid>/stat` is field 1 pid, field 2 comm, field 3 state, and so on up
/// to field 22 starttime. Split after the command, the first token is field 3,
/// so starttime sits at index 22 - 3.
const AFTER_COMM_STARTTIME: usize = 19;
/// Index of `ppid` within the fields that follow the command field (field 4).
const AFTER_COMM_PPID: usize = 1;

/// The parts of `/proc/<pid>/stat` breadram needs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProcStat {
    /// Process id.
    pub pid: u32,
    /// The command name, which may contain spaces and parentheses.
    pub comm: String,
    /// The single character process state.
    pub state: char,
    /// Parent process id.
    pub ppid: u32,
    /// Start time in clock ticks since boot. Combined with the pid this is the
    /// process generation, which is what makes identity reuse detectable.
    pub starttime: u64,
}

/// A `/proc/<pid>/stat` parse failure.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ProcParseError {
    /// The command field, which is wrapped in parentheses, was absent.
    #[error("stat line has no command field")]
    MissingComm,
    /// The leading pid was not a number.
    #[error("stat line has a non numeric pid")]
    BadPid,
    /// One of the fixed fields was absent.
    #[error("stat line is missing the {0} field")]
    MissingField(&'static str),
    /// A fixed field was not a number.
    #[error("stat field {field} is not a number: {value:?}")]
    BadNumber {
        /// The field name.
        field: &'static str,
        /// The offending value.
        value: String,
    },
}

/// Parse `/proc/<pid>/stat`.
///
/// The command field is wrapped in parentheses and may itself contain spaces and
/// parentheses, so it is located by the first `(` and the last `)`.
pub fn parse_proc_stat(contents: &str) -> Result<ProcStat, ProcParseError> {
    let open = contents.find('(').ok_or(ProcParseError::MissingComm)?;
    let close = contents.rfind(')').ok_or(ProcParseError::MissingComm)?;
    if close < open {
        return Err(ProcParseError::MissingComm);
    }

    let pid: u32 = contents[..open]
        .trim()
        .parse()
        .map_err(|_| ProcParseError::BadPid)?;
    let comm = contents[open + 1..close].to_string();
    let rest: Vec<&str> = contents[close + 1..].split_whitespace().collect();

    let state = rest
        .first()
        .and_then(|token| token.chars().next())
        .ok_or(ProcParseError::MissingField("state"))?;
    let starttime = number(&rest, AFTER_COMM_STARTTIME, "starttime")?;

    let ppid_token = rest
        .get(AFTER_COMM_PPID)
        .ok_or(ProcParseError::MissingField("ppid"))?;
    let ppid: u32 = ppid_token.parse().map_err(|_| ProcParseError::BadNumber {
        field: "ppid",
        value: ppid_token.to_string(),
    })?;

    Ok(ProcStat {
        pid,
        comm,
        state,
        ppid,
        starttime,
    })
}

fn number(rest: &[&str], index: usize, field: &'static str) -> Result<u64, ProcParseError> {
    let token = rest.get(index).ok_or(ProcParseError::MissingField(field))?;
    token.parse().map_err(|_| ProcParseError::BadNumber {
        field,
        value: token.to_string(),
    })
}

/// Selected `/proc/<pid>/status` values, in bytes.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ProcStatus {
    /// `Name`.
    pub name: Option<String>,
    /// `VmRSS`, resident set size.
    pub rss_bytes: Option<u64>,
    /// `VmSwap`, swapped out resident memory.
    pub swap_bytes: Option<u64>,
}

/// Parse `/proc/<pid>/status`.
///
/// Lenient: the file carries many fields and they differ by kernel, so unknown
/// and malformed lines are skipped. Fields that are absent stay `None` so a
/// missing value is never mistaken for zero.
pub fn parse_proc_status(contents: &str) -> ProcStatus {
    let mut status = ProcStatus::default();
    for raw in contents.lines() {
        let Some((key, rest)) = raw.split_once(':') else {
            continue;
        };
        let mut parts = rest.split_whitespace();
        let Some(value) = parts.next() else {
            continue;
        };
        match key.trim() {
            "Name" => status.name = Some(value.to_string()),
            "VmRSS" => status.rss_bytes = to_bytes(value, parts.next()),
            "VmSwap" => status.swap_bytes = to_bytes(value, parts.next()),
            _ => {}
        }
    }
    status
}

fn to_bytes(value: &str, unit: Option<&str>) -> Option<u64> {
    let number: u64 = value.parse().ok()?;
    match unit {
        None => Some(number),
        Some("kB") => Some(number.saturating_mul(1024)),
        Some(_) => None,
    }
}

/// The cgroup v2 path from `/proc/<pid>/cgroup`.
///
/// A cgroup v2 line is `0::/path`. On a v1 host there is no `0::` line and this
/// returns `None`, which callers report as a degraded capability.
pub fn parse_cgroup_path(contents: &str) -> Option<String> {
    for line in contents.lines() {
        let mut parts = line.splitn(3, ':');
        let (Some(_hierarchy), Some(controllers), Some(path)) =
            (parts.next(), parts.next(), parts.next())
        else {
            continue;
        };
        if controllers.is_empty() && !path.trim().is_empty() {
            return Some(path.trim().to_string());
        }
    }
    None
}

/// The systemd unit or scope name at the end of a cgroup path.
///
/// Slices are grouping levels rather than workloads, so they return `None`.
pub fn systemd_unit(path: &str) -> Option<String> {
    let last = path.rsplit('/').next()?;
    if last.ends_with(".service") || last.ends_with(".scope") {
        Some(last.to_string())
    } else {
        None
    }
}

/// The executable identity from `/proc/<pid>/cmdline`.
///
/// The file is NUL separated. The first field is `argv[0]`, which is reduced to
/// its file name. Kernel threads have an empty cmdline and return `None`.
pub fn parse_cmdline(contents: &str) -> Option<String> {
    let first = contents.split('\0').next()?.trim();
    if first.is_empty() {
        return None;
    }
    let name = first.rsplit('/').next().unwrap_or(first);
    if name.is_empty() {
        None
    } else {
        Some(name.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_a_stat_line_with_spaces_and_parens_in_comm() {
        // comm is "(my app (v2))" and there are trailing fields after starttime.
        let line = "1234 (my app (v2)) S 1 1234 1234 0 -1 4194560 100 0 0 0 5 6 0 0 20 0 3 0 999999 12345 678 \
                    0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0";
        let stat = parse_proc_stat(line).unwrap();
        assert_eq!(stat.pid, 1234);
        assert_eq!(stat.comm, "my app (v2)");
        assert_eq!(stat.state, 'S');
        assert_eq!(stat.ppid, 1);
        assert_eq!(stat.starttime, 999999);
    }

    #[test]
    fn a_stat_line_without_comm_is_an_error() {
        assert_eq!(
            parse_proc_stat("1234 no parens here"),
            Err(ProcParseError::MissingComm)
        );
    }

    #[test]
    fn a_stat_line_with_a_too_short_field_list_is_an_error() {
        assert!(matches!(
            parse_proc_stat("1234 (x) S 1"),
            Err(ProcParseError::MissingField("starttime"))
        ));
    }

    #[test]
    fn a_non_numeric_starttime_is_reported() {
        let mut fields = vec!["S"; 20];
        fields[1] = "1";
        fields[19] = "abc";
        let line = format!("7 (x) {}", fields.join(" "));
        assert!(matches!(
            parse_proc_stat(&line),
            Err(ProcParseError::BadNumber {
                field: "starttime",
                ..
            })
        ));
    }

    #[test]
    fn parses_status_memory() {
        let contents =
            "Name:\tollama\nState:\tS (sleeping)\nVmRSS:\t  1048576 kB\nVmSwap:\t       0 kB\n";
        let status = parse_proc_status(contents);
        assert_eq!(status.name.as_deref(), Some("ollama"));
        assert_eq!(status.rss_bytes, Some(1_073_741_824));
        assert_eq!(status.swap_bytes, Some(0));
    }

    #[test]
    fn missing_status_fields_stay_none() {
        let status = parse_proc_status("Name:\tfoo\n");
        assert_eq!(status.rss_bytes, None);
        assert_eq!(status.swap_bytes, None);
    }

    #[test]
    fn unknown_units_do_not_become_zero() {
        let status = parse_proc_status("VmRSS:\t10 MB\n");
        assert_eq!(status.rss_bytes, None);
    }

    #[test]
    fn parses_a_cgroup_v2_path() {
        let contents = "0::/user.slice/user-1000.slice/user@1000.service/kitty-875185-0.scope\n";
        assert_eq!(
            parse_cgroup_path(contents).as_deref(),
            Some("/user.slice/user-1000.slice/user@1000.service/kitty-875185-0.scope")
        );
    }

    #[test]
    fn a_cgroup_v1_host_has_no_v2_path() {
        let contents = "11:memory:/user.slice\n10:cpu:/user.slice\n";
        assert_eq!(parse_cgroup_path(contents), None);
    }

    #[test]
    fn extracts_a_systemd_unit_but_not_a_slice() {
        assert_eq!(
            systemd_unit("/user.slice/user-1000.slice/app.service").as_deref(),
            Some("app.service")
        );
        assert_eq!(
            systemd_unit("/user.slice/user-1000.slice/run-123.scope").as_deref(),
            Some("run-123.scope")
        );
        assert_eq!(systemd_unit("/user.slice/user-1000.slice"), None);
    }

    #[test]
    fn parses_cmdline_to_a_file_name() {
        assert_eq!(
            parse_cmdline("/usr/bin/ollama\0serve\0").as_deref(),
            Some("ollama")
        );
        assert_eq!(
            parse_cmdline("python3\0train.py\0").as_deref(),
            Some("python3")
        );
    }

    #[test]
    fn a_kernel_thread_has_no_executable() {
        assert_eq!(parse_cmdline("\0"), None);
        assert_eq!(parse_cmdline(""), None);
    }
}
