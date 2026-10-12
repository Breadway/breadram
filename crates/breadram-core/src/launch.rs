//! Launch a workload into a managed user scope.
//!
//! The wrapper is a client of systemd's user manager, not of the daemon. A
//! missing daemon, a missing user session, or a refused scope must not stop
//! the program from running: the caller falls back to a direct exec and says
//! so. Descendants stay in the scope because the scope is the parent cgroup,
//! which is how a fork keeps the parent's controller membership.

use crate::control::manager::UnitName;
use crate::control::ControlError;

/// The unit name prefix for scopes this wrapper creates.
pub const LAUNCH_SCOPE_PREFIX: &str = "breadram-launch-";

/// How a launch was placed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LaunchPlacement {
    /// The program runs inside a new user scope.
    Scope,
    /// The program runs in the caller's cgroup. The reason says why.
    Direct,
}

/// A planned launch. Nothing here starts a process.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LaunchPlan {
    /// Where the program will run.
    pub placement: LaunchPlacement,
    /// The scope unit, when one will be created.
    pub unit: Option<String>,
    /// Why a scope was not used. Empty when a scope is used.
    pub fallback: String,
    /// The argument vector to exec. For a scope this is `systemd-run` plus
    /// the program; for a direct launch it is the program itself.
    pub argv: Vec<String>,
}

/// Why a scope could not be planned.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum LaunchError {
    /// The command line cannot be launched.
    #[error("{0}")]
    Usage(String),
    /// The scope name was rejected.
    #[error(transparent)]
    Unit(#[from] ControlError),
}

/// Plan a launch into `breadram-launch-<name>.scope`.
///
/// `name` is a short identity, not a path. The program must be an absolute
/// path so a scope and a direct launch resolve the same binary. `scope_available`
/// is false when there is no user session (`XDG_RUNTIME_DIR` unset, or
/// `systemd-run` missing): the plan is then a direct exec and names the reason.
pub fn plan_launch(
    name: &str,
    program: &[String],
    scope_available: bool,
) -> Result<LaunchPlan, LaunchError> {
    if program.is_empty() {
        return Err(LaunchError::Usage(
            "launch needs a program after --".to_string(),
        ));
    }
    if !program[0].starts_with('/') {
        return Err(LaunchError::Usage(format!(
            "the program must be an absolute path, not {:?}",
            program[0]
        )));
    }
    let unit = scope_unit(name)?;
    if !scope_available {
        return Ok(LaunchPlan {
            placement: LaunchPlacement::Direct,
            unit: None,
            fallback: "no systemd user session: launching in the caller's cgroup".to_string(),
            argv: program.to_vec(),
        });
    }
    let mut argv = vec![
        "systemd-run".to_string(),
        "--user".to_string(),
        "--scope".to_string(),
        "--quiet".to_string(),
        "--collect".to_string(),
        format!("--unit={unit}"),
        "--".to_string(),
    ];
    argv.extend(program.iter().cloned());
    Ok(LaunchPlan {
        placement: LaunchPlacement::Scope,
        unit: Some(unit),
        fallback: String::new(),
        argv,
    })
}

/// The scope unit for a launch name.
///
/// The name is restricted the same way a unit name is: no path, no leading
/// dash, no shell metacharacters. Two launches with the same name share a
/// unit, which is the caller's choice.
pub fn scope_unit(name: &str) -> Result<String, LaunchError> {
    if name.is_empty() || name.len() > 64 {
        return Err(LaunchError::Usage(
            "the scope name must be 1 to 64 bytes".to_string(),
        ));
    }
    if !name
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
    {
        return Err(LaunchError::Usage(
            "the scope name may contain only ASCII letters, digits, '-' and '_'".to_string(),
        ));
    }
    let unit = format!("{LAUNCH_SCOPE_PREFIX}{name}.scope");
    UnitName::parse(&unit)?;
    Ok(unit)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_scope_launch_puts_the_program_after_the_separator() {
        let plan = plan_launch(
            "editor",
            &["/usr/bin/editor".to_string(), "notes".to_string()],
            true,
        )
        .unwrap();
        assert_eq!(plan.placement, LaunchPlacement::Scope);
        assert_eq!(plan.unit.as_deref(), Some("breadram-launch-editor.scope"));
        assert!(plan.fallback.is_empty());
        let split = plan.argv.iter().position(|arg| arg == "--").unwrap();
        assert_eq!(&plan.argv[split + 1..], ["/usr/bin/editor", "notes"]);
        assert!(plan.argv.iter().any(|arg| arg == "--user"));
        assert!(plan.argv.iter().any(|arg| arg == "--scope"));
    }

    #[test]
    fn a_missing_session_launches_directly_and_says_so() {
        let plan = plan_launch("editor", &["/usr/bin/editor".to_string()], false).unwrap();
        assert_eq!(plan.placement, LaunchPlacement::Direct);
        assert!(plan.unit.is_none());
        assert!(!plan.fallback.is_empty());
        assert_eq!(plan.argv, vec!["/usr/bin/editor".to_string()]);
    }

    #[test]
    fn a_relative_program_and_a_bad_name_are_refused() {
        let relative = plan_launch("editor", &["editor".to_string()], true).unwrap_err();
        assert!(matches!(relative, LaunchError::Usage(_)));
        let name = plan_launch("..", &["/bin/true".to_string()], true).unwrap_err();
        assert!(matches!(name, LaunchError::Usage(_)));
        assert!(plan_launch("editor", &[], true).is_err());
    }
}
