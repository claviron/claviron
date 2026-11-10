use super::*;
use crate::TaskExecutorKind;

/// The codec matches kinds as strings, so the compiler cannot see a missing
/// arm: every kind and schedule a task can have must cross kind 218.
#[test]
fn every_task_kind_and_schedule_crosses_the_task_info_codec() {
    let kinds = [
        TaskExecutorKind::Builtin,
        TaskExecutorKind::Command,
        TaskExecutorKind::Php,
        TaskExecutorKind::Js,
        TaskExecutorKind::Lua,
    ];
    for kind in kinds {
        for schedule in ["cron", "interval", "daemon", "manual"] {
            let info = TaskInfo {
                kind: kind.as_str().to_owned(),
                schedule: schedule.to_owned(),
            };
            let mut bytes = Vec::new();
            write_task_info(&mut bytes, Some(&info)).unwrap();
            let mut offset = 0;
            let read = read_task_info(&bytes, &mut offset).unwrap().unwrap();
            assert_eq!((read.kind, read.schedule), (info.kind, info.schedule));
        }
    }
}
