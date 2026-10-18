use super::*;

#[test]
fn runtime_qualified_command_round_trips() {
    let command = ApplicationFleetCommand {
        action: ApplicationCommandAction::Restart,
        id: ApplicationId::new(
            ApplicationRuntime::new("js").unwrap(),
            ApplicationName::new("wallet").unwrap(),
        ),
    };

    assert_eq!(
        ApplicationFleetCommand::decode_orbit(&command.encode_orbit()).unwrap(),
        command
    );
}

#[test]
fn codec_rejects_missing_identity_components() {
    assert!(ApplicationFleetCommand::decode_orbit(&[1, 0, 0]).is_err());
    assert!(ApplicationFleetCommand::decode_orbit(&[1, 2, 0, b'j']).is_err());
}
