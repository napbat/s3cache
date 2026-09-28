use super::*;

#[test]
fn get_and_head_observations_keep_origin_last_modified() {
    let modified = Timestamp::from(SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000));
    let get = GetObjectOutput {
        last_modified: Some(modified.clone()),
        ..Default::default()
    };
    let head = HeadObjectOutput {
        last_modified: Some(modified.clone()),
        ..Default::default()
    };

    assert_eq!(observed!(&get).last_modified, Some(modified.clone()));
    assert_eq!(observed!(&head).last_modified, Some(modified));
}
