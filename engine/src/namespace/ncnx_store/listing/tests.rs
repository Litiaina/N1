use super::*;
use std::os::unix::ffi::OsStringExt;
use std::os::unix::fs::symlink;

struct Fixture(PathBuf);

impl Fixture {
    fn new() -> Self {
        let root = std::env::temp_dir().join(format!("n2-listing-bounded-{}", Ulid::new()));
        fs::create_dir_all(&root).expect("create listing fixture");
        Self(root)
    }

    fn record(&self, key: &str) -> PathBuf {
        let path = self.0.join(format!("{key}{NCNX_SUFFIX}"));
        fs::create_dir_all(path.parent().expect("record parent")).expect("create record parent");
        fs::write(&path, b"not-a-decodable-NCNX-record").expect("write filename-only fixture");
        path
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn observed_page(
    root: &Path,
    prefix: &str,
    after: Option<&str>,
    include_after: bool,
    limit: usize,
) -> (Vec<NcnxListItem>, usize, usize) {
    let mut peak_candidates = 0;
    let mut peak_depth = 0;
    let page = list_names_observed(
        root,
        prefix,
        after,
        include_after,
        limit,
        |candidates, depth| {
            peak_candidates = peak_candidates.max(candidates);
            peak_depth = peak_depth.max(depth);
        },
    )
    .expect("filename-only listing");
    assert!(peak_candidates <= limit.clamp(1, 10_000));
    (page, peak_candidates, peak_depth)
}

#[test]
fn wide_single_folder_pages_all_4097_names_with_at_most_17_candidates() {
    let fixture = Fixture::new();
    let expected = (0..4097)
        .map(|index| format!("projects/file-{index:05}.bin"))
        .collect::<Vec<_>>();
    // A coprime permutation keeps physical creation order unrelated to the
    // required logical ordering, including the earliest names in later slots.
    for index in 0..4097 {
        fixture.record(&expected[(index * 997) % expected.len()]);
    }
    assert!(
        NcnxRecord::decode(&fs::read(fixture.0.join("projects/file-00000.bin.ncnx")).unwrap())
            .is_err()
    );

    let mut actual = Vec::new();
    let mut after = None;
    let mut requests = 0;
    loop {
        let (page, peak_candidates, peak_depth) =
            observed_page(&fixture.0, "projects/", after.as_deref(), false, 17);
        assert!(peak_candidates <= 17);
        assert!(peak_depth <= 2, "wide listing retained extra branch frames");
        requests += 1;
        assert!(requests <= expected.len().div_ceil(17) + 1);
        if page.is_empty() {
            break;
        }
        assert!(
            page.windows(2)
                .all(|pair| pair[0].logical_key < pair[1].logical_key)
        );
        assert!(page.iter().all(|item| item.trash.is_none()));
        after = page.last().map(|item| item.logical_key.clone());
        actual.extend(page.into_iter().map(|item| item.logical_key));
    }
    assert_eq!(actual, expected);
}

#[test]
fn bounded_selection_matches_reference_for_mixed_prefixes_unicode_and_cursors() {
    let fixture = Fixture::new();
    let mut expected = vec![
        "a",
        "a-previous",
        "a/child",
        "a/child/deep",
        "a/child/deep/deepest",
        "a0",
        "z/file",
        "z/深",
        "é",
        "é/child",
        "éclair",
        "α/first",
        "中-01",
        "中/a",
        "中/b",
    ];
    // Reverse creation also exercises replacement of the current largest
    // selected candidate rather than relying on directory insertion order.
    for key in expected.iter().rev() {
        fixture.record(key);
    }
    expected.sort_unstable();
    fs::create_dir_all(fixture.0.join("a/empty/deeper")).unwrap();
    fs::create_dir_all(fixture.0.join("empty")).unwrap();
    fixture.record(".n2-private/should-not-list");
    fixture.record("a/.n2-private");
    fs::write(fixture.0.join("ignored.tmp"), b"not a namespace record").unwrap();
    let outside = Fixture::new();
    outside.record("should-not-follow");
    symlink(&outside.0, fixture.0.join("linked-directory")).unwrap();
    symlink(
        outside.0.join("should-not-follow.ncnx"),
        fixture.0.join("linked-record.ncnx"),
    )
    .unwrap();

    for prefix in ["", "a", "a/", "a/ch", "a0", "é", "中", "z/", "missing/"] {
        for after in [
            None,
            Some(""),
            Some("a"),
            Some("a/"),
            Some("a/child"),
            Some("é"),
            Some("中/z"),
            Some("zzzzz"),
        ] {
            for include_after in [false, true] {
                for limit in [0, 1, 3, 17, 10_001] {
                    let reference = expected
                        .iter()
                        .copied()
                        .filter(|key| {
                            key.starts_with(prefix)
                                && after.is_none_or(|after| {
                                    *key > after || (include_after && *key == after)
                                })
                        })
                        .take(limit.clamp(1, 10_000))
                        .collect::<Vec<_>>();
                    let (page, _, _) =
                        observed_page(&fixture.0, prefix, after, include_after, limit);
                    let actual = page
                        .iter()
                        .map(|item| item.logical_key.as_str())
                        .collect::<Vec<_>>();
                    assert_eq!(
                        actual, reference,
                        "prefix={prefix:?} after={after:?} include_after={include_after} limit={limit}"
                    );
                }
            }
        }
    }
}

#[test]
fn deeply_nested_listing_retains_only_page_candidates_and_active_depth() {
    let fixture = Fixture::new();
    let mut expected = Vec::new();
    let mut parent = String::new();
    for depth in 0..48 {
        parent.push_str("branch/");
        let key = format!("{parent}file-{depth:03}");
        fixture.record(&key);
        expected.push(key);
        for index in 0..8 {
            fixture.record(&format!("{parent}z{index}/ignored-later"));
        }
    }
    let (page, peak_candidates, peak_depth) = observed_page(&fixture.0, "", None, false, 5);
    let mut all = expected;
    for depth in 1..=48 {
        for index in 0..8 {
            all.push(format!("{}z{index}/ignored-later", "branch/".repeat(depth)));
        }
    }
    all.sort_unstable();
    assert_eq!(
        page.into_iter()
            .map(|item| item.logical_key)
            .collect::<Vec<_>>(),
        all[..5]
    );
    assert_eq!(peak_candidates, 5);
    assert!(peak_depth <= 50);
    assert!(peak_depth >= 49);
}

#[test]
fn trash_generations_keep_full_cursor_order_and_filename_only_pagination() {
    let fixture = Fixture::new();
    let mut expected = Vec::new();
    for (index, nanos) in [3i64, 11, 29].into_iter().enumerate() {
        let mutation = Ulid::from_parts(index as u64 + 1, index as u128 + 5).to_string();
        let generation = format!("{nanos}.{mutation}");
        for key in (0..97)
            .rev()
            .map(|number| format!("docs/file-{number:03}.bin"))
        {
            fixture.record(&format!("{generation}/{key}"));
            expected.push(format!("{key}\0{nanos:020}\0{mutation}"));
        }
    }
    expected.sort_unstable();
    let mut actual = Vec::new();
    let mut after = None;
    loop {
        let page = list_generation_trash_names(&fixture.0, "docs/", after.as_deref(), 17)
            .expect("bounded trash listing");
        if page.is_empty() {
            break;
        }
        assert!(actual.len() < expected.len());
        for item in page {
            let locator = item.trash.expect("trash generation locator");
            let cursor = format!(
                "{}\0{:020}\0{}",
                locator.logical_key, locator.deleted_at_unix_nanos, locator.delete_mutation_id
            );
            assert!(after.as_ref().is_none_or(|after| &cursor > after));
            after = Some(cursor.clone());
            actual.push(cursor);
        }
    }
    assert_eq!(actual, expected);
}

#[test]
fn missing_branches_are_empty_and_non_utf8_components_fail_closed() {
    let fixture = Fixture::new();
    assert!(
        list_names(&fixture.0.join("missing"), "", None, false, 17)
            .unwrap()
            .is_empty()
    );
    let invalid = fixture
        .0
        .join(std::ffi::OsString::from_vec(b"invalid-\xff.ncnx".to_vec()));
    fs::write(&invalid, b"unread record").unwrap();
    assert!(matches!(
        list_names(&fixture.0, "", None, false, 17),
        Err(AppError::Conflict(_))
    ));
}

#[test]
fn resumed_parent_cookies_preserve_all_interleaved_files_and_directories() {
    let fixture = Fixture::new();
    let mut expected = Vec::new();
    for index in (0..257).rev() {
        for key in [
            format!("root-{index:04}"),
            format!("branch-{index:04}/first"),
            format!("branch-{index:04}/nested/last"),
        ] {
            fixture.record(&key);
            expected.push(key);
        }
    }
    expected.sort_unstable();
    let (page, peak_candidates, peak_depth) = observed_page(&fixture.0, "", None, false, 10_000);
    assert_eq!(peak_candidates, expected.len());
    assert_eq!(peak_depth, 3);
    assert_eq!(
        page.into_iter()
            .map(|item| item.logical_key)
            .collect::<Vec<_>>(),
        expected
    );
}

#[test]
fn suspended_directory_identity_change_rejects_the_old_cookie() {
    let fixture = Fixture::new();
    fixture.record("parent/keep");
    let path = fixture.0.join("parent");
    let mut frame = DirectoryFrame::new(path.clone(), "parent/".into());
    let mut directory = open_directory(&mut frame).unwrap().unwrap();
    while let Some(entry) = directory.read() {
        let entry = entry.unwrap();
        if entry.file_name().to_bytes() == b"keep.ncnx" {
            frame.next_cookie = entry.offset().try_into().unwrap();
            break;
        }
    }
    assert_ne!(frame.next_cookie, 0);
    drop(directory);
    // Retain the old inode while replacing its name, so the test cannot pass
    // accidentally through immediate filesystem inode-number reuse.
    fs::rename(&path, fixture.0.join("retained-parent")).unwrap();
    fs::create_dir(&path).unwrap();
    assert!(matches!(
        open_directory(&mut frame),
        Err(AppError::ServiceUnavailable(_))
    ));
    assert!(fixture.0.join("retained-parent/keep.ncnx").is_file());
}

#[test]
fn trash_root_errors_are_not_reported_as_successful_empty_pages() {
    let fixture = Fixture::new();
    let path = fixture.0.join("not-a-directory");
    fs::write(&path, b"not a trash directory").unwrap();
    assert!(list_generation_trash_names(&path, "", None, 17).is_err());
    assert!(
        list_generation_trash_names(&fixture.0.join("missing"), "", None, 17)
            .unwrap()
            .is_empty()
    );
}

fn concurrent_deep_listing_with_low_fd_limit() {
    let fixture = Fixture::new();
    let mut expected = Vec::new();
    let mut parent = String::new();
    for depth in 0..128 {
        parent.push_str("d/");
        for name in ["first", "last"] {
            let key = format!("{parent}{name}-{depth:03}");
            fixture.record(&key);
            expected.push(key);
        }
    }
    expected.sort_unstable();
    let expected = std::sync::Arc::new(expected);
    let mut previous = std::mem::MaybeUninit::<libc::rlimit>::uninit();
    assert_eq!(
        unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, previous.as_mut_ptr()) },
        0
    );
    let previous = unsafe { previous.assume_init() };
    assert!(previous.rlim_max >= 48);
    struct RestoreLimit(libc::rlimit);
    impl Drop for RestoreLimit {
        fn drop(&mut self) {
            assert_eq!(unsafe { libc::setrlimit(libc::RLIMIT_NOFILE, &self.0) }, 0);
        }
    }
    let _restore = RestoreLimit(previous);
    let limited = libc::rlimit {
        rlim_cur: 48,
        rlim_max: previous.rlim_max,
    };
    assert_eq!(unsafe { libc::setrlimit(libc::RLIMIT_NOFILE, &limited) }, 0);
    let barrier = std::sync::Arc::new(std::sync::Barrier::new(33));
    std::thread::scope(|scope| {
        for _ in 0..32 {
            let barrier = barrier.clone();
            let root = fixture.0.clone();
            let expected = expected.clone();
            scope.spawn(move || {
                barrier.wait();
                let page = list_names(&root, "", None, false, 1000)
                    .expect("deep concurrent listing must not exhaust descriptors");
                assert_eq!(
                    page.into_iter()
                        .map(|item| item.logical_key)
                        .collect::<Vec<_>>(),
                    *expected
                );
            });
        }
        barrier.wait();
    });
}

#[test]
fn deep_listing_uses_bounded_descriptors_under_32_concurrent_requests() {
    const CHILD: &str = "N2_LISTING_LOW_FD_CHILD";
    if let Some(completion) = std::env::var_os(CHILD) {
        concurrent_deep_listing_with_low_fd_limit();
        fs::write(completion, b"PASS").expect("record actual low-FD child execution");
        return;
    }
    let completion =
        std::env::temp_dir().join(format!("n2-listing-low-fd-completed-{}", Ulid::new()));
    let mut child = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "namespace::ncnx_store::listing::tests::deep_listing_uses_bounded_descriptors_under_32_concurrent_requests", "--nocapture"])
        .env(CHILD, &completion)
        .spawn()
        .expect("spawn isolated low-FD listing test");
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
    loop {
        if let Some(status) = child.try_wait().expect("poll isolated low-FD listing test") {
            assert!(status.success(), "isolated low-FD listing failed: {status}");
            break;
        }
        if std::time::Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            panic!("isolated low-FD listing exceeded its 60-second limit");
        }
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
    assert_eq!(
        fs::read(&completion).expect("child must execute the actual regression"),
        b"PASS"
    );
    fs::remove_file(completion).unwrap();
}
