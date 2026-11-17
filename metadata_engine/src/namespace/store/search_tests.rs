use super::super::tests::{FRAGMENT, fixture, publish, reserve};
use super::*;

fn spec(
    q: &str,
    scope: &str,
    field: SearchField,
    matching: SearchMatch,
    limit: usize,
) -> SearchSpec {
    SearchSpec::new(q, scope, field, matching, limit).unwrap()
}

fn seed(namespace: &Namespace, paths: &[&str]) {
    for (i, path) in paths.iter().enumerate() {
        let n = i as u128 + 10;
        let handle = reserve(namespace, path, n);
        publish(namespace, &handle, n);
    }
}

#[test]
fn fields_scope_case_and_head_visibility_follow_named_metadata() {
    let (_root, namespace) = fixture();
    seed(
        &namespace,
        &[
            "docs/Report.TXT",
            "docs/nested/report.pdf",
            "docs2/report.txt",
            "report.txt",
        ],
    );
    let page = namespace
        .search(
            FRAGMENT,
            &spec(
                "REPORT",
                "/docs/",
                SearchField::Filename,
                SearchMatch::Prefix,
                10,
            ),
            None,
        )
        .unwrap();
    assert_eq!(page.files.len(), 2);
    assert_eq!(page.total_matches, Some(2));
    assert!(page.files.iter().all(|file| file.path.starts_with("docs/")));
    let page = namespace
        .search(
            FRAGMENT,
            &spec(".TXT", "", SearchField::Extension, SearchMatch::Exact, 10),
            None,
        )
        .unwrap();
    assert_eq!(page.files.len(), 3);
    let page = namespace
        .search(
            FRAGMENT,
            &spec(
                "DOCS/REPORT.TXT",
                "",
                SearchField::Path,
                SearchMatch::Exact,
                10,
            ),
            None,
        )
        .unwrap();
    assert_eq!(page.files[0].path, "docs/Report.TXT");
    let page = namespace
        .search(
            FRAGMENT,
            &spec(
                "report",
                "report.txt",
                SearchField::Any,
                SearchMatch::Contains,
                10,
            ),
            None,
        )
        .unwrap();
    assert_eq!(page.files.len(), 1);
    let trash = namespace
        .trash(FRAGMENT, "docs/Report.TXT", 1700000001)
        .unwrap();
    assert_eq!(
        namespace
            .search(
                FRAGMENT,
                &spec(
                    "report",
                    "docs",
                    SearchField::Any,
                    SearchMatch::Contains,
                    10
                ),
                None
            )
            .unwrap()
            .files
            .len(),
        1
    );
    namespace.recover(FRAGMENT, &trash).unwrap();
    namespace
        .rename(FRAGMENT, "docs/Report.TXT", "docs/New.txt")
        .unwrap();
    assert_eq!(
        namespace
            .search(
                FRAGMENT,
                &spec(
                    "report",
                    "docs",
                    SearchField::Any,
                    SearchMatch::Contains,
                    10
                ),
                None
            )
            .unwrap()
            .files
            .len(),
        1
    );
    assert_eq!(
        namespace
            .search(
                FRAGMENT,
                &spec(
                    "new",
                    "docs",
                    SearchField::Filename,
                    SearchMatch::Prefix,
                    10
                ),
                None
            )
            .unwrap()
            .files
            .len(),
        1
    );
}

#[test]
fn search_is_fragment_scoped_and_excludes_unpublished_or_trashed_heads() {
    let (_root, namespace) = fixture();
    seed(&namespace, &["visible.bin", "trashed.bin"]);
    reserve(&namespace, "incomplete.bin", 50);
    namespace
        .trash(FRAGMENT, "trashed.bin", 1700000001)
        .unwrap();
    let query = spec("bin", "", SearchField::Any, SearchMatch::Contains, 10);
    let page = namespace.search(FRAGMENT, &query, None).unwrap();
    assert_eq!(
        page.files
            .iter()
            .map(|f| f.path.as_str())
            .collect::<Vec<_>>(),
        ["visible.bin"]
    );
    assert!(
        namespace
            .search(PoolId([42; 16]), &query, None)
            .unwrap()
            .files
            .is_empty()
    );
}

#[test]
fn result_pages_use_last_examined_key_without_duplicates_or_gaps() {
    let (_root, namespace) = fixture();
    seed(
        &namespace,
        &["a-match", "b-other", "c-match", "d-other", "e-match"],
    );
    let query = spec("match", "", SearchField::Any, SearchMatch::Contains, 1);
    let mut after = None;
    let mut paths = Vec::new();
    for _ in 0..10 {
        let page = namespace
            .search(FRAGMENT, &query, after.as_deref())
            .unwrap();
        assert!(page.files.len() <= 1);
        paths.extend(page.files.into_iter().map(|f| f.path));
        after = page.after;
        if after.is_none() {
            break;
        }
    }
    assert!(after.is_none());
    assert_eq!(paths, ["a-match", "c-match", "e-match"]);
}

#[test]
fn sparse_search_stops_at_its_work_budget_and_resumes_after_an_empty_page() {
    let (_root, namespace) = fixture();
    for start in (0..SCAN_ROWS + 9).step_by(256) {
        let mut tx = namespace.engine.write().unwrap();
        for i in start..(start + 256).min(SCAN_ROWS + 9) {
            let path = format!("entry-{i:05}.bin");
            let entry = Entry {
                object_id: format!("id-{i}"),
                head: Some(PublishedVersion {
                    object_id: format!("id-{i}"),
                    version_id: format!("v-{i}"),
                    content_type: "application/octet-stream".into(),
                    total_size_bytes: 1,
                    created_at_unix: 1,
                }),
            };
            tx.put(
                Table::Entries,
                &RecordKey::new(FRAGMENT, &path).unwrap(),
                &serde_json::to_vec(&entry).unwrap(),
            )
            .unwrap();
        }
        tx.commit().unwrap();
    }
    let needle = format!("entry-{:05}", SCAN_ROWS + 8);
    let query = spec(&needle, "", SearchField::Any, SearchMatch::Contains, 10);
    let before = namespace
        .engine
        .snapshot()
        .scan(Table::Entries, FRAGMENT, "", None, 1)
        .unwrap();
    let page = namespace.search(FRAGMENT, &query, None).unwrap();
    assert!(page.files.is_empty());
    assert_eq!(page.scanned_entries, SCAN_ROWS);
    assert_eq!(page.total_matches, None);
    let next = namespace
        .search(FRAGMENT, &query, page.after.as_deref())
        .unwrap();
    assert_eq!(next.files.len(), 1);
    assert_eq!(next.scanned_entries, 9);
    assert!(next.after.is_none());
    assert_eq!(
        before,
        namespace
            .engine
            .snapshot()
            .scan(Table::Entries, FRAGMENT, "", None, 1)
            .unwrap()
    );
}

#[test]
fn unicode_names_and_page_mutations_are_read_from_current_heads() {
    let (_root, namespace) = fixture();
    seed(&namespace, &["a/CAFÉ.txt", "b/東京.bin", "c/café.txt"]);
    let query = spec("CAFÉ", "", SearchField::Filename, SearchMatch::Contains, 1);
    let first = namespace.search(FRAGMENT, &query, None).unwrap();
    namespace
        .rename(FRAGMENT, "c/café.txt", "d/café.txt")
        .unwrap();
    let next = namespace
        .search(FRAGMENT, &query, first.after.as_deref())
        .unwrap();
    assert_eq!(next.files[0].path, "d/café.txt");
    assert_eq!(
        namespace
            .search(
                FRAGMENT,
                &spec("東京", "", SearchField::Any, SearchMatch::Contains, 10),
                None
            )
            .unwrap()
            .files
            .len(),
        1
    );
}

#[test]
fn invalid_queries_and_out_of_scope_cursors_are_rejected() {
    let (_root, namespace) = fixture();
    for (q, scope, limit) in [
        ("", "", 10),
        (" ", "", 10),
        ("a\0b", "", 10),
        ("a", "../b", 10),
        ("a", "", 0),
        ("a", "", 1001),
    ] {
        assert!(SearchSpec::new(q, scope, SearchField::Any, SearchMatch::Contains, limit).is_err());
    }
    assert!(
        SearchSpec::new(
            &"x".repeat(1025),
            "",
            SearchField::Any,
            SearchMatch::Contains,
            10
        )
        .is_err()
    );
    assert!(
        namespace
            .search(
                FRAGMENT,
                &spec("a", "docs", SearchField::Any, SearchMatch::Contains, 1),
                Some("other/x")
            )
            .is_err()
    );
}
