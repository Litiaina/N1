//! Read-only namespace search. Each page owns one bounded metadata snapshot;
//! it never reads object payloads or builds a retained namespace projection.
use super::*;

const SCAN_ROWS: usize = 4096;
const BATCH_ROWS: usize = 256;

#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum SearchField {
    #[default]
    Any,
    Filename,
    Path,
    Extension,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum SearchMatch {
    Exact,
    Prefix,
    #[default]
    Contains,
}

#[derive(Clone, Debug, Serialize)]
pub struct SearchSpec {
    needle: String,
    scope: String,
    field: SearchField,
    matching: SearchMatch,
    limit: usize,
}

#[derive(Debug)]
pub struct SearchPage {
    pub files: Vec<NamespaceFile>,
    pub after: Option<String>,
    pub scanned_entries: usize,
    /// Known only when this request examined the complete query range.
    pub total_matches: Option<usize>,
}

impl SearchSpec {
    pub(crate) fn is_scoped(&self) -> bool {
        !self.scope.is_empty()
    }

    pub fn new(
        query: &str,
        scope: &str,
        field: SearchField,
        matching: SearchMatch,
        limit: usize,
    ) -> Result<Self> {
        let query = query.trim();
        if query.is_empty() || query.len() > 1024 || query.contains('\0') {
            return Err(Error::Invalid(
                "q must contain 1–1024 bytes without NUL".into(),
            ));
        }
        if scope.len() > 4096 || scope.contains('\0') || !(1..=1000).contains(&limit) {
            return Err(Error::Invalid(
                "invalid search scope or limit (1–1000)".into(),
            ));
        }
        let scope = crate::namespace::paths::normalize_path(scope)
            .map_err(|e| Error::Invalid(e.to_string()))?;
        let needle = query.to_lowercase();
        if matches!(field, SearchField::Extension) && needle.trim_start_matches('.').is_empty() {
            return Err(Error::Invalid("extension query is empty".into()));
        }
        Ok(Self {
            needle,
            scope,
            field,
            matching,
            limit,
        })
    }

    fn matches(&self, path: &str) -> bool {
        if !self.scope.is_empty()
            && path != self.scope
            && !path
                .strip_prefix(&self.scope)
                .is_some_and(|tail| tail.starts_with('/'))
        {
            return false;
        }
        let path = path.to_lowercase();
        let filename = path.rsplit('/').next().unwrap_or("");
        let extension = filename.rsplit_once('.').map_or("", |(_, ext)| ext);
        let matches = |value: &str, needle: &str| match self.matching {
            SearchMatch::Exact => value == needle,
            SearchMatch::Prefix => value.starts_with(needle),
            SearchMatch::Contains => value.contains(needle),
        };
        match self.field {
            SearchField::Any => matches(&path, &self.needle) || matches(filename, &self.needle),
            SearchField::Path => matches(&path, &self.needle),
            SearchField::Filename => matches(filename, &self.needle),
            SearchField::Extension => matches(extension, self.needle.trim_start_matches('.')),
        }
    }
}

impl Namespace {
    pub fn search(
        &self,
        fragment: PoolId,
        spec: &SearchSpec,
        after: Option<&str>,
    ) -> Result<SearchPage> {
        let prefix = spec.scope.clone();
        if after.is_some_and(|key| !key.starts_with(&prefix)) {
            return Err(Error::Invalid("search cursor is outside its scope".into()));
        }
        let view = self.engine.snapshot();
        let mut cursor = after.map(|key| RecordKey::new(fragment, key)).transpose()?;
        let mut files = Vec::with_capacity(spec.limit);
        let mut scanned_entries = 0;
        while scanned_entries < SCAN_ROWS && files.len() < spec.limit {
            let take = BATCH_ROWS.min(SCAN_ROWS - scanned_entries);
            let rows = view.scan(Table::Entries, fragment, &prefix, cursor.as_ref(), take)?;
            if rows.is_empty() {
                break;
            }
            for (key, value) in rows {
                scanned_entries += 1;
                let entry: Entry = decode(value)?;
                if let Some(version) = entry.head
                    && spec.matches(key.identifier())
                {
                    files.push(NamespaceFile {
                        path: key.identifier().into(),
                        version,
                    });
                }
                cursor = Some(key);
                if files.len() == spec.limit {
                    break;
                }
            }
        }
        // One bounded lookahead distinguishes exhaustion from a work/result cap.
        // scan() may also stop at its byte cap, so a short batch is not EOF.
        let more = !view
            .scan(Table::Entries, fragment, &prefix, cursor.as_ref(), 1)?
            .is_empty();
        let next = if more {
            cursor.map(|key| key.identifier().to_owned())
        } else {
            None
        };
        let total_matches = (after.is_none() && !more).then_some(files.len());
        Ok(SearchPage {
            files,
            after: next,
            scanned_entries,
            total_matches,
        })
    }
}

#[cfg(test)]
#[path = "search_tests.rs"]
mod tests;
