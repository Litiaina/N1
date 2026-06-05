use serde::Serialize;

pub fn quorum_required(voters: usize) -> usize {
    (voters / 2).saturating_add(1).max(1)
}

#[derive(Clone, Debug, Serialize)]
pub struct QuorumOutcome<T> {
    pub required: usize,
    pub successes: usize,
    pub failures: usize,
    pub values: Vec<T>,
}

impl<T> QuorumOutcome<T> {
    pub fn reached(&self) -> bool {
        self.successes >= self.required
    }
}

#[cfg(test)]
mod tests {
    use super::quorum_required;

    #[test]
    fn quorum_is_majority() {
        assert_eq!(quorum_required(1), 1);
        assert_eq!(quorum_required(2), 2);
        assert_eq!(quorum_required(3), 2);
        assert_eq!(quorum_required(5), 3);
    }
}
