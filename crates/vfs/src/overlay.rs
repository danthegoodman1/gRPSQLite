//! The writes and truncations of one uncommitted transaction.

use std::collections::BTreeMap;

use bytes::Bytes;

/// Buffered changes to a file, applied over the committed file at commit.
///
/// The committed bytes at offsets at or past `cut` read as zeros; `extents`
/// then overwrite their ranges; `size`, when set, is the file's size.
#[derive(Debug, Default)]
pub struct Overlay {
    /// Non-overlapping, non-adjacent-merged byte ranges keyed by start offset.
    extents: BTreeMap<u64, Vec<u8>>,
    /// The smallest size the transaction truncated to.
    cut: Option<u64>,
    /// The file's size after the last truncation, grown by later writes.
    size: Option<u64>,
}

/// One write of a commit.
#[derive(Debug, PartialEq, Eq)]
pub struct Extent {
    pub offset: u64,
    pub data: Bytes,
}

impl Overlay {
    pub fn is_empty(&self) -> bool {
        self.extents.is_empty() && self.cut.is_none()
    }

    pub fn clear(&mut self) {
        *self = Overlay::default();
    }

    pub fn write(&mut self, offset: u64, data: &[u8]) {
        if data.is_empty() {
            return;
        }
        let end = offset + data.len() as u64;
        let overlapping: Vec<u64> = self
            .extents
            .range(..end)
            .rev()
            .take_while(|(start, d)| *start + d.len() as u64 > offset)
            .map(|(start, _)| *start)
            .collect();
        let mut start = offset;
        let mut stop = end;
        let mut old = Vec::with_capacity(overlapping.len());
        for s in overlapping {
            if let Some(d) = self.extents.remove(&s) {
                start = start.min(s);
                stop = stop.max(s + d.len() as u64);
                old.push((s, d));
            }
        }
        let mut merged = vec![0u8; (stop - start) as usize];
        for (s, d) in old {
            let at = (s - start) as usize;
            merged[at..at + d.len()].copy_from_slice(&d);
        }
        let at = (offset - start) as usize;
        merged[at..at + data.len()].copy_from_slice(data);
        self.extents.insert(start, merged);
        if let Some(size) = &mut self.size {
            *size = (*size).max(end);
        }
    }

    pub fn truncate(&mut self, size: u64) {
        let dropped: Vec<u64> = self.extents.range(size..).map(|(s, _)| *s).collect();
        for s in dropped {
            self.extents.remove(&s);
        }
        if let Some((s, d)) = self.extents.range_mut(..size).next_back() {
            let keep = (size - *s) as usize;
            if d.len() > keep {
                d.truncate(keep);
            }
        }
        self.cut = Some(self.cut.map_or(size, |c| c.min(size)));
        self.size = Some(size);
    }

    /// The end of the last extent, or 0.
    fn extents_end(&self) -> u64 {
        self.extents
            .iter()
            .next_back()
            .map_or(0, |(s, d)| s + d.len() as u64)
    }

    /// The file's size given the committed size.
    pub fn size(&self, committed: u64) -> u64 {
        self.size
            .unwrap_or_else(|| committed.max(self.extents_end()))
    }

    /// Whether the extents cover every byte of `[offset, offset + len)`.
    pub fn covers(&self, offset: u64, len: u64) -> bool {
        let end = offset + len;
        let mut pos = offset;
        for (s, d) in self.extents.range(..end) {
            let e = s + d.len() as u64;
            if e <= pos {
                continue;
            }
            if *s > pos {
                return false;
            }
            pos = e;
            if pos >= end {
                return true;
            }
        }
        pos >= end
    }

    /// Applies the overlay to `buf`, which holds `committed_len` committed
    /// bytes read at `offset` and zeros after them. Returns how many bytes of
    /// `buf` lie within the file.
    pub fn apply(&self, offset: u64, buf: &mut [u8], committed_len: usize) -> usize {
        let end = offset + buf.len() as u64;
        if let Some(cut) = self.cut {
            if cut < end {
                let from = cut.saturating_sub(offset) as usize;
                buf[from..].fill(0);
            }
        }
        for (s, d) in self.extents.range(..end) {
            let e = s + d.len() as u64;
            if e <= offset {
                continue;
            }
            let from = (*s).max(offset);
            let to = e.min(end);
            let src = (from - s) as usize;
            let dst = (from - offset) as usize;
            let n = (to - from) as usize;
            buf[dst..dst + n].copy_from_slice(&d[src..src + n]);
        }
        let valid_end = match self.size {
            Some(size) => size.min(end),
            None => {
                let committed_end = offset + committed_len as u64;
                committed_end.max(self.extents_end().min(end))
            }
        };
        valid_end.saturating_sub(offset) as usize
    }

    /// The commit's truncation point and writes, including zeros for any gap
    /// the transaction opened between the truncation point and the file's end.
    pub fn into_commit_parts(self) -> (Option<u64>, Vec<Extent>) {
        let mut writes = self.extents;
        if let (Some(cut), Some(size)) = (self.cut, self.size) {
            let mut pos = cut;
            let mut gaps = Vec::new();
            for (s, d) in writes.range(..size) {
                let e = s + d.len() as u64;
                if e <= pos {
                    continue;
                }
                if *s > pos {
                    gaps.push((pos, *s));
                }
                pos = pos.max(e);
            }
            if pos < size {
                gaps.push((pos, size));
            }
            for (from, to) in gaps {
                writes.insert(from, vec![0; (to - from) as usize]);
            }
        }
        let writes = writes
            .into_iter()
            .map(|(offset, data)| Extent {
                offset,
                data: Bytes::from(data),
            })
            .collect();
        (self.cut, writes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A reference model: the file as a plain byte vector.
    fn apply_model(file: &mut Vec<u8>, op: &Op) {
        match op {
            Op::Write(o, d) => {
                let end = *o as usize + d.len();
                if file.len() < end {
                    file.resize(end, 0);
                }
                file[*o as usize..end].copy_from_slice(d);
            }
            Op::Truncate(s) => file.resize(*s as usize, 0),
        }
    }

    enum Op {
        Write(u64, Vec<u8>),
        Truncate(u64),
    }

    /// Commits `ops` the way a server would and compares with the model.
    fn check(base: &[u8], ops: &[Op]) {
        let mut model = base.to_vec();
        let mut overlay = Overlay::default();
        for op in ops {
            apply_model(&mut model, op);
            match op {
                Op::Write(o, d) => overlay.write(*o, d),
                Op::Truncate(s) => overlay.truncate(*s),
            }
        }
        // Reads through the overlay see the model.
        let len = model.len() + 10;
        let mut buf = vec![0u8; len];
        let n = base.len().min(len);
        buf[..n].copy_from_slice(&base[..n]);
        let valid = overlay.apply(0, &mut buf, n);
        assert_eq!(valid, model.len());
        assert_eq!(&buf[..valid], &model[..]);
        assert_eq!(overlay.size(base.len() as u64), model.len() as u64);
        // Committing: truncate to the cut, then apply the writes.
        let (cut, writes) = overlay.into_commit_parts();
        let mut server = base.to_vec();
        if let Some(c) = cut {
            server.resize(c as usize, 0);
        }
        for w in writes {
            apply_model(&mut server, &Op::Write(w.offset, w.data.to_vec()));
        }
        assert_eq!(server, model);
    }

    #[test]
    fn writes_merge() {
        check(
            &[1; 10],
            &[
                Op::Write(5, vec![2; 10]),
                Op::Write(0, vec![3; 7]),
                Op::Write(20, vec![4; 2]),
            ],
        );
    }

    #[test]
    fn truncate_then_grow() {
        check(
            &[1; 100],
            &[
                Op::Write(10, vec![2; 10]),
                Op::Truncate(15),
                Op::Write(40, vec![3; 5]),
                Op::Truncate(30),
            ],
        );
        check(&[1; 100], &[Op::Truncate(5), Op::Write(50, vec![9; 5])]);
        check(&[1; 100], &[Op::Truncate(200)]);
    }

    #[test]
    fn covers_detects_gaps() {
        let mut o = Overlay::default();
        o.write(0, &[1; 10]);
        o.write(20, &[1; 10]);
        assert!(o.covers(0, 10));
        assert!(o.covers(22, 5));
        assert!(!o.covers(5, 10));
    }

    #[test]
    fn randomized_against_model() {
        let mut x: u64 = 0x9e37_79b9_7f4a_7c15;
        let mut next = |bound: u64| {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x % bound
        };
        for _ in 0..2000 {
            let base = vec![7u8; next(64) as usize];
            let ops: Vec<Op> = (0..next(8))
                .map(|i| {
                    if next(4) == 0 {
                        Op::Truncate(next(80))
                    } else {
                        Op::Write(next(64), vec![i as u8 + 1; next(20) as usize + 1])
                    }
                })
                .collect();
            check(&base, &ops);
        }
    }
}
