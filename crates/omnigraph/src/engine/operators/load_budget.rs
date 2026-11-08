//! One csv load's memory shares. `ValidateExec` carves them once from the
//! pool's room after the key sets are admitted, one per charging thread, so
//! whether a charge is refused depends on that thread's own charges and
//! never on how far another thread got; the pool counts beside them.

use std::sync::atomic::{AtomicUsize, Ordering};

/// Who charges a share: a parse partition's producer (its source pieces,
/// decoding, decoded rows, their validation facts and the copy the sink
/// will hold of them), partition 0's sweeper sizing the unique holders, or
/// the fragment sink, whose charges are counted on the pool and never
/// refused: its open chunks are a copy of rows a partition already charged.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::engine) enum ShareClass {
    Partition(usize),
    Holders,
    Sink,
}

/// Of the room's sixty-fourths: the holders' (the partitions' when the plan has
/// none) and the partitions' over their count; the four left are uncharged
/// (lease metadata, registry growth, the sink's chunks), counted on the pool.
const SIXTY_FOURTHS: u64 = 64;
const HOLDERS: u64 = 8;
const PARTITIONS: u64 = 52;

#[derive(Debug)]
struct Share {
    bytes: usize,
    used: AtomicUsize,
}

impl Share {
    fn new(bytes: usize) -> Self {
        Self {
            bytes,
            used: AtomicUsize::new(0),
        }
    }

    /// `Err` is the demand: what the share would hold with `additional`.
    fn charge(&self, additional: usize) -> Result<(), usize> {
        self.used
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |used| {
                used.checked_add(additional)
                    .filter(|next| *next <= self.bytes)
            })
            .map(|_| ())
            .map_err(|used| used.saturating_add(additional))
    }

    fn release(&self, bytes: usize) {
        let _ = self
            .used
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |used| {
                Some(used.saturating_sub(bytes))
            });
    }
}

#[derive(Debug)]
pub(in crate::engine) struct LoadBudget {
    limit: u64,
    reserved: u64,
    partition_sixty_fourths: u64,
    partitions: Vec<Share>,
    holders: Share,
}

impl LoadBudget {
    /// The shares of one run over the room `limit` leaves past `reserved`,
    /// the pool's total at the carve, `partitions` ways; `holders` is whether
    /// the plan sizes unique holders, else their sixty-fourths go to the partitions.
    pub(in crate::engine) fn carve(
        limit: u64,
        reserved: u64,
        partitions: usize,
        holders: bool,
    ) -> Self {
        let partitions = partitions.max(1);
        let room = limit.saturating_sub(reserved);
        let (partition_sixty_fourths, holders_sixty_fourths) = if holders {
            (PARTITIONS, HOLDERS)
        } else {
            (PARTITIONS + HOLDERS, 0)
        };
        let share = |numerator: u64, denominator: u64| {
            Share::new(
                usize::try_from(room.saturating_mul(numerator) / denominator).unwrap_or(usize::MAX),
            )
        };
        let per_partition = SIXTY_FOURTHS.saturating_mul(partitions as u64);
        Self {
            limit,
            reserved,
            partition_sixty_fourths,
            partitions: (0..partitions)
                .map(|_| share(partition_sixty_fourths, per_partition))
                .collect(),
            holders: share(holders_sixty_fourths, SIXTY_FOURTHS),
        }
    }

    pub(in crate::engine) fn limit(&self) -> u64 {
        self.limit
    }

    /// `None` for the sink, which has no share to refuse.
    fn share(&self, class: ShareClass) -> Option<&Share> {
        match class {
            ShareClass::Partition(partition) => Some(&self.partitions[partition]),
            ShareClass::Holders => Some(&self.holders),
            ShareClass::Sink => None,
        }
    }

    /// The sixty-fourths `class` takes of the room, over the partitions it is split
    /// across; the sink's is the room itself.
    fn fraction(&self, class: ShareClass) -> (u64, u64) {
        match class {
            ShareClass::Partition(_) => (
                self.partition_sixty_fourths,
                SIXTY_FOURTHS.saturating_mul(self.partitions.len() as u64),
            ),
            ShareClass::Holders => (HOLDERS, SIXTY_FOURTHS),
            ShareClass::Sink => (SIXTY_FOURTHS, SIXTY_FOURTHS),
        }
    }

    /// The smallest limit whose `class` share holds `demand` bytes with the
    /// pool as it was at the carve: what a refusal reports as `actual`.
    pub(in crate::engine) fn admitting_limit(&self, class: ShareClass, demand: usize) -> u64 {
        let (numerator, denominator) = self.fraction(class);
        let room = u64::try_from(demand)
            .unwrap_or(u64::MAX)
            .saturating_mul(denominator)
            .div_ceil(numerator.max(1));
        self.reserved.saturating_add(room)
    }

    /// `Err` is the limit that would admit the charge.
    pub(in crate::engine) fn charge(&self, class: ShareClass, bytes: usize) -> Result<(), u64> {
        match self.share(class) {
            Some(share) => share
                .charge(bytes)
                .map_err(|demand| self.admitting_limit(class, demand)),
            None => Ok(()),
        }
    }

    pub(in crate::engine) fn release(&self, class: ShareClass, bytes: usize) {
        if let Some(share) = self.share(class) {
            share.release(bytes);
        }
    }

    /// Admission: every partition's share holds `floor`, the source bytes a
    /// partition needs before it can cut a slice; `Err` is the limit that
    /// would.
    pub(in crate::engine) fn admit(&self, floor: usize) -> Result<(), u64> {
        if self.partitions[0].bytes >= floor {
            Ok(())
        } else {
            Err(self.admitting_limit(ShareClass::Partition(0), floor))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_shares_leave_four_sixty_fourths_of_the_room_uncharged() {
        let budget = LoadBudget::carve(1_000, 100, 3, true);
        let room = 900;
        let shares: usize = budget
            .partitions
            .iter()
            .map(|share| share.bytes)
            .sum::<usize>()
            + budget.holders.bytes;
        assert!(shares <= room * 60 / 64, "{shares}");
        assert_eq!(budget.partitions[2].bytes, room * 52 / (64 * 3));
        assert_eq!(budget.holders.bytes, room * 8 / 64);
        assert!(budget.admit(budget.partitions[0].bytes).is_ok());
        assert!(budget.admit(budget.partitions[0].bytes + 1).is_err());

        let without_holders = LoadBudget::carve(1_000, 100, 3, false);
        assert_eq!(without_holders.partitions[0].bytes, room * 60 / (64 * 3));
        assert_eq!(without_holders.holders.bytes, 0);
    }

    #[test]
    fn the_sink_is_never_refused_and_its_release_touches_no_share() {
        let budget = LoadBudget::carve(1_000, 100, 2, true);
        assert!(budget.charge(ShareClass::Sink, usize::MAX).is_ok());
        assert!(budget.charge(ShareClass::Sink, usize::MAX).is_ok());
        budget.release(ShareClass::Sink, usize::MAX);
        let partition = budget.partitions[1].bytes;
        assert!(budget.charge(ShareClass::Partition(1), partition).is_ok());
        assert!(budget.charge(ShareClass::Partition(1), 1).is_err());
        assert!(
            budget
                .charge(ShareClass::Holders, budget.holders.bytes)
                .is_ok()
        );
        assert!(budget.charge(ShareClass::Holders, 1).is_err());
        assert_eq!(budget.admitting_limit(ShareClass::Sink, 50), 150);
    }

    #[test]
    fn the_admitting_limit_is_the_limit_that_admits_and_one_less_refuses() {
        for holders in [true, false] {
            let budget = LoadBudget::carve(1_000, 100, 2, holders);
            let classes: &[ShareClass] = if holders {
                &[ShareClass::Partition(1), ShareClass::Holders]
            } else {
                &[ShareClass::Partition(1)]
            };
            for &class in classes {
                let demand = budget.share(class).unwrap().bytes + 1;
                let actual = budget.charge(class, demand).unwrap_err();
                assert_eq!(actual, budget.admitting_limit(class, demand));
                assert!(
                    LoadBudget::carve(actual, 100, 2, holders)
                        .charge(class, demand)
                        .is_ok()
                );
                assert!(
                    LoadBudget::carve(actual - 1, 100, 2, holders)
                        .charge(class, demand)
                        .is_err()
                );
            }
            let floor = budget.partitions[0].bytes + 1;
            assert_eq!(
                budget.admit(floor).unwrap_err(),
                budget.admitting_limit(ShareClass::Partition(0), floor)
            );
        }
    }

    #[test]
    fn concurrent_charges_never_exceed_a_share_and_a_release_reopens_it() {
        let budget = LoadBudget::carve(64 * 1_000 + 100, 100, 1, true);
        let share = budget.partitions[0].bytes;
        let accepted = std::thread::scope(|scope| {
            let handles: Vec<_> = (0..8)
                .map(|_| {
                    scope.spawn(|| {
                        (0..share)
                            .filter(|_| budget.charge(ShareClass::Partition(0), 1).is_ok())
                            .count()
                    })
                })
                .collect();
            handles
                .into_iter()
                .map(|handle| handle.join().unwrap())
                .sum::<usize>()
        });
        assert_eq!(accepted, share);
        assert!(budget.charge(ShareClass::Partition(0), 1).is_err());
        assert!(budget.charge(ShareClass::Holders, 1).is_ok());
        budget.release(ShareClass::Partition(0), 1);
        assert!(budget.charge(ShareClass::Partition(0), 1).is_ok());
    }
}
