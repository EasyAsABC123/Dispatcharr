//! Source season dictionary insertion order and separately sorted season list.
//! A null SQL season sorts last, but Python maps it into season1. Avoid an
//! O(seasons*episodes) filter or Vec<Value> tree. Original Dispatcharr credit.
use crate::{reader, vod_episode_rows::Episode};
use dispatcharr_store::StoreError;
use std::{mem::size_of, ops::Range};

pub(crate) struct Group {
    pub season: i32,
    pub first: Range<usize>,
    pub trailing_nulls: Option<Range<usize>>,
}
pub(crate) struct Groups {
    /// First occurrence order for the JSON episodes object.
    pub entries: Vec<Group>,
    /// Sorted Source seasons_list indices, without moving episode groups.
    pub sorted: Vec<usize>,
    pub retained_bytes: usize,
}
pub(crate) fn build(rows: &[Episode], available: usize) -> Result<Groups, StoreError> {
    let mut count = 0usize;
    let mut last: Option<Option<i32>> = None;
    let mut saw_one = false;
    for row in rows {
        if row.season == Some(1) {
            saw_one = true;
        }
        if last != Some(row.season) {
            count = count.checked_add(1).ok_or_else(reader::capacity)?;
        }
        if let Some(previous) = last {
            if (previous.is_none() && row.season.is_some())
                || matches!((previous,row.season),(Some(a),Some(b))if b<a)
            {
                return Err(StoreError::Unsupported(
                    "XC episode SQL season order invariant failed",
                ));
            }
        }
        last = Some(row.season);
    }
    if last == Some(None) && saw_one {
        count -= 1;
    }
    let quote = count
        .checked_mul(
            size_of::<Group>()
                .checked_mul(2)
                .and_then(|n| n.checked_add(2 * size_of::<usize>()))
                .ok_or_else(reader::capacity)?,
        )
        .and_then(|n| n.checked_add(4096))
        .ok_or_else(reader::capacity)?;
    if quote > available {
        return Err(reader::capacity());
    }
    let mut entries: Vec<Group> = Vec::new();
    entries
        .try_reserve_exact(count)
        .map_err(|_| reader::capacity())?;
    let mut previous = None;
    let mut nulls = false;
    let mut null_position = None;
    for (index, row) in rows.iter().enumerate() {
        let season = row.season.unwrap_or(1);
        if let Some(raw) = row.season {
            if nulls || previous.is_some_and(|last| raw < last) {
                return Err(StoreError::Unsupported(
                    "XC episode SQL season order invariant failed",
                ));
            }
            if previous == Some(raw) {
                entries.last_mut().expect("previous group").first.end = index + 1;
            } else {
                entries.push(Group {
                    season,
                    first: index..index + 1,
                    trailing_nulls: None,
                });
            }
            previous = Some(raw);
        } else if !nulls {
            // Before nulls, entries are numerically sorted by SQL. Source
            // appends these rows to an existing season1 if it was seen.
            match entries.binary_search_by_key(&1, |g| g.season) {
                Ok(position) => {
                    entries[position].trailing_nulls = Some(index..index + 1);
                    null_position = Some(position);
                }
                Err(_) => {
                    null_position = Some(entries.len());
                    entries.push(Group {
                        season: 1,
                        first: index..index + 1,
                        trailing_nulls: None,
                    });
                }
            }
            nulls = true;
        } else {
            // At most one split group exists, so find once among bounded
            // groups only at null start; repeated rows do not scan them.
            let position = null_position.expect("null group");
            if let Some(range) = entries[position].trailing_nulls.as_mut() {
                range.end = index + 1;
            } else {
                entries[position].first.end = index + 1;
            }
        }
    }
    let mut sorted = Vec::new();
    sorted
        .try_reserve_exact(entries.len())
        .map_err(|_| reader::capacity())?;
    sorted.extend(0..entries.len());
    sorted.sort_unstable_by_key(|&i| entries[i].season);
    let retained_bytes = entries
        .capacity()
        .checked_mul(size_of::<Group>())
        .and_then(|n| n.checked_add(sorted.capacity().checked_mul(size_of::<usize>())?))
        .ok_or_else(reader::capacity)?;
    if retained_bytes > quote {
        return Err(reader::capacity());
    }
    Ok(Groups {
        entries,
        sorted,
        retained_bytes,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    fn episode(season: Option<i32>) -> Episode {
        Episode {
            id: 1,
            name: String::new(),
            description: None,
            rating: None,
            imdb: None,
            air_date: None,
            duration: None,
            season,
            number: None,
            properties: None,
            created: chrono::DateTime::from_timestamp(0, 0).unwrap(),
        }
    }
    #[test]
    fn null_season_has_insertion_order_separate_from_sorted_list() {
        let rows = [episode(Some(0)), episode(Some(2)), episode(None)];
        let groups = build(&rows, 64 * 1024).unwrap();
        assert_eq!(
            groups.entries.iter().map(|g| g.season).collect::<Vec<_>>(),
            [0, 2, 1]
        );
        assert_eq!(
            groups
                .sorted
                .iter()
                .map(|&i| groups.entries[i].season)
                .collect::<Vec<_>>(),
            [0, 1, 2]
        );
        assert_eq!(groups.entries[2].first, 2..3);
    }
    #[test]
    fn null_seasons_append_to_existing_one_without_reordering_episodes() {
        let rows = [
            episode(Some(1)),
            episode(Some(2)),
            episode(None),
            episode(None),
        ];
        let groups = build(&rows, 64 * 1024).unwrap();
        assert_eq!(groups.entries.len(), 2);
        assert_eq!(groups.entries[0].first, 0..1);
        assert_eq!(groups.entries[0].trailing_nulls, Some(2..4));
        assert_eq!(groups.entries[1].first, 1..2);
    }
    #[test]
    fn invalid_sql_order_and_insufficient_existing_quote_are_rejected() {
        assert!(build(&[episode(None), episode(Some(1))], 64 * 1024).is_err());
        assert!(build(&[episode(Some(2)), episode(Some(1))], 64 * 1024).is_err());
        assert!(build(&[episode(Some(1))], 1).is_err());
    }
}
