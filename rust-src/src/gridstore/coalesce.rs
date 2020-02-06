use std::borrow::Borrow;
use std::cmp::Reverse;
use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::fmt::Debug;

use failure::Error;
use itertools::Itertools;
use ordered_float::OrderedFloat;
use rayon::prelude::*;

use crate::gridstore::common::*;
use crate::gridstore::store::GridStore;

/// Takes a vector of phrasematch subqueries (stack) and match options, gets matching grids, sorts the grids,
/// and returns a result of a sorted vector of contexts (lists of grids with added metadata)
pub fn coalesce<T: Borrow<GridStore> + Clone + Debug>(
    stack: Vec<PhrasematchSubquery<T>>,
    match_opts: &MatchOpts,
) -> Result<Vec<CoalesceContext>, Error> {
    let bigger_max = 2 * MAX_CONTEXTS;
    let contexts = if stack.len() <= 1 {
        let subquery = &stack[0];
        let matching = subquery.store.borrow().streaming_get_matching(
            &subquery.match_key,
            match_opts,
            bigger_max,
        )?;
        coalesce_single(subquery, matching, match_opts)?
    } else {
        let stack_with_data: Result<
            Vec<(PhrasematchSubquery<T>, Vec<MatchEntry>, MatchOpts)>,
            Error,
        > = stack
            .into_iter()
            .map(|subquery| {
                let adjusted_match_opts = match_opts.adjust_to_zoom(subquery.zoom);
                let matching = subquery.store.borrow().streaming_get_matching(
                    &subquery.match_key,
                    match_opts,
                    bigger_max,
                )?;

                // limit to 100,000 records -- we may want to experiment with this number; it was 500k in
                // carmen-cache, but hopefully we're sorting more intelligently on the way in here so
                // shouldn't need as many records. Still, we should limit it somehow.
                let grids: Vec<MatchEntry> = matching.take(100_000).collect();
                Ok((subquery, grids, adjusted_match_opts))
            })
            .collect();

        coalesce_multi(stack_with_data?)?
    };

    let mut out = Vec::with_capacity(MAX_CONTEXTS);
    if !contexts.is_empty() {
        let max_relevance = contexts[0].relev;
        let mut sets: HashSet<u64> = HashSet::new();
        for context in contexts {
            if out.len() >= MAX_CONTEXTS {
                break;
            }
            // 0.25 is the smallest allowed relevance
            if max_relevance - context.relev >= 0.25 {
                break;
            }
            let inserted = sets.insert(context.entries[0].tmp_id.into());
            if inserted {
                out.push(context);
            }
        }
    }
    Ok(out)
}

pub fn parallel_coalesce<T: Borrow<GridStore> + Clone + Debug + Send + Sync>(
    stacks: &[(Vec<PhrasematchSubquery<T>>, MatchOpts)],
) -> Result<Vec<Vec<CoalesceContext>>, Error> {
    let bigger_max = 2 * MAX_CONTEXTS;
    // let v: Vec<PhrasematchSubquery<T>> = Vec::new();
    // let () = (&v).into_par_iter();

    let mut tagged_subqueries: HashMap<(&PhrasematchSubquery<T>, &MatchOpts), (bool, bool)> =
        HashMap::new();
    for (stack, match_opts) in stacks.iter() {
        if stack.len() == 1 {
            tagged_subqueries
                .entry((&stack[0], match_opts))
                .and_modify(|e| e.0 = true)
                .or_insert((true, false));
        } else {
            for subquery in stack.iter() {
                // always insert, setting true regardless
                tagged_subqueries
                    .entry((subquery, match_opts))
                    .and_modify(|e| e.1 = true)
                    .or_insert((false, true));
            }
        }
    }

    let subqueries: Vec<
        Result<
            (
                (&PhrasematchSubquery<T>, &MatchOpts),
                (Option<Vec<CoalesceContext>>, Option<Vec<MatchEntry>>, MatchOpts),
            ),
            Error,
        >,
    > = tagged_subqueries
        .into_par_iter()
        .map(|((subquery, match_opts), (needs_single, needs_multi))| {
            let output = if needs_multi {
                let adjusted_match_opts = match_opts.adjust_to_zoom(subquery.zoom);
                let matching = subquery.store.borrow().streaming_get_matching(
                    &subquery.match_key,
                    &adjusted_match_opts,
                    bigger_max,
                )?;

                // limit to 100,000 records -- we may want to experiment with this number; it was 500k in
                // carmen-cache, but hopefully we're sorting more intelligently on the way in here so
                // shouldn't need as many records. Still, we should limit it somehow.
                let multi_grids: Vec<MatchEntry> = matching.take(100_000).collect();

                let single_results = if needs_single {
                    Some(coalesce_single(
                        &subquery,
                        multi_grids.iter().cloned(),
                        &adjusted_match_opts,
                    )?)
                } else {
                    None
                };
                (single_results, Some(multi_grids), adjusted_match_opts)
            } else {
                let adjusted_match_opts = match_opts.adjust_to_zoom(subquery.zoom);
                let matching = subquery.store.borrow().streaming_get_matching(
                    &subquery.match_key,
                    &adjusted_match_opts,
                    bigger_max,
                )?;
                let single_results =
                    Some(coalesce_single(subquery, matching, &adjusted_match_opts)?);

                (single_results, None, adjusted_match_opts)
            };

            Ok(((subquery, match_opts), output))
        })
        .collect();
    let subqueries: Result<
        HashMap<
            (&PhrasematchSubquery<T>, &MatchOpts),
            (Option<Vec<CoalesceContext>>, Option<Vec<MatchEntry>>, MatchOpts),
        >,
        Error,
    > = subqueries.into_iter().collect();
    let subqueries = subqueries?;

    let all_contexts: Result<Vec<Vec<CoalesceContext>>, Error> = (&stacks)
        .into_par_iter()
        .map(|(stack, match_opts)| {
            let contexts = if stack.len() == 1 {
                let subquery = &stack[0];
                let (single_results, _, _) = subqueries.get(&(subquery, &match_opts)).unwrap();

                single_results.as_ref().unwrap().clone()
            } else {
                let stack_with_data: Vec<(&PhrasematchSubquery<T>, &Vec<MatchEntry>, MatchOpts)> =
                    stack
                        .iter()
                        .map(|subquery| {
                            let (_, multi_grids, opts) =
                                subqueries.get(&(subquery, &match_opts)).unwrap();

                            let adjusted_opts = opts.clone();
                            (subquery, multi_grids.as_ref().unwrap(), adjusted_opts)
                        })
                        .collect();

                coalesce_multi(stack_with_data)?
            };

            let mut out = Vec::with_capacity(MAX_CONTEXTS);
            if !contexts.is_empty() {
                let relev_max = contexts[0].relev;
                let mut sets: HashSet<u64> = HashSet::new();
                for context in contexts {
                    if out.len() >= MAX_CONTEXTS {
                        break;
                    }
                    // 0.25 is the smallest allowed relevance
                    if relev_max - context.relev >= 0.25 {
                        break;
                    }
                    let inserted = sets.insert(context.entries[0].tmp_id.into());
                    if inserted {
                        out.push(context);
                    }
                }
            }

            Ok(out)
        })
        .collect();

    all_contexts
}

fn grid_to_coalesce_entry<T: Borrow<GridStore> + Clone>(
    grid: &MatchEntry,
    subquery: &PhrasematchSubquery<T>,
    match_opts: &MatchOpts,
) -> CoalesceEntry {
    // Zoom has been adjusted in coalesce_multi, or correct zoom has been passed in for coalesce_single
    debug_assert!(match_opts.zoom == subquery.zoom);
    let relevance = grid.grid_entry.relev * subquery.weight;

    CoalesceEntry {
        grid_entry: GridEntry { relev: relevance, ..grid.grid_entry },
        matches_language: grid.matches_language,
        idx: subquery.idx,
        tmp_id: ((subquery.idx as u32) << 25) + grid.grid_entry.id,
        mask: subquery.mask,
        distance: grid.distance,
        scoredist: grid.scoredist,
    }
}

fn coalesce_single<T: Borrow<GridStore> + Clone, U: Iterator<Item = MatchEntry>>(
    subquery: &PhrasematchSubquery<T>,
    grids: U,
    match_opts: &MatchOpts,
) -> Result<Vec<CoalesceContext>, Error> {
    let bigger_max = 2 * MAX_CONTEXTS;
    let mut max_relevance: f64 = 0.;
    let mut previous_id: u32 = 0;
    let mut previous_relevance: f64 = 0.;
    let mut previous_scoredist: f64 = 0.;
    let mut min_scoredist = std::f64::MAX;
    let mut feature_count: usize = 0;

    let mut coalesced: HashMap<u32, CoalesceEntry> = HashMap::new();

    for grid in grids {
        let coalesce_entry = grid_to_coalesce_entry(&grid, subquery, match_opts);

        // If it's the same feature as the last one, but a lower scoredist don't add it
        if previous_id == coalesce_entry.grid_entry.id
            && coalesce_entry.scoredist <= previous_scoredist
        {
            continue;
        }

        if feature_count > bigger_max {
            if coalesce_entry.scoredist < min_scoredist {
                continue;
            } else if coalesce_entry.grid_entry.relev < previous_relevance {
                // Grids should be sorted by relevance coming out of get_matching,
                // so if it's lower than the last relevance, stop
                break;
            }
        }

        if max_relevance - coalesce_entry.grid_entry.relev >= 0.25 {
            break;
        }
        if coalesce_entry.grid_entry.relev > max_relevance {
            max_relevance = coalesce_entry.grid_entry.relev;
        }

        // Save current values before mocing into coalesced
        let current_id = coalesce_entry.grid_entry.id;
        let current_relev = coalesce_entry.grid_entry.relev;
        let current_scoredist = coalesce_entry.scoredist;

        // If it's the same feature as one that's been added before, but a higher scoredist, update the entry
        match coalesced.entry(current_id) {
            Entry::Occupied(mut already_coalesced) => {
                if current_scoredist > already_coalesced.get().scoredist
                    && current_relev >= already_coalesced.get().grid_entry.relev
                {
                    already_coalesced.insert(coalesce_entry);
                }
            }
            Entry::Vacant(entry) => {
                entry.insert(coalesce_entry);
            }
        }

        if previous_id != current_id {
            feature_count += 1;
        }
        if match_opts.proximity.is_none() && feature_count > bigger_max {
            break;
        }
        if current_scoredist < min_scoredist {
            min_scoredist = current_scoredist;
        }
        previous_id = current_id;
        previous_relevance = current_relev;
        previous_scoredist = current_scoredist;
    }

    let mut contexts: Vec<CoalesceContext> = coalesced
        .iter()
        .map(|(_, entry)| CoalesceContext {
            entries: vec![entry.clone()],
            mask: entry.mask,
            relev: entry.grid_entry.relev,
        })
        .collect();

    contexts.sort_by_key(|context| {
        Reverse((
            OrderedFloat(context.relev),
            OrderedFloat(context.entries[0].scoredist),
            context.entries[0].grid_entry.x,
            context.entries[0].grid_entry.y,
            context.entries[0].grid_entry.id,
        ))
    });

    contexts.truncate(MAX_CONTEXTS);
    Ok(contexts)
}

fn coalesce_multi<
    T: Borrow<GridStore> + Clone,
    U: AsRef<[MatchEntry]>,
    V: Borrow<PhrasematchSubquery<T>>,
>(
    mut stack: Vec<(V, U, MatchOpts)>,
) -> Result<Vec<CoalesceContext>, Error> {
    stack.sort_by_key(|(subquery, _, _)| {
        let subquery = subquery.borrow();
        (subquery.zoom, subquery.idx)
    });

    let mut coalesced: HashMap<(u16, u16, u16), Vec<CoalesceContext>> = HashMap::new();
    let mut contexts: Vec<CoalesceContext> = Vec::new();

    let mut max_relevance: f64 = 0.;

    for (i, (subquery, grids, adjusted_match_opts)) in stack.iter().enumerate() {
        let mut to_add_to_coalesced: HashMap<(u16, u16, u16), Vec<CoalesceContext>> =
            HashMap::new();
        let subquery = subquery.borrow();
        let compatible_zooms: Vec<u16> = stack
            .iter()
            .filter_map(|(subquery_b, _, _)| {
                let subquery_b = subquery_b.borrow();
                if subquery.idx == subquery_b.idx || subquery.zoom < subquery_b.zoom {
                    None
                } else {
                    Some(subquery_b.zoom)
                }
            })
            .dedup()
            .collect();

        for grid in grids.as_ref().iter() {
            let coalesce_entry = grid_to_coalesce_entry(&grid, subquery, &adjusted_match_opts);

            let zxy = (subquery.zoom, grid.grid_entry.x, grid.grid_entry.y);

            let mut context_mask = coalesce_entry.mask;
            let mut context_relevance = coalesce_entry.grid_entry.relev;
            let mut entries: Vec<CoalesceEntry> = vec![coalesce_entry];

            // See which other zooms are compatible.
            // These should all be lower zooms, so "zoom out" by dividing by 2^(difference in zooms)
            for other_zoom in compatible_zooms.iter() {
                let scale_factor: u16 = 1 << (subquery.zoom - *other_zoom);
                let other_zxy = (
                    *other_zoom,
                    entries[0].grid_entry.x / scale_factor,
                    entries[0].grid_entry.y / scale_factor,
                );

                if let Some(already_coalesced) = coalesced.get(&other_zxy) {
                    let mut prev_mask = 0;
                    let mut prev_relev: f64 = 0.;
                    for parent_context in already_coalesced {
                        for parent_entry in &parent_context.entries {
                            // this cover is functionally identical with previous and
                            // is more relevant, replace the previous.
                            if parent_entry.mask == prev_mask
                                && parent_entry.grid_entry.relev > prev_relev
                            {
                                entries.pop();
                                entries.push(parent_entry.clone());
                                // Update the context-level aggregate relev
                                context_relevance -= prev_relev;
                                context_relevance += parent_entry.grid_entry.relev;

                                prev_mask = parent_entry.mask;
                                prev_relev = parent_entry.grid_entry.relev;
                            } else if (context_mask & parent_entry.mask) == 0 {
                                entries.push(parent_entry.clone());

                                context_relevance += parent_entry.grid_entry.relev;
                                context_mask = context_mask | parent_entry.mask;

                                prev_mask = parent_entry.mask;
                                prev_relev = parent_entry.grid_entry.relev;
                            }
                        }
                    }
                }
            }
            if context_relevance > max_relevance {
                max_relevance = context_relevance;
            }

            if i == (stack.len() - 1) {
                if entries.len() == 1 {
                    // Slightly penalize contexts that have no stacking
                    context_relevance -= 0.01;
                } else if entries[0].mask > entries[1].mask {
                    // Slightly penalize contexts in ascending order
                    context_relevance -= 0.01
                }

                if max_relevance - context_relevance < 0.25 {
                    contexts.push(CoalesceContext {
                        entries,
                        mask: context_mask,
                        relev: context_relevance,
                    });
                }
            } else if i == 0 || entries.len() > 1 {
                if let Some(already_coalesced) = to_add_to_coalesced.get_mut(&zxy) {
                    already_coalesced.push(CoalesceContext {
                        entries,
                        mask: context_mask,
                        relev: context_relevance,
                    });
                } else {
                    to_add_to_coalesced.insert(
                        zxy,
                        vec![CoalesceContext {
                            entries,
                            mask: context_mask,
                            relev: context_relevance,
                        }],
                    );
                }
            }
        }
        for (to_add_zxy, to_add_context) in to_add_to_coalesced {
            if let Some(existing_vector) = coalesced.get_mut(&to_add_zxy) {
                existing_vector.extend(to_add_context);
            } else {
                coalesced.insert(to_add_zxy, to_add_context);
            }
        }
    }

    for (_, matched) in coalesced {
        for context in matched {
            if max_relevance - context.relev < 0.25 {
                contexts.push(context);
            }
        }
    }

    contexts.sort_by_key(|context| {
        (
            Reverse(OrderedFloat(context.relev)),
            Reverse(OrderedFloat(context.entries[0].scoredist)),
            context.entries[0].idx,
            Reverse(context.entries[0].grid_entry.x),
            Reverse(context.entries[0].grid_entry.y),
            Reverse(context.entries[0].grid_entry.id),
        )
    });

    Ok(contexts)
}
