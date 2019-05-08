use std::cmp::Reverse;
use std::collections::HashSet;
use std::error::Error;

use ordered_float::OrderedFloat;

use crate::gridstore::common::*;
use crate::gridstore::store::GridStore;

#[derive(Debug, Clone)]
pub struct PhrasematchSubquery<'a> {
    pub store: &'a GridStore,
    pub weight: f64,
    pub match_key: MatchKey,
    pub idx: u16,
    pub zoom: u16,
    pub mask: u32,
}

pub fn coalesce(
    stack: &[PhrasematchSubquery],
    match_opts: &MatchOpts,
) -> Result<Vec<CoalesceContext>, Box<Error>> {
    let contexts = if stack.len() > 1 {
        coalesce_single(&stack[0], match_opts)?
    } else {
        //coalesce_multi(stack, match_opts)?
        Vec::new()
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
}

fn coalesce_single(
    subquery: &PhrasematchSubquery,
    match_opts: &MatchOpts,
) -> Result<Vec<CoalesceContext>, Box<Error>> {
    let grids = subquery.store.get_matching(&subquery.match_key, match_opts)?;
    let mut contexts: Vec<CoalesceContext> = Vec::new();
    let mut max_relev: f32 = 0.;
    let mut last_id: u32 = 0;
    let mut last_relev: f32 = 0.;
    let mut last_scoredist: f64 = 0.;
    let mut last_distance: f64 = 0.;
    let mut min_scoredist = std::f64::MAX;
    let mut feature_count: usize = 0;

    for grid in grids {
        // Calculate distance, scoredist, and language-adjusted relevance
        let (distance, scoredist, relev) = match match_opts.proximity {
            Some(Proximity { point: [proximity_x, proximity_y], radius }) => {
                // TODO: skip calculations of distance and scoredist if all of the inputs are the same
                let distance =
                    tile_dist(proximity_x, proximity_y, grid.grid_entry.x, grid.grid_entry.y);
                let scoredist = scoredist(match_opts.zoom, distance, grid.grid_entry.score, radius);
                // TODO: don't do language penalty if feature is inside proximity/scaled radius
                let relev = if grid.matches_language {
                    grid.grid_entry.relev
                } else {
                    grid.grid_entry.relev * 0.96
                };
                (distance, scoredist, relev)
            }
            None => {
                let relev = if grid.matches_language {
                    grid.grid_entry.relev
                } else {
                    grid.grid_entry.relev * 0.96
                };
                (0., grid.grid_entry.score as f64, relev)
            }
        };

        let coalesce_entry = CoalesceEntry {
            grid_entry: GridEntry { relev, ..grid.grid_entry },
            matches_language: grid.matches_language,
            idx: subquery.idx,
            tmp_id: ((subquery.idx as u32) << 25) + grid.grid_entry.id,
            mask: subquery.mask,
            distance: distance,
            scoredist: scoredist,
        };

        // If it's the same feature as the last one, but a lower scoredist don't add it
        if last_id == coalesce_entry.grid_entry.id && coalesce_entry.scoredist <= last_scoredist {
            continue;
        }

        if feature_count > MAX_CONTEXTS {
            if coalesce_entry.scoredist < min_scoredist {
                continue;
            } else if coalesce_entry.grid_entry.relev < last_relev {
                // Grids should be sorted by relevance coming out of get_matching,
                // so if it's lower than the last relevance, stop
                break;
            }
        }

        if max_relev - coalesce_entry.grid_entry.relev >= 0.25 {
            break;
        }
        if coalesce_entry.grid_entry.relev > max_relev {
            max_relev = coalesce_entry.grid_entry.relev;
        }
        // For coalesce single, there is only one coalesce entry per context
        contexts.push(CoalesceContext {
            mask: coalesce_entry.mask,
            relev: coalesce_entry.grid_entry.relev,
            entries: vec![coalesce_entry.clone()],
        });

        if last_id != coalesce_entry.grid_entry.id {
            feature_count += 1;
        }
        if match_opts.proximity.is_none() && feature_count > MAX_CONTEXTS {
            break;
        }
        if coalesce_entry.scoredist < min_scoredist {
            min_scoredist = coalesce_entry.scoredist;
        }
        last_id = coalesce_entry.grid_entry.id;
        last_relev = coalesce_entry.grid_entry.relev;
        last_scoredist = coalesce_entry.scoredist;
        last_distance = coalesce_entry.distance;
    }

    contexts.sort_by_key(|context| {
        (
            Reverse(OrderedFloat(context.relev)),
            Reverse(OrderedFloat(context.entries[0].scoredist)),
            context.entries[0].grid_entry.id,
            context.entries[0].grid_entry.x,
            context.entries[0].grid_entry.y,
        )
    });

    contexts.dedup_by_key(|context| context.entries[0].grid_entry.id);
    contexts.truncate(MAX_CONTEXTS);
    Ok(contexts)
}

fn tile_dist(proximity_x: u16, proximity_y: u16, grid_x: u16, grid_y: u16) -> f64 {
    let dx = (proximity_x as f64) - (grid_x as f64);
    let dy = (proximity_y as f64) - (grid_y as f64);
    ((dx * dx) + (dy * dy)).sqrt()
}

fn scoredist(zoom: u16, distance: f64, score: u8, radius: f64) -> f64 {
    // TODO: implement
    score as f64
}
