use std::borrow::Borrow;
use std::cmp::Reverse;
use std::collections::{HashMap, HashSet};

use failure::Error;
use itertools::Itertools;
use ordered_float::OrderedFloat;

use crate::gridstore::common::*;
use crate::gridstore::store::GridStore;

/// Takes a vector of phrasematch subqueries (stack) and match options, gets matching grids, sorts the grids,
/// and returns a result of a sorted vector of contexts (lists of grids with added metadata)
pub fn coalesce<T: Borrow<GridStore> + Clone>(
    stack: Vec<PhrasematchSubquery<T>>,
    match_opts: &MatchOpts,
) -> Result<Vec<CoalesceContext>, Error> {
    let contexts = if stack.len() <= 1 {
        coalesce_single(&stack[0], match_opts)?
    } else {
        coalesce_multi(stack, match_opts)?
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

fn grid_to_coalesce_entry<T: Borrow<GridStore> + Clone>(
    grid: &MatchEntry,
    subquery: &PhrasematchSubquery<T>,
    match_opts: &MatchOpts,
) -> CoalesceEntry {
    // Zoom has been adjusted in coalesce_multi, or correct zoom has been passed in for coalesce_single
    debug_assert!(match_opts.zoom == subquery.zoom);
    // TODO: do we need to check for bbox here?
    let relev = grid.grid_entry.relev * subquery.weight;

    CoalesceEntry {
        grid_entry: GridEntry { relev, ..grid.grid_entry },
        matches_language: grid.matches_language,
        idx: subquery.idx,
        tmp_id: ((subquery.idx as u32) << 25) + grid.grid_entry.id,
        mask: subquery.mask,
        distance: grid.distance,
        scoredist: grid.scoredist,
    }
}

fn coalesce_single<T: Borrow<GridStore> + Clone>(
    subquery: &PhrasematchSubquery<T>,
    match_opts: &MatchOpts,
) -> Result<Vec<CoalesceContext>, Error> {
    let grids = subquery.store.borrow().get_matching(&subquery.match_key, match_opts)?;
    let mut contexts: Vec<CoalesceContext> = Vec::new();
    let mut max_relev: f64 = 0.;
    // TODO: rename all of the last things to previous things
    let mut last_id: u32 = 0;
    let mut last_relev: f64 = 0.;
    let mut last_scoredist: f64 = 0.;
    let mut min_scoredist = std::f64::MAX;
    let mut feature_count: usize = 0;
    let bigger_max = 2 * MAX_CONTEXTS;

    for grid in grids {
        let coalesce_entry = grid_to_coalesce_entry(&grid, subquery, match_opts);

        // If it's the same feature as the last one, but a lower scoredist don't add it
        if last_id == coalesce_entry.grid_entry.id && coalesce_entry.scoredist <= last_scoredist {
            continue;
        }

        if feature_count > bigger_max {
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
        if match_opts.proximity.is_none() && feature_count > bigger_max {
            break;
        }
        if coalesce_entry.scoredist < min_scoredist {
            min_scoredist = coalesce_entry.scoredist;
        }
        last_id = coalesce_entry.grid_entry.id;
        last_relev = coalesce_entry.grid_entry.relev;
        last_scoredist = coalesce_entry.scoredist;
    }

    contexts.sort_by_key(|context| {
        (
            Reverse(OrderedFloat(context.relev)),
            Reverse(OrderedFloat(context.entries[0].scoredist)),
            context.entries[0].grid_entry.x,
            context.entries[0].grid_entry.y,
            context.entries[0].grid_entry.id,
        )
    });

    contexts.dedup_by_key(|context| context.entries[0].grid_entry.id);
    contexts.truncate(MAX_CONTEXTS);
    Ok(contexts)
}

fn coalesce_multi<T: Borrow<GridStore> + Clone>(
    mut stack: Vec<PhrasematchSubquery<T>>,
    match_opts: &MatchOpts,
) -> Result<Vec<CoalesceContext>, Error> {
    stack.sort_by_key(|subquery| (subquery.zoom, subquery.idx));

    let mut coalesced: HashMap<(u16, u16, u16), Vec<CoalesceContext>> = HashMap::new();
    let mut contexts: Vec<CoalesceContext> = Vec::new();

    let mut max_relev: f64 = 0.;

    for (i, subquery) in stack.iter().enumerate() {
        let compatible_zooms: Vec<u16> = stack
            .iter()
            .filter_map(|subquery_b| {
                if subquery.idx == subquery_b.idx || subquery.zoom < subquery_b.zoom {
                    None
                } else {
                    Some(subquery_b.zoom)
                }
            })
            .dedup()
            .collect();
        // TODO: check if zooms are equivalent here, and only call adjust_to_zoom if they arent?
        // That way we could avoid a function call and creating a cloned object in the common case where the zooms are the same
        let adjusted_match_opts = match_opts.adjust_to_zoom(subquery.zoom);
        let grids =
            subquery.store.borrow().get_matching(&subquery.match_key, &adjusted_match_opts)?;

        for grid in grids.take(100_000) {
            let coalesce_entry = grid_to_coalesce_entry(&grid, subquery, &adjusted_match_opts);

            let zxy = (subquery.zoom, grid.grid_entry.x, grid.grid_entry.y);

            let mut context_mask = coalesce_entry.mask;
            let mut context_relev = coalesce_entry.grid_entry.relev;
            let mut entries: Vec<CoalesceEntry> = vec![coalesce_entry];

            // See which other zooms are compatible.
            // These should all be lower zooms, so "zoom out" by dividing by 2^(difference in zooms)
            for other_zoom in compatible_zooms.iter() {
                let scale_factor: u16 = 1 << (subquery.zoom - other_zoom);
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
                                context_relev -= prev_relev;
                                context_relev += parent_entry.grid_entry.relev;

                                prev_mask = parent_entry.mask;
                                prev_relev = parent_entry.grid_entry.relev;
                            } else if context_mask & parent_entry.mask == 0 {
                                entries.push(parent_entry.clone());

                                context_relev += parent_entry.grid_entry.relev;
                                context_mask = context_mask | parent_entry.mask;

                                prev_mask = parent_entry.mask;
                                prev_relev = parent_entry.grid_entry.relev;
                            }
                        }
                    }
                }
            }
            if context_relev > max_relev {
                max_relev = context_relev;
            }

            if i == (stack.len() - 1) {
                if entries.len() == 1 {
                    // Slightly penalize contexts that have no stacking
                    context_relev -= 0.01;
                } else if entries[0].mask > entries[1].mask {
                    // Slightly penalize contexts in ascending order
                    context_relev -= 0.01
                }

                if max_relev - context_relev < 0.25 {
                    contexts.push(CoalesceContext {
                        entries,
                        mask: context_mask,
                        relev: context_relev,
                    });
                }
            } else if i == 0 || entries.len() > 1 {
                if let Some(already_coalesced) = coalesced.get_mut(&zxy) {
                    already_coalesced.push(CoalesceContext {
                        entries,
                        mask: context_mask,
                        relev: context_relev,
                    });
                } else {
                    coalesced.insert(
                        zxy,
                        vec![CoalesceContext { entries, mask: context_mask, relev: context_relev }],
                    );
                }
            }
        }
    }

    for (_, matched) in coalesced {
        for context in matched {
            if max_relev - context.relev < 0.25 {
                contexts.push(context);
            }
        }
    }

    contexts.sort_by_key(|context| {
        (
            Reverse(OrderedFloat(context.relev)),
            Reverse(OrderedFloat(context.entries[0].scoredist)),
            context.entries[0].idx,
            context.entries[0].grid_entry.id,
            context.entries[0].grid_entry.x,
            context.entries[0].grid_entry.y,
        )
    });

    Ok(contexts)
}

/// Calculates the tile distance between a proximity x and y and a grid x and y
pub fn tile_dist(proximity_x: u16, proximity_y: u16, grid_x: u16, grid_y: u16) -> f64 {
    let dx = (proximity_x as f64) - (grid_x as f64);
    let dy = (proximity_y as f64) - (grid_y as f64);
    ((dx * dx) + (dy * dy)).sqrt()
}

#[test]
fn tile_dist_test() {
    assert_eq!(
        tile_dist(1, 1, 1, 1),
        0.,
        "Grid with the same x and y as as the proximity x and y should have tile_dist 0"
    );
    assert_eq!(
        tile_dist(1, 1, 1, 0),
        1.,
        "Grid one tile away from proximity tile should have tile_dist 1"
    );
    assert_eq!(
        tile_dist(1, 1, 0, 0),
        1.4142135623730951,
        "Grid diagonal from proximity tile should have tile_dist between 0 and 1 "
    );
}

/// Returns the number of tiles per mile for a given zoom level
const fn tiles_per_mile_by_zoom(zoom: u16) -> f64 {
    // Array of the pre-calculated ratio of number of tiles per mile at each zoom level
    //
    // 32 tiles is about 40 miles at z14, use this as our mile <=> tile conversion.
    // The formula is (32 / 40 ) / 1.5^(14-zoom).
    // Pow functions are not supported in constant functions in rust,
    // and a custom constant pow function can't be implemented because if statements and loops are not yet supported in constant functions.
    //
    // Note: the formula uses 1.5^(14-zoom) instead of 2^(14-zoom) to be consistent with current behavior,
    // but a truly consistent radius scaled across zoom levels would use 2 as the base.
    // (See https://github.com/mapbox/carmen-cache/pull/110#discussion_r136497028)
    const TILES_PER_MILE_BY_ZOOM: [f64; 17] = [
        0.002740389912625401,
        0.004110584868938102,
        0.006165877303407152,
        0.009248815955110727,
        0.013873223932666092,
        0.020809835898999138,
        0.031214753848498707,
        0.046822130772748057,
        0.07023319615912209,
        0.10534979423868314,
        0.1580246913580247,
        0.23703703703703705,
        0.35555555555555557,
        0.5333333333333333,
        0.8,
        1.2000000000000002,
        1.8000000000000003,
    ];
    TILES_PER_MILE_BY_ZOOM[zoom as usize]
}

#[test]
fn tiles_per_mile_by_zoom_test() {
    assert_eq!(tiles_per_mile_by_zoom(14), 0.8, "Tiles per mile for zoom 14 should be 0.8");
    assert_eq!(
        tiles_per_mile_by_zoom(16),
        1.8000000000000003,
        "Tiles per mile should work for up to zoom 16"
    );
    assert_eq!(
        tiles_per_mile_by_zoom(6),
        0.031214753848498707,
        "Tiles per mile should work for down to zoom 6"
    );
}

/// Convert proximity radius from miles into scaled number of tiles
#[inline]
fn proximity_radius(zoom: u16, radius: f64) -> f64 {
    debug_assert!(zoom <= 16);
    // In carmen-cache, there's an array of pre-calculated values for zooms 6-14, otherwise it does the exact same calculation as zoomTileRadius (now tiles_per_mile)
    // Does this even need to be a function?
    radius * tiles_per_mile_by_zoom(zoom)
}

#[test]
fn proximity_radius_test() {
    assert_eq!(
        proximity_radius(14, 400.),
        320.,
        "Proximity radius in tiles for zoom 14, radius 400 is as expected"
    );
    assert_eq!(
        proximity_radius(16, 400.),
        720.0000000000001,
        "proximity_radius should work for zoom 14"
    );
    assert_eq!(proximity_radius(6, 0.), 0., "proximity_radius for a radius of 0 should be 0");
    assert_eq!(
        proximity_radius(6, 40.),
        1.2485901539399482,
        "proximity_radius in tiles for zoom 6, radius 40 is as expected"
    );
    // TODO: test zoom > 14?
}

// We don't know the scale of the axis we're modeling, but it doesn't really
// matter as we just need internal consistency.
const E_POW: [f64; 8] = [
    1.,
    2.718281828459045,
    7.38905609893065,
    20.085536923187668,
    54.598150033144236,
    148.4131591025766,
    403.4287934927351,
    1096.6331584284585,
];

pub fn scoredist(mut zoom: u16, mut distance: f64, mut score: u8, radius: f64) -> f64 {
    if zoom < 6 {
        zoom = 6;
    }
    if score > 7 {
        score = 7;
    }

    // If the distance is 0, set a minimum distance to avoid dividing by distratios that approach zero
    if distance < 1. {
        distance = 0.8;
    }

    let mut dist_ratio: f64 = distance / proximity_radius(zoom, radius);

    // Beyond the proximity radius just let scoredist be driven by score.
    if dist_ratio > 1.0 {
        dist_ratio = 1.00;
    }
    ((6. * E_POW[score as usize] / E_POW[7]) + 1.) / dist_ratio
}

#[test]
fn scoredist_test() {
    assert_eq!(scoredist(14, 1., 0, 400.), 321.7508133738646, "scoredist for a feature 1 tile away from proximity point with score 0 and radius 400 should be 321.7508133738646");
    assert_eq!(scoredist(14, 0., 0, 400.), 402.1885167173308, "scoredist for a feature on the same tile as the proximity point with score 0 and radius 400 should be 402.1885167173308,");
}
