use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use byteorder::{BigEndian, LittleEndian, ReadBytesExt};
use failure::Error;
use flatbuffers;
use itertools::Itertools;
use min_max_heap::MinMaxHeap;
use morton::deinterleave_morton;
use ordered_float::OrderedFloat;
use rocksdb::{DBCompressionType, Direction, IteratorMode, Options, DB};

use crate::gridstore::common::*;
use crate::gridstore::gridstore_generated::*;
use crate::gridstore::spatial;

#[derive(Debug)]
pub struct GridStore {
    db: DB,
    pub path: PathBuf,
}

// this is a bit of a hack -- it constructs a flatbuffers vector bounded by the lifetime
// of the underlying buffer, rather than by the lifetime of its parent vector, in the event
// that vectors are nested
fn get_vector<'a, T: 'a>(
    buf: &'a [u8],
    table: &flatbuffers::Table,
    field: flatbuffers::VOffsetT,
) -> Option<flatbuffers::Vector<'a, flatbuffers::ForwardsUOffset<T>>> {
    let o = table.vtable().get(field) as usize;
    if o == 0 {
        return None;
    }

    let addr = table.loc + o;
    let offset = (&buf[addr..(addr + 4)]).read_u32::<LittleEndian>().unwrap() as usize;
    Some(flatbuffers::Vector::new(buf, addr + offset))
}

#[inline]
fn decode_value<T: AsRef<[u8]>>(value: T) -> impl Iterator<Item = GridEntry> {
    let record_ref = {
        let value_ref: &[u8] = value.as_ref();
        // this is pretty sketch: we're opting out of compiler lifetime protection
        // for this reference. This usage should be safe though, because we'll move the
        // reference and the underlying owned object around together as a unit (the
        // tuple below) so that when we pull the reference into the inner closures,
        // we'll drag the owned object along, and won't drop it until the whole
        // nest of closures is deleted
        let static_ref: &'static [u8] = unsafe { std::mem::transmute(value_ref) };
        (value, static_ref)
    };
    let record = get_root_as_phrase_record(record_ref.1);
    let rs_vec =
        get_vector::<RelevScore>(record_ref.1, &record._tab, PhraseRecord::VT_RELEV_SCORES)
            .unwrap();
    let id_vec =
        get_vector::<IdList>(record_ref.1, &record._tab, PhraseRecord::VT_ID_LISTS).unwrap();

    let iter = rs_vec.iter().flat_map(move |rs_obj| {
        // grab a reference to the outer object to make sure it doesn't get freed
        let _ref = &record_ref;

        let relev_score = rs_obj.relev_score();
        let relev = relev_int_to_float(relev_score >> 4);
        // mask for the least significant four bits
        let score = relev_score & 15;

        let coords = rs_obj.coords().unwrap();

        coords.into_iter().flat_map(move |coords_obj| {
            let (x, y) = deinterleave_morton(coords_obj.coord());
            let id_list_idx = coords_obj.id_list() as usize;

            let ids = id_vec.get(id_list_idx).ids();

            ids.unwrap().iter().map(move |id_comp| {
                let id = id_comp >> 8;
                let source_phrase_hash = (id_comp & 255) as u8;
                GridEntry { relev, score, x, y, id, source_phrase_hash }
            })
        })
    });
    iter
}

#[inline]
fn decode_matching_value<T: AsRef<[u8]>>(value: T, match_opts: &MatchOpts, matches_language: bool) -> impl Iterator<Item = MatchEntry> {
    let match_opts = match_opts.clone();

    let record_ref = {
        let value_ref: &[u8] = value.as_ref();
        // this is pretty sketch: we're opting out of compiler lifetime protection
        // for this reference. This usage should be safe though, because we'll move the
        // reference and the underlying owned object around together as a unit (the
        // tuple below) so that when we pull the reference into the inner closures,
        // we'll drag the owned object along, and won't drop it until the whole
        // nest of closures is deleted
        let static_ref: &'static [u8] = unsafe { std::mem::transmute(value_ref) };
        (value, static_ref)
    };
    let record = get_root_as_phrase_record(record_ref.1);
    let rs_vec =
        get_vector::<RelevScore>(record_ref.1, &record._tab, PhraseRecord::VT_RELEV_SCORES)
            .unwrap();
    let id_vec =
        get_vector::<IdList>(record_ref.1, &record._tab, PhraseRecord::VT_ID_LISTS).unwrap();

    let iter = rs_vec.iter().flat_map(move |rs_obj| {
        // grab a reference to the outer object to make sure it doesn't get freed
        let _ref = &record_ref;

        let relev_score = rs_obj.relev_score();
        let relev = relev_int_to_float(relev_score >> 4);
        // mask for the least significant four bits
        let score = relev_score & 15;

        let coords_vec = rs_obj.coords().unwrap();

        let coords = match &match_opts {
            MatchOpts { bbox: None, proximity: None, .. } => {
                Some(Box::new(coords_vec.into_iter()) as Box<Iterator<Item = &Coord>>)
            }
            MatchOpts { bbox: Some(bbox), proximity: None, .. } => {
                // TODO should the bbox argument be changed to a reference in bbox? The compiler was complaining
                match spatial::bbox_filter(coords_vec, *bbox) {
                    Some(v) => Some(Box::new(v) as Box<Iterator<Item = &Coord>>),
                    None => None,
                }
            }
            MatchOpts { bbox: None, proximity: Some(prox_pt), .. } => {
                match spatial::proximity(coords_vec, prox_pt.point) {
                    Some(v) => Some(Box::new(v) as Box<Iterator<Item = &Coord>>),
                    None => None,
                }
            }
            MatchOpts { bbox: Some(bbox), proximity: Some(prox_pt), .. } => {
                match spatial::bbox_proximity_filter(coords_vec, *bbox, prox_pt.point) {
                    Some(v) => Some(Box::new(v) as Box<Iterator<Item = &Coord>>),
                    None => None,
                }
            }
        };

        let coords = coords.unwrap_or_else(|| Box::new((Option::<&Coord>::None).into_iter()) as Box<Iterator<Item = &Coord>>);

        let match_opts = match_opts.clone();
        coords.flat_map(move |coords_obj| {
            let (x, y) = deinterleave_morton(coords_obj.coord());

            let (distance, within_radius, scoredist) = match &match_opts {
                MatchOpts { proximity: Some(prox_pt), zoom, .. } => {
                    let distance =
                        spatial::tile_dist(prox_pt.point[0], prox_pt.point[1], x, y);
                    (
                        distance,
                        // The proximity radius calculation is also done in scoredist
                        // There could be an opportunity to optimize by doing it once
                        distance <= spatial::proximity_radius(*zoom, prox_pt.radius),
                        spatial::scoredist(*zoom, distance, score, prox_pt.radius),
                    )
                }
                _ => (0f64, false, score as f64),
            };

            let id_list_idx = coords_obj.id_list() as usize;

            let ids = id_vec.get(id_list_idx).ids();

            ids.unwrap().iter().map(move |id_comp| {
                let id = id_comp >> 8;
                let source_phrase_hash = (id_comp & 255) as u8;
                MatchEntry {
                    grid_entry: GridEntry {
                        relev: relev
                            * (if matches_language || within_radius {
                                1f64
                            } else {
                                0.96f64
                            }),
                        score,
                        x,
                        y,
                        id,
                        source_phrase_hash,
                    },
                    matches_language,
                    distance,
                    scoredist,
                }
            })
        })
    });
    iter
}

struct QueueElement<T: Iterator<Item = MatchEntry>> {
    next_entry: MatchEntry,
    entry_iter: T
}

impl<T: Iterator<Item = MatchEntry>> QueueElement<T> {
    fn sort_key(&self) -> (OrderedFloat<f64>, OrderedFloat<f64>, u16, u16, u32) {
        (
            OrderedFloat(self.next_entry.grid_entry.relev),
            OrderedFloat(self.next_entry.scoredist),
            self.next_entry.grid_entry.x,
            self.next_entry.grid_entry.y,
            self.next_entry.grid_entry.id
        )
    }
}

impl<T: Iterator<Item = MatchEntry>> Ord for QueueElement<T> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.sort_key().cmp(&other.sort_key())
    }
}

impl<T: Iterator<Item = MatchEntry>> PartialOrd for QueueElement<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<T: Iterator<Item = MatchEntry>> PartialEq for QueueElement<T> {
    fn eq(&self, other: &Self) -> bool {
        self.sort_key() == other.sort_key()
    }
}

impl<T: Iterator<Item = MatchEntry>> Eq for QueueElement<T> {}

impl GridStore {
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self, Error> {
        let path = path.as_ref().to_owned();
        let mut opts = Options::default();
        opts.set_read_only(true);
        opts.set_compression_type(DBCompressionType::Lz4hc);
        opts.set_allow_mmap_reads(true);
        let db = DB::open(&opts, &path)?;
        Ok(GridStore { db, path })
    }

    #[inline(never)]
    pub fn get(&self, key: &GridKey) -> Result<Option<impl Iterator<Item = GridEntry>>, Error> {
        let mut db_key: Vec<u8> = Vec::new();
        key.write_to(0, &mut db_key)?;

        Ok(match self.db.get(&db_key)? {
            Some(value) => Some(decode_value(value)),
            None => None,
        })
    }

    pub fn eager_get_matching(
        &self,
        match_key: &MatchKey,
        match_opts: &MatchOpts,
        max_values: usize,
    ) -> Result<Vec<MatchEntry>, Error> {
        let mut range_list: Vec<(u32, u32, u8)> = Vec::new();
        match match_key.match_phrase {
            MatchPhrase::Exact(id) => range_list.push((id, id + 1, 0)),
            MatchPhrase::Range { start, end } => {
                let remainder = start % 1024;
                let prefix_start = if remainder == 0 { start } else { start + (1024 - remainder) };
                let remainder = end % 1024;
                let prefix_end = if remainder == 0 { end } else { end - remainder };
                if prefix_start >= prefix_end {
                    range_list.push((start, end, 0));
                } else {
                    if start != prefix_start {
                        range_list.push((start, prefix_start, 0));
                    }
                    range_list.push((prefix_start, prefix_end, 1));
                    if end != prefix_end {
                        range_list.push((prefix_end, end, 0));
                    }
                }
            }
        }

        let match_opts = match_opts.clone();

        let mut pri_queue = MinMaxHeap::<QueueElement<_>>::new();

        for (start, end, type_marker) in range_list {
            let mut range_key = match_key.clone();
            range_key.match_phrase = MatchPhrase::Range { start, end };
            let mut db_key: Vec<u8> = Vec::new();
            range_key.write_start_to(type_marker, &mut db_key)?;

            let db_iter = self
                .db
                .iterator(IteratorMode::From(&db_key, Direction::Forward))
                .take_while(|(k, _)| range_key.matches_key(type_marker, k).unwrap());

            for (key, value) in db_iter {
                let matches_language = match_key.matches_language(&key).unwrap();
                let mut entry_iter = decode_matching_value(value, &match_opts, matches_language);
                if let Some(next_entry) = entry_iter.next() {
                    let queue_element = QueueElement { next_entry, entry_iter };
                    if pri_queue.len() >= max_values {
                        let worst_entry = pri_queue.peek_min().unwrap();
                        if worst_entry >= &queue_element {
                            continue;
                        } else {
                            pri_queue.replace_min(queue_element);
                        }
                    } else {
                        pri_queue.push(queue_element);
                    }
                }
            }
        }

        let mut out = Vec::new();
        while out.len() < max_values && pri_queue.len() > 0 {
            let mut best_entry = pri_queue.pop_max().unwrap();
            if let Some(mut next_entry) = best_entry.entry_iter.next() {
                std::mem::swap(&mut next_entry, &mut (best_entry.next_entry));
                out.push(next_entry);
                pri_queue.push(best_entry);
            } else {
                out.push(best_entry.next_entry);
            }
        }
        Ok(out)
    }

    // this is only called this because of inertia -- I'm open to a rename
    #[inline(never)]
    pub fn get_matching(
        &self,
        match_key: &MatchKey,
        match_opts: &MatchOpts,
    ) -> Result<impl Iterator<Item = MatchEntry>, Error> {
        let mut range_list: Vec<(u32, u32, u8)> = Vec::new();
        match match_key.match_phrase {
            MatchPhrase::Exact(id) => range_list.push((id, id + 1, 0)),
            MatchPhrase::Range { start, end } => {
                let remainder = start % 1024;
                let prefix_start = if remainder == 0 { start } else { start + (1024 - remainder) };
                let remainder = end % 1024;
                let prefix_end = if remainder == 0 { end } else { end - remainder };
                if prefix_start >= prefix_end {
                    range_list.push((start, end, 0));
                } else {
                    if start != prefix_start {
                        range_list.push((start, prefix_start, 0));
                    }
                    range_list.push((prefix_start, prefix_end, 1));
                    if end != prefix_end {
                        range_list.push((prefix_end, end, 0));
                    }
                }
            }
        }

        let match_opts = match_opts.clone();
        let mut record_refs: Vec<(Box<[u8]>, &'static [u8], bool)> = Vec::new();
        for (start, end, type_marker) in range_list {
            let mut range_key = match_key.clone();
            range_key.match_phrase = MatchPhrase::Range { start, end };
            let mut db_key: Vec<u8> = Vec::new();
            range_key.write_start_to(type_marker, &mut db_key)?;

            let db_iter = self
                .db
                .iterator(IteratorMode::From(&db_key, Direction::Forward))
                .take_while(|(k, _)| range_key.matches_key(type_marker, k).unwrap());

            for (key, value) in db_iter {
                let matches_language = match_key.matches_language(&key).unwrap();
                let record_ref = {
                    let value_ref: &[u8] = value.as_ref();
                    // same approach as in get above -- maybe sketchy
                    let static_ref: &'static [u8] = unsafe { std::mem::transmute(value_ref) };
                    (value, static_ref, matches_language)
                };
                record_refs.push(record_ref);
            }
        }

        // eagerly bucket all the relev/score chunks from all the groups
        // DB entries into a single set
        let mut coords_for_relev = BTreeMap::new();
        for record_ref in &record_refs {
            let record = get_root_as_phrase_record(record_ref.1);
            let rs_vec =
                get_vector::<RelevScore>(record_ref.1, &record._tab, PhraseRecord::VT_RELEV_SCORES)
                    .unwrap();
            let id_list_vec =
                get_vector::<IdList>(record_ref.1, &record._tab, PhraseRecord::VT_ID_LISTS)
                    .unwrap();

            let matches_language = record_ref.2;

            for rs_obj in rs_vec {
                let relev_score = rs_obj.relev_score();
                let relev = relev_int_to_float(relev_score >> 4);
                // mask for the least significant four bits
                let score = relev_score & 15;

                let coords_vec = rs_obj.coords().unwrap();
                // TODO could this be a reference? The compiler was saying:
                // "cannot move out of captured variable in an `FnMut` closure"
                // "help: consider borrowing here: `&match_opts`rustc(E0507)""
                let coords = match &match_opts {
                    MatchOpts { bbox: None, proximity: None, .. } => {
                        Some(Box::new(coords_vec.into_iter()) as Box<Iterator<Item = &Coord>>)
                    }
                    MatchOpts { bbox: Some(bbox), proximity: None, .. } => {
                        // TODO should the bbox argument be changed to a reference in bbox? The compiler was complaining
                        match spatial::bbox_filter(coords_vec, *bbox) {
                            Some(v) => Some(Box::new(v) as Box<Iterator<Item = &Coord>>),
                            None => None,
                        }
                    }
                    MatchOpts { bbox: None, proximity: Some(prox_pt), .. } => {
                        match spatial::proximity(coords_vec, prox_pt.point) {
                            Some(v) => Some(Box::new(v) as Box<Iterator<Item = &Coord>>),
                            None => None,
                        }
                    }
                    MatchOpts { bbox: Some(bbox), proximity: Some(prox_pt), .. } => {
                        match spatial::bbox_proximity_filter(coords_vec, *bbox, prox_pt.point) {
                            Some(v) => Some(Box::new(v) as Box<Iterator<Item = &Coord>>),
                            None => None,
                        }
                    }
                };

                if coords.is_some() {
                    let slot =
                        coords_for_relev.entry(OrderedFloat(relev)).or_insert_with(|| vec![]);
                    slot.push((score, matches_language, coords.unwrap(), id_list_vec.clone()));
                }
            }
        }

        struct SortGroup<'a> {
            coords: Coord,
            id_lists: flatbuffers::Vector<'a, flatbuffers::ForwardsUOffset<IdList<'a>>>,
            scoredist: f64,
            x: u16,
            y: u16,
            score: u8,
            distance: f64,
            matches_language: bool,
            within_radius: bool,
        }

        let out = coords_for_relev.into_iter().rev().flat_map(move |(relev, coord_sets)| {
            let match_opts = match_opts.clone();
            // this is necessitated by the unsafe hackery above: we need to grab a reference
            // to ref_set so that it gets moved into the closure, so that its memory doesn't
            // get freed before we're done with it
            let _ref_set = &record_refs;
            let relev = relev.into_inner();
            // for each relev/score, lazily k-way-merge the child entities by z-order curve value
            let merged = coord_sets
                .into_iter()
                .map(move |(score, matches_language, coord_vec, id_lists)| {
                    let match_opts = match_opts.clone();
                    coord_vec.map(move |coords| {
                        let coord = coords.coord();
                        let (x, y) = deinterleave_morton(coord);
                        let (distance, within_radius, scoredist) = match &match_opts {
                            MatchOpts { proximity: Some(prox_pt), zoom, .. } => {
                                let distance =
                                    spatial::tile_dist(prox_pt.point[0], prox_pt.point[1], x, y);
                                (
                                    distance,
                                    // The proximity radius calculation is also done in scoredist
                                    // There could be an opportunity to optimize by doing it once
                                    distance <= spatial::proximity_radius(*zoom, prox_pt.radius),
                                    spatial::scoredist(*zoom, distance, score, prox_pt.radius),
                                )
                            }
                            _ => (0f64, false, score as f64),
                        };
                        SortGroup {
                            coords: *coords,
                            id_lists,
                            scoredist,
                            x,
                            y,
                            score,
                            distance,
                            matches_language,
                            within_radius,
                        }
                    })
                })
                .kmerge_by(|a, b| {
                    (a.matches_language || a.within_radius, a.scoredist)
                        .partial_cmp(&(b.matches_language || b.within_radius, b.scoredist))
                        .unwrap()
                        == Ordering::Greater
                });

            // group together entries from different keys that have the same scoredist, x, and y
            somewhat_eager_groupby(merged, |a| {
                ((*a).scoredist, (*a).x, (*a).y, (*a).matches_language, (*a).within_radius)
            })
            .flat_map(
                move |((scoredist, x, y, matches_language, within_radius), coords_obj_group)| {
                    // get all the feature IDs from all the entries with the same scoredist/X/Y, and eagerly
                    // combine them and sort descending if necessary (if there's only one entry,
                    // it's already sorted)
                    let mut distance = 0f64;
                    let mut score = 0;
                    let all_ids: Vec<u32> = match coords_obj_group.len() {
                        0 => Vec::new(),
                        1 => {
                            score = coords_obj_group[0].score;
                            distance = coords_obj_group[0].distance;

                            let id_list_idx = coords_obj_group[0].coords.id_list() as usize;
                            let ids = coords_obj_group[0].id_lists.get(id_list_idx).ids();

                            ids.unwrap().iter().collect()
                        }
                        _ => {
                            let mut ids = Vec::new();
                            score = coords_obj_group[0].score;
                            distance = coords_obj_group[0].distance;
                            for group in coords_obj_group {
                                let id_list_idx = group.coords.id_list() as usize;
                                let fb_ids = group.id_lists.get(id_list_idx).ids();
                                ids.extend(fb_ids.unwrap().iter());
                            }
                            ids.sort_by(|a, b| b.cmp(a));
                            ids.dedup();
                            ids
                        }
                    };

                    all_ids.into_iter().map(move |id_comp| {
                        let id = id_comp >> 8;
                        let source_phrase_hash = (id_comp & 255) as u8;
                        MatchEntry {
                            grid_entry: GridEntry {
                                relev: relev
                                    * (if matches_language || within_radius {
                                        1f64
                                    } else {
                                        0.96f64
                                    }),
                                score,
                                x,
                                y,
                                id,
                                source_phrase_hash,
                            },
                            matches_language,
                            distance,
                            scoredist,
                        }
                    })
                },
            )
        });

        Ok(out)
    }

    pub fn keys<'i>(&'i self) -> impl Iterator<Item = Result<GridKey, Error>> + 'i {
        let db_iter = self.db.iterator(IteratorMode::Start);
        db_iter.take_while(|(key, _)| key[0] == 0).map(|(key, _)| {
            let phrase_id = (&key[1..]).read_u32::<BigEndian>()?;

            let key_lang_partial = &key[5..];
            let lang_set: u128 = if key_lang_partial.len() == 0 {
                // 0-length language array is the shorthand for "matches everything"
                std::u128::MAX
            } else {
                let mut key_lang_full = [0u8; 16];
                key_lang_full[(16 - key_lang_partial.len())..].copy_from_slice(key_lang_partial);

                (&key_lang_full[..]).read_u128::<BigEndian>()?
            };

            Ok(GridKey { phrase_id, lang_set })
        })
    }

    pub fn iter<'i>(
        &'i self,
    ) -> impl Iterator<Item = Result<(GridKey, Vec<GridEntry>), Error>> + 'i {
        let db_iter = self.db.iterator(IteratorMode::Start);
        db_iter.take_while(|(key, _)| key[0] == 0).map(|(key, value)| {
            let phrase_id = (&key[1..]).read_u32::<BigEndian>()?;

            let key_lang_partial = &key[5..];
            let lang_set: u128 = if key_lang_partial.len() == 0 {
                // 0-length language array is the shorthand for "matches everything"
                std::u128::MAX
            } else {
                let mut key_lang_full = [0u8; 16];
                key_lang_full[(16 - key_lang_partial.len())..].copy_from_slice(key_lang_partial);

                (&key_lang_full[..]).read_u128::<BigEndian>()?
            };

            let entries: Vec<_> = decode_value(value).collect();

            Ok((GridKey { phrase_id, lang_set }, entries))
        })
    }
}
