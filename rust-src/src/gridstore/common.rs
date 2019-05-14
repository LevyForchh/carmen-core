use std::error::Error;

use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, PartialOrd, Ord, PartialEq, Eq, Clone)]
pub struct GridKey {
    pub phrase_id: u32,
    pub lang_set: u128,
}

impl GridKey {
    pub fn write_to(&self, type_marker: u8, db_key: &mut Vec<u8>) -> Result<(), Box<Error>> {
        db_key.push(type_marker);
        // next goes the ID
        db_key.write_u32::<BigEndian>(self.phrase_id)?;
        // now the language ID
        match self.lang_set {
            std::u128::MAX => { /* do nothing -- this is the all-languages marker */ }
            0 => {
                db_key.push(0);
            }
            _ => {
                let lang_set = self.lang_set.to_be_bytes();
                let iter = lang_set.iter().skip_while(|byte| **byte == 0u8);
                db_key.extend(iter);
            }
        }
        Ok(())
    }
}

#[derive(Serialize, Deserialize, Debug, PartialOrd, Ord, PartialEq, Eq, Clone)]
pub enum MatchPhrase {
    Exact(u32),
    Range { start: u32, end: u32 },
}

#[derive(Serialize, Deserialize, Debug, PartialOrd, Ord, PartialEq, Eq, Clone)]
pub struct MatchKey {
    pub match_phrase: MatchPhrase,
    pub lang_set: u128,
}

impl MatchKey {
    pub fn write_start_to(&self, type_marker: u8, db_key: &mut Vec<u8>) -> Result<(), Box<Error>> {
        db_key.push(type_marker);
        // next goes the ID
        let start = match self.match_phrase {
            MatchPhrase::Exact(phrase_id) => phrase_id,
            MatchPhrase::Range { start, .. } => start,
        };
        db_key.write_u32::<BigEndian>(start)?;
        Ok(())
    }

    pub fn matches_key(&self, db_key: &[u8]) -> Result<bool, Box<Error>> {
        let key_phrase = (&db_key[1..]).read_u32::<BigEndian>()?;
        Ok(match self.match_phrase {
            MatchPhrase::Exact(phrase_id) => phrase_id == key_phrase,
            MatchPhrase::Range { start, end } => start <= key_phrase && key_phrase < end,
        })
    }

    pub fn matches_language(&self, db_key: &[u8]) -> Result<bool, Box<Error>> {
        let key_lang_partial = &db_key[5..];
        if key_lang_partial.len() == 0 {
            // 0-length language array is the shorthand for "matches everything"
            return Ok(true);
        }

        let mut key_lang_full = [0u8; 16];
        key_lang_full[(16 - key_lang_partial.len())..].copy_from_slice(key_lang_partial);

        let key_lang_set: u128 = (&key_lang_full[..]).read_u128::<BigEndian>()?;

        Ok(self.lang_set & key_lang_set != 0)
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct Proximity {
    pub point: [u16; 2],
    pub radius: f64,
}

#[derive(Debug, PartialEq, Clone)]
pub struct MatchOpts {
    pub bbox: Option<[u16; 4]>,
    pub proximity: Option<Proximity>,
    pub zoom: u16,
}

impl Default for MatchOpts {
    fn default() -> Self {
        MatchOpts { bbox: None, proximity: None, zoom: 16 }
    }
}

impl MatchOpts {
    fn adjust_to_zoom(&self, target_z: u16) -> MatchOpts {
        let z_diff = target_z as i16 - self.zoom as i16;
        if z_diff == 0 {
            self.clone()
        } else {
            let mut adjusted_match_opts = MatchOpts { zoom: target_z, ..MatchOpts::default() };
            if let Some(orig_proximity) = &self.proximity {
                if z_diff < 0 {
                    // If this is a zoom out, just divide by 2 for every level of zooming out
                    let zoom_levels = z_diff.abs();
                    adjusted_match_opts.proximity = Some(Proximity {
                        // Shifting to the right by a number is the same as dividing by 2 that number of times
                        point: [
                            orig_proximity.point[0] >> zoom_levels,
                            orig_proximity.point[1] >> zoom_levels,
                        ],
                        radius: orig_proximity.radius,
                    });
                } else {
                    // If this is a zoom in, choose the closest to the middle of the possible tiles at the higher zoom level
                    // The scale of the coordinates for zooming in is 2^(difference in zs)
                    let scale_multiplier = 1 << z_diff;
                    // Pick a coordinate halfway between the possible higher zoom tiles, subtracting one to pick the one on the top left of the four middle tiles for consistency
                    let midpoint_coord_adjuster = scale_multiplier / 2 - 1;
                    let adjusted_coords: Vec<u16> = orig_proximity
                        .point
                        .iter()
                        .map(|coord| coord * scale_multiplier + midpoint_coord_adjuster)
                        .collect();
                    adjusted_match_opts.proximity = Some(Proximity {
                        point: [adjusted_coords[0], adjusted_coords[1]],
                        radius: orig_proximity.radius,
                    });
                }
            }
            if let Some(orig_bbox) = &self.bbox {
                let [mut min_x, mut min_y, mut max_x, mut max_y] = orig_bbox;
                if z_diff < 0 {
                    let zoom_levels = z_diff.abs();
                    // TODO: is it more performant to just do these 4 calculations, or to turn it into an iter, then map, then collect, then access items in the vector?
                    // If this is a zoom out, just divide each coordinate by 2^(positive zoom diff). This is the same as shifting bits to the right.
                    min_x = min_x >> zoom_levels;
                    min_y = min_y >> zoom_levels;
                    max_x = max_x >> zoom_levels;
                    max_y = max_y >> zoom_levels;
                    adjusted_match_opts.bbox = Some([min_x, min_y, max_x, max_y]);
                } else {
                    // If this is a zoom in
                    let scale_multiplier = 1 << z_diff;
                    // Scale the top left (min x and y) tile coordinates by 2^(zoom diff).
                    min_x = min_x * scale_multiplier;
                    min_y = min_y * scale_multiplier;
                    // Scale the bottom right (max x and y) tile coordinates by 2^(zoom diff), and add the new number of tiles (-1) to get the outer edge of possible tiles
                    max_x = max_x * scale_multiplier + scale_multiplier - 1;
                    max_y = max_y * scale_multiplier + scale_multiplier - 1;
                    adjusted_match_opts.bbox = Some([min_x, min_y, max_x, max_y]);
                }
            }
            adjusted_match_opts.clone()
            // TODO: error handling?
        }
    }
}

#[test]
fn adjust_to_zoom_test_proximity() {
    let match_opts1 = MatchOpts {
        proximity: Some(Proximity { point: [2, 28], radius: 400. }),
        zoom: 14,
        ..MatchOpts::default()
    };
    let adjusted_match_opts1 = match_opts1.adjust_to_zoom(6);
    assert_eq!(adjusted_match_opts1.zoom, 6, "Adjusted MatchOpts should have target zoom as zoom");
    assert_eq!(adjusted_match_opts1.proximity.unwrap().point, [0, 0], "should be 0,0");

    let match_opts2 = MatchOpts {
        proximity: Some(Proximity { point: [11, 25], radius: 400. }),
        zoom: 6,
        ..MatchOpts::default()
    };
    let adjusted_match_opts2 = match_opts2.adjust_to_zoom(8);
    assert_eq!(adjusted_match_opts2.zoom, 8, "Adjusted MatchOpts should have target zoom as zoom");
    assert_eq!(adjusted_match_opts2.proximity.unwrap().point, [45, 101], "Should be 45, 101");

    let match_opts3 = MatchOpts {
        proximity: Some(Proximity { point: [6, 6], radius: 400. }),
        zoom: 4,
        ..MatchOpts::default()
    };
    // TODO: a function taht takes an original, new zoom, and expected zxy and generates these?
    // TODO: remove some of the tests for the radius and that the new zoom is as expected?
    let same_zoom = match_opts3.adjust_to_zoom(4);
    assert_eq!(same_zoom, match_opts3, "If the zoom is the same as the original, adjusted MatchOpts should be a clone of the original");
    let zoomed_out_1z = match_opts3.adjust_to_zoom(3);
    let proximity_out_1z = zoomed_out_1z.proximity.unwrap();
    assert_eq!(proximity_out_1z.point, [3, 3], "4/6/6 zoomed out to zoom 3 should be 3/3/3");
    assert_eq!(proximity_out_1z.radius, 400., "The adjusted radius should be the original radius");
    assert_eq!(zoomed_out_1z.zoom, 3, "The adjusted zoom should be the target zoom");
    let zoomed_out_2z = match_opts3.adjust_to_zoom(2);
    let proximity_out_2z = zoomed_out_2z.proximity.unwrap();
    assert_eq!(proximity_out_2z.point, [1, 1], "4/6/6 zoomed out to zoom 2 should be 2/1/1");
    assert_eq!(proximity_out_2z.radius, 400., "The adjusted radius should be the original radius");
    assert_eq!(zoomed_out_2z.zoom, 2, "The adjusted zoom should be the target zoom");
    let zoomed_in_1z = match_opts3.adjust_to_zoom(5);
    let proximity_in_1z = zoomed_in_1z.proximity.unwrap();
    assert_eq!(proximity_in_1z.point, [12, 12], "4/6/6 zoomed in to zoom 5 should be 5/12/12");
    assert_eq!(proximity_in_1z.radius, 400., "The adjusted radius should be the original radius");
    assert_eq!(zoomed_in_1z.zoom, 5, "The adjusted zoom should be the target zoom");
    let zoomed_in_2z = match_opts3.adjust_to_zoom(6);
    let proximity_in_2z = zoomed_in_2z.proximity.unwrap();
    assert_eq!(proximity_in_2z.point, [25, 25], "4/6/6 zoomed in to zoom 6 should be 6/25/25");
    assert_eq!(proximity_in_2z.radius, 400., "The adjusted radius should be the original radius");
    assert_eq!(zoomed_in_2z.zoom, 6, "The adjusted zoom should be the target zoom");
    let zoomed_in_3z = match_opts3.adjust_to_zoom(7);
    let proximity_in_3z = zoomed_in_3z.proximity.unwrap();
    assert_eq!(proximity_in_3z.point, [51, 51], "4/6/6 zoomed in to zoom 7 should be 7/51/51");
    assert_eq!(proximity_in_3z.radius, 400., "The adjusted radius should be the original radius");
    assert_eq!(zoomed_in_3z.zoom, 7, "The adjusted zoom should be the target zoom");
}

#[test]
fn adjust_to_zoom_text_bbox() {
    // Test case where single parent tile contains entire bbox
    let match_opts = MatchOpts { bbox: Some([6, 4, 7, 5]), zoom: 4, ..MatchOpts::default() };
    let zoomed_out_1z = match_opts.adjust_to_zoom(3);
    assert_eq!(zoomed_out_1z.bbox.unwrap(), [3,2,3,2], "Bbox covering 4 tiles zoomed out 1z can be 1 parent tile if it contains all 4 original tiles");
    assert_eq!(zoomed_out_1z.zoom, 3, "The adjusted zoom should be the target zoom");
    let zoomed_back_in_1z = zoomed_out_1z.adjust_to_zoom(4);
    assert_eq!(
        zoomed_back_in_1z, match_opts,
        "The zoomed in bbox from 1 parent tile should include the 4 tiles it contains"
    );

    // Test case where higher zoom level bbox spans multiple parent tiles
    let match_opts2 = MatchOpts { bbox: Some([6, 5, 7, 6]), zoom: 4, ..MatchOpts::default() };
    let zoomed_out_1z_2 = match_opts2.adjust_to_zoom(3);
    assert_eq!(
        zoomed_out_1z_2.bbox.unwrap(),
        [3, 2, 3, 3],
        "Bboxes that span two parent tiles should return a bbox that includes both parent tiles"
    );
    let zoomed_back_in_1z_2 = zoomed_out_1z_2.adjust_to_zoom(4);
    assert_eq!(
        zoomed_back_in_1z_2.bbox.unwrap(),
        [6, 4, 7, 7],
        "The zoomed in bbox from 2 parent tiles should include all 8 tiles they contain"
    );

    // Gut check simple case
    let simple_match_opts = MatchOpts { bbox: Some([3, 3, 3, 3]), zoom: 3, ..MatchOpts::default() };
    assert_eq!(
        simple_match_opts.adjust_to_zoom(4).bbox.unwrap(),
        [6, 6, 7, 7],
        "[3,3,3,3] is correctly scaled to zoom 4"
    );
    assert_eq!(
        simple_match_opts.adjust_to_zoom(5).bbox.unwrap(),
        [12, 12, 15, 15],
        "[3,3,3,3] is correctly scaled to zoom 5"
    );

    // Multi-tile parent bbox zoom in
    let multi_tile_match_opts =
        MatchOpts { bbox: Some([5, 3, 7, 4]), zoom: 3, ..MatchOpts::default() };
    assert_eq!(
        multi_tile_match_opts.adjust_to_zoom(4).bbox.unwrap(),
        [10, 6, 15, 9],
        "Multi-tile parent zoomed in one zoom level includes all the higher-zoom tiles"
    );
    assert_eq!(
        multi_tile_match_opts.adjust_to_zoom(5).bbox.unwrap(),
        [20, 12, 31, 19],
        "Multi-tile parent zoomed in two zoom levels includes all the higher-zoom tiles"
    );

    // Multi-parent, multi-tile bbox zoomed out
    let multi_parent_match_opts =
        MatchOpts { bbox: Some([6, 3, 8, 4]), zoom: 5, ..MatchOpts::default() };
    assert_eq!(
        multi_parent_match_opts.adjust_to_zoom(4).bbox.unwrap(),
        [3, 1, 4, 2],
        "Multi-tile parent zoomed in one zoom level includes all the higher-zoom tiles"
    );
}

// keys consist of a marker byte indicating type (regular entry, prefix cache, etc.) followed by
// a 32-bit phrase ID followed by a variable-length set of bytes for language -- everything after
// the phrase ID is assumed to be language, and it might be up to 128 bits long, but we'll strip
// leading (in a big-endian sense/most-significant sense) zero bytes for compactness
pub const MAX_KEY_LENGTH: usize = 1 + (32 / 8) + (128 / 8);

// The max number of contexts to return from Coalesce
pub const MAX_CONTEXTS: usize = 40;

#[derive(Serialize, Deserialize, Debug, PartialOrd, PartialEq, Clone)]
pub struct GridEntry {
    // these will be truncated to 4 bits apiece
    pub relev: f32,
    pub score: u8,
    pub x: u16,
    pub y: u16,
    // this will be truncated to 24 bits
    pub id: u32,
    pub source_phrase_hash: u8,
}

#[derive(Serialize, Deserialize, Debug, PartialOrd, PartialEq)]
pub struct MatchEntry {
    pub grid_entry: GridEntry,
    pub matches_language: bool,
}

#[derive(Serialize, Deserialize, Debug, PartialOrd, PartialEq, Clone)]
pub struct CoalesceEntry {
    pub grid_entry: GridEntry,
    pub matches_language: bool,
    pub idx: u16,
    pub tmp_id: u32,
    pub mask: u32,
    pub distance: f64,
    pub scoredist: f64,
}

#[derive(Serialize, Deserialize, Debug, PartialOrd, PartialEq)]
pub struct CoalesceContext {
    pub mask: u32,
    pub relev: f32,
    pub entries: Vec<CoalesceEntry>,
}

#[inline]
pub fn relev_float_to_int(relev: f32) -> u8 {
    if relev == 0.4 {
        0
    } else if relev == 0.6 {
        1
    } else if relev == 0.8 {
        2
    } else {
        3
    }
}

#[inline]
pub fn relev_int_to_float(relev: u8) -> f32 {
    match relev {
        0 => 0.4,
        1 => 0.6,
        2 => 0.8,
        _ => 1.,
    }
}
