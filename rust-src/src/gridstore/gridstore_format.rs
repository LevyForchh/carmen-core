use std::convert::TryInto;
use std::marker::PhantomData;

use integer_encoding::VarInt;

#[derive(Copy, Clone)]
pub struct ScalarOffset<T: VarEncodable> {
    addr: usize,
    phantom: PhantomData<T>
}

impl<T: VarEncodable> ScalarOffset<T> {
    fn new(addr: usize) -> Self {
        Self { addr, phantom: PhantomData }
    }
}

#[derive(Copy, Clone)]
pub struct FixedVecOffset<T: VarEncodable> {
    addr: usize,
    phantom: PhantomData<T>
}

impl<T: FixedEncodable> FixedVecOffset<T> {
    fn new(addr: usize) -> Self {
        Self { addr, phantom: PhantomData }
    }

    fn from_fixed_pointer(data: &[u8], offset: usize) -> Self {
        let ptr = u32::from_le_bytes(data[offset..(offset + 4)].try_into().unwrap());
        Self::new(ptr as usize)
    }

    fn from_var_pointer(data: &[u8], offset: usize) -> (Self, usize) {
        let (ptr, len_len) = u32::decode_var(&data[offset..]);
        (Self::new(ptr as usize), len_len)
    }
}

#[derive(Copy, Clone)]
pub struct VarVecOffset<T: VarEncodable> {
    addr: usize,
    phantom: PhantomData<T>
}

impl<T: VarEncodable> VarVecOffset<T> {
    fn new(addr: usize) -> Self {
        Self { addr, phantom: PhantomData }
    }

    fn from_fixed_pointer(data: &[u8], offset: usize) -> Self {
        let ptr = u32::from_le_bytes(data[offset..(offset + 4)].try_into().unwrap());
        Self::new(ptr as usize)
    }

    fn from_var_pointer(data: &[u8], offset: usize) -> (Self, usize) {
        let (ptr, len_len) = u32::decode_var(&data[offset..]);
        (Self::new(ptr as usize), len_len)
    }
}


pub trait VarEncodable: Sized {
    fn write_to(&self, buffer: &mut Vec<u8>) -> usize;
    fn read_from(buffer: &[u8], offset: ScalarOffset<Self>) -> (Self, usize);
}

pub trait FixedEncodable: Sized {
    const SIZE: usize;
    fn write_fixed_to(&self, buffer: &mut Vec<u8>) -> ();
    fn read_fixed_from(buffer: &[u8], offset: ScalarOffset<Self>) -> Self;
}

impl<T> VarEncodable for T where T: FixedEncodable {
    fn write_to(&self, buffer: &mut Vec<u8>) -> usize {
        self.write_fixed_to(buffer);
        T::SIZE
    }
    fn read_from(buffer: &[u8], offset: ScalarOffset<T>) -> (T, usize) {
        (T::read_fixed_from(buffer, offset), T::SIZE)
    }
}

pub struct Writer {
    data: Vec<u8>
}

impl Writer {
    pub fn new() -> Self {
        Writer { data: Vec::new() }
    }

    pub fn write_scalar<T: VarEncodable>(&mut self, s: T) -> ScalarOffset<T> {
        let loc = self.data.len();
        s.write_to(&mut self.data);
        ScalarOffset::new(loc)
    }

    pub fn write_fixed_vec<T: FixedEncodable>(&mut self, s: &[T]) -> FixedVecOffset<T> {
        let loc = self.data.len();
        let mut len_buf = [0u8; 8];
        let len_len = (s.len() as u32).encode_var(&mut len_buf);
        self.data.extend_from_slice(&len_buf[..len_len]);
        for item in s {
            item.write_fixed_to(&mut self.data);
        }
        FixedVecOffset::new(loc)
    }

    pub fn write_var_vec<T: VarEncodable>(&mut self, s: &[T]) -> VarVecOffset<T> {
        let loc = self.data.len();
        let mut len_buf = [0u8; 8];
        let len_len = (s.len() as u32).encode_var(&mut len_buf);
        self.data.extend_from_slice(&len_buf[..len_len]);
        for item in s {
            item.write_to(&mut self.data);
        }
        VarVecOffset::new(loc)
    }

    pub fn finish(self) -> Vec<u8> {
        self.data
    }
}

pub struct Reader<U: AsRef<[u8]>> {
    data: U
}

impl<U: AsRef<[u8]>> Reader<U> {
    pub fn new(data: U) -> Self {
        Reader { data }
    }

    pub fn read_fixed_scalar<'a, T: FixedEncodable>(&'a self, offset: ScalarOffset<T>) -> T {
        T::read_fixed_from(self.data.as_ref(), offset)
    }

    pub fn read_var_scalar<'a, T: VarEncodable>(&'a self, offset: ScalarOffset<T>) -> (T, usize) {
        T::read_from(self.data.as_ref(), offset)
    }

    pub fn read_fixed_vec<'a, T: FixedEncodable>(&'a self, offset: FixedVecOffset<T>) -> FixedVec<'a, T> {
        FixedVec::new(self.data.as_ref(), offset)
    }

    pub fn read_var_vec<'a, T: VarEncodable>(&'a self, offset: VarVecOffset<T>) -> VarVec<'a, T> {
        VarVec::new(self.data.as_ref(), offset)
    }

    pub fn read_root<'a, T: FixedEncodable>(&'a self) -> T {
        let offset = ScalarOffset::new(self.data.as_ref().len() - T::SIZE);
        self.read_fixed_scalar(offset)
    }
}

pub fn read_fixed_vec_raw<'a, T: FixedEncodable>(buffer: &'a [u8], offset: FixedVecOffset<T>) -> FixedVec<'a, T> {
    FixedVec::new(buffer, offset)
}

pub fn read_var_vec_raw<'a, T: VarEncodable>(buffer: &'a [u8], offset: VarVecOffset<T>) -> VarVec<'a, T> {
    VarVec::new(buffer, offset)
}

#[derive(Copy, Clone)]
pub struct FixedVec<'a, T> {
    data: &'a [u8],
    start: usize,
    len: usize,
    phantom: PhantomData<&'a T>
}

impl<'a, T: FixedEncodable> FixedVec<'a, T> {
    pub fn new(data: &'a [u8], offset: FixedVecOffset<T>) -> Self {
        let (len, len_len) = u32::decode_var(&data[(offset.addr as usize)..]);
        let start = (offset.addr + len_len) as usize;
        FixedVec { data, start, len: len.try_into().unwrap(), phantom: PhantomData }
    }

    pub fn get(&self, pos: usize) -> T {
        let offset = self.start + (pos * T::SIZE);
        T::read_fixed_from(self.data, ScalarOffset::new(offset))
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn iter(&self) -> impl Iterator<Item = T> + '_ {
        (0..self.len).map(move |idx| self.get(idx))
    }

    pub fn into_iter(self) -> impl Iterator<Item = T> + 'a {
        (0..self.len).map(move |idx| self.get(idx))
    }
}

#[derive(Copy, Clone)]
pub struct VarVec<'a, T> {
    data: &'a [u8],
    start: usize,
    len: usize,
    phantom: PhantomData<&'a T>
}

impl<'a, T: VarEncodable> VarVec<'a, T> {
    pub fn new(data: &'a [u8], offset: VarVecOffset<T>) -> Self {
        let (len, len_len) = u32::decode_var(&data[(offset.addr as usize)..]);
        let start = (offset.addr + len_len) as usize;
        VarVec { data, start, len: len.try_into().unwrap(), phantom: PhantomData }
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn iter(&self) -> impl Iterator<Item = T> + '_ {
        let mut loc: usize = self.start;
        let mut i: usize = 0;
        std::iter::from_fn(move || {
            if i < self.len {
                let (val, incr) = T::read_from(self.data, ScalarOffset::new(loc));
                i += 1;
                loc += incr;
                Some(val)
            } else {
                None
            }
        })
    }

    pub fn into_iter(self) -> impl Iterator<Item = T> + 'a {
        let mut loc: usize = self.start;
        let mut i: usize = 0;
        std::iter::from_fn(move || {
            if i < self.len {
                let (val, incr) = T::read_from(self.data, ScalarOffset::new(loc));
                i += 1;
                loc += incr;
                Some(val)
            } else {
                None
            }
        })
    }
}

pub struct RelevScore {
    pub relev_score: u8,
    pub coords: FixedVecOffset<Coord>
}

impl VarEncodable for RelevScore {
    fn write_to(&self, buffer: &mut Vec<u8>) -> usize {
        buffer.push(self.relev_score);
        let mut addr_buf = [0u8; 8];
        let addr_len = (self.coords.addr as u32).encode_var(&mut addr_buf);
        buffer.extend_from_slice(&addr_buf[..addr_len]);
        1 + addr_len
    }

    fn read_from(buffer: &[u8], offset: ScalarOffset<Self>) -> (Self, usize) {
        let relev_score = buffer[offset.addr];
        let (coords, addr_len) = FixedVecOffset::from_var_pointer(buffer, offset.addr + 1);
        (RelevScore { relev_score, coords }, 1 + addr_len)
    }
}

#[derive(Copy, Clone)]
pub struct Coord {
    pub coord: u32,
    pub ids: VarVecOffset<VarU32>
}

impl FixedEncodable for Coord {
    const SIZE: usize = 8;
    fn write_fixed_to(&self, buffer: &mut Vec<u8>) {
        buffer.extend_from_slice(&(self.coord as u32).to_le_bytes());
        buffer.extend_from_slice(&(self.ids.addr as u32).to_le_bytes());
    }
    fn read_fixed_from(buffer: &[u8], offset: ScalarOffset<Self>) -> Self {
        let coord = u32::from_le_bytes(buffer[offset.addr..(offset.addr + 4)].try_into().unwrap());
        let ids = VarVecOffset::<VarU32>::from_fixed_pointer(buffer, offset.addr + 4);
        Coord { coord, ids }
    }
}

impl FixedEncodable for u32 {
    const SIZE: usize = 4;
    fn write_fixed_to(&self, buffer: &mut Vec<u8>) {
        buffer.extend_from_slice(&self.to_le_bytes());
    }

    fn read_fixed_from(buffer: &[u8], offset: ScalarOffset<Self>) -> Self {
        u32::from_le_bytes(buffer[(offset.addr as usize)..offset.addr + 4].try_into().unwrap())
    }
}

#[derive(Copy, Clone)]
pub struct VarU32(u32);

impl VarEncodable for VarU32 {
    fn write_to(&self, buffer: &mut Vec<u8>) -> usize {
        let mut buf = [0u8; 8];
        let len = self.0.encode_var(&mut buf);
        buffer.extend_from_slice(&buf[..len]);
        len
    }

    fn read_from(buffer: &[u8], offset: ScalarOffset<Self>) -> (Self, usize) {
        let (val, len) = u32::decode_var(&buffer[offset.addr..]);
        (VarU32(val), len)
    }
}

impl<'a> VarVec<'a, VarU32> {
    pub fn delta_write_slice_to(s: &[u32], wtr: &mut Writer) -> VarVecOffset<VarU32> {
        let loc = wtr.data.len();
        let mut len_buf = [0u8; 8];
        let len_len = (s.len() as u32).encode_var(&mut len_buf);
        wtr.data.extend_from_slice(&len_buf[..len_len]);
        let mut prev: u32 = 0;
        for (i, item) in s.iter().enumerate() {
            if i == 0 {
                VarU32(*item).write_to(&mut wtr.data);
            } else {
                VarU32(prev - *item).write_to(&mut wtr.data);
            }
            prev = *item;
        }
        VarVecOffset::new(loc)
    }

    pub fn delta_iter(&self) -> impl Iterator<Item = u32> + '_ {
        let mut first: bool = true;
        let mut prev: u32 = 0;
        self.iter().map(move |item| {
            let out = if first {
                first = false;
                item.0
            } else {
                prev - item.0
            };
            prev = item.0;
            out
        })
    }

    pub fn into_delta_iter(self) -> impl Iterator<Item = u32> + 'a {
        let mut first: bool = true;
        let mut prev: u32 = 0;
        self.into_iter().map(move |item| {
            let out = if first {
                first = false;
                item.0
            } else {
                prev - item.0
            };
            prev = item.0;
            out
        })
    }
}

pub struct PhraseRecord {
    pub relev_scores: VarVecOffset<RelevScore>
}

pub fn read_phrase_record_from<U: AsRef<[u8]>>(reader: &Reader<U>) -> PhraseRecord {
    reader.read_root()
}

impl FixedEncodable for PhraseRecord {
    const SIZE: usize = 4;

    fn write_fixed_to(&self, buffer: &mut Vec<u8>) {
        buffer.extend_from_slice(&(self.relev_scores.addr as u32).to_le_bytes());
    }

    fn read_fixed_from(buffer: &[u8], offset: ScalarOffset<Self>) -> Self {
        let relev_scores = VarVecOffset::from_fixed_pointer(buffer, offset.addr);
        PhraseRecord { relev_scores }
    }
}

#[cfg(test)]
use itertools::Itertools;

#[test]
fn test_write() {
    #[derive(Debug, PartialOrd, PartialEq, Ord, Eq, Clone)]
    struct Grid {
        relev_score: u8,
        coord: u32,
        id: u32
    }

    let mut grids = vec![
        Grid { relev_score: 50, coord: 421, id: 1235 },
        Grid { relev_score: 50, coord: 421, id: 1239 },
        Grid { relev_score: 50, coord: 842, id: 12 },
        Grid { relev_score: 100, coord: 42, id: 1235 },
        Grid { relev_score: 210, coord: 15, id: 12835 },
        Grid { relev_score: 250, coord: 842, id: 12 },
        Grid { relev_score: 106, coord: 2, id: 8 },
        Grid { relev_score: 210, coord: 15, id: 636 },
        Grid { relev_score: 250, coord: 8420, id: 1 },
        Grid { relev_score: 106, coord: 2, id: 8 },
    ];

    grids.sort_by(|a, b| b.cmp(&a));

    let mut writer = Writer::new();

    let mut rses = Vec::new();
    for (relev_score, rs_group) in &(&grids).into_iter().group_by(|g| g.relev_score) {
        let mut coords = Vec::new();
        for (coord, coord_group) in &rs_group.into_iter().group_by(|g| g.coord) {
            let ids: Vec<_> = coord_group.into_iter().map(|g| g.id).dedup().collect();
            let w_ids = VarVec::<VarU32>::delta_write_slice_to(&ids, &mut writer);
            coords.push(Coord { coord, ids: w_ids });
        }
        let w_coords = writer.write_fixed_vec(&coords);
        rses.push(RelevScore { relev_score, coords: w_coords });
    }
    let w_rses = writer.write_var_vec(&rses);

    let record = PhraseRecord { relev_scores: w_rses };
    writer.write_scalar(record);

    let reader = Reader::new(writer.data);
    let r_reader = read_phrase_record_from(&reader);

    let mut out_grids = Vec::new();
    for rs in reader.read_var_vec(r_reader.relev_scores).iter() {
        for coord in reader.read_fixed_vec(rs.coords).iter() {
            for id in reader.read_var_vec(coord.ids).delta_iter() {
                out_grids.push(Grid { relev_score: rs.relev_score, coord: coord.coord, id })
            }
        }
    }

    let deduped_grids: Vec<_> = grids.iter().cloned().dedup().collect();
    assert_eq!(deduped_grids, out_grids);
}