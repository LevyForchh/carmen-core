use std::convert::TryInto;
use std::marker::PhantomData;

#[derive(Copy, Clone)]
pub struct ScalarOffset<T: Encodable> {
    addr: usize,
    phantom: PhantomData<T>
}

impl<T: Encodable> ScalarOffset<T> {
    fn new(addr: usize) -> Self {
        Self { addr, phantom: PhantomData }
    }
}

#[derive(Copy, Clone)]
pub struct VecOffset<T: Encodable> {
    addr: usize,
    phantom: PhantomData<T>
}

impl<T: Encodable> VecOffset<T> {
    fn new(addr: usize) -> Self {
        Self { addr, phantom: PhantomData }
    }

    fn from_pointer(data: &[u8], offset: usize) -> Self {
        let ptr = u32::from_le_bytes(data[offset..(offset + 4)].try_into().unwrap());
        Self::new(ptr as usize)
    }
}


pub trait Encodable: Sized {
    const SIZE: usize;
    fn write_to(&self, buffer: &mut Vec<u8>) -> ();
    fn read_from(buffer: &[u8], offset: ScalarOffset<Self>) -> Self;
}

pub struct Writer {
    data: Vec<u8>
}

impl Writer {
    pub fn new() -> Self {
        Writer { data: Vec::new() }
    }

    pub fn write_scalar<T: Encodable>(&mut self, s: T) -> ScalarOffset<T> {
        let loc = self.data.len();
        s.write_to(&mut self.data);
        ScalarOffset::new(loc)
    }

    pub fn write_vec<T: Encodable>(&mut self, s: &[T]) -> VecOffset<T> {
        let loc = self.data.len();
        self.data.extend_from_slice(&(s.len() as u32).to_le_bytes());
        for item in s {
            item.write_to(&mut self.data);
        }
        VecOffset::new(loc)
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

    pub fn read_scalar<'a, T: Encodable>(&'a self, offset: ScalarOffset<T>) -> T {
        T::read_from(self.data.as_ref(), offset)
    }

    pub fn read_vec<'a, T: Encodable>(&'a self, offset: VecOffset<T>) -> EncodedVec<'a, T> {
        EncodedVec::new(self.data.as_ref(), offset)
    }

    pub fn read_root<'a, T: Encodable>(&'a self) -> T {
        let offset = ScalarOffset::new(self.data.as_ref().len() - T::SIZE);
        self.read_scalar(offset)
    }
}

pub fn read_vec_raw<'a, T: Encodable>(buffer: &'a [u8], offset: VecOffset<T>) -> EncodedVec<'a, T> {
    EncodedVec::new(buffer, offset)
}

#[derive(Copy, Clone)]
pub struct EncodedVec<'a, T> {
    data: &'a [u8],
    start: usize,
    len: usize,
    phantom: PhantomData<&'a T>
}

impl<'a, T: Encodable> EncodedVec<'a, T> {
    pub fn new(data: &'a [u8], offset: VecOffset<T>) -> Self {
        let start = (offset.addr + 4) as usize;
        let len = u32::from_le_bytes(data[(offset.addr as usize)..start].try_into().unwrap());
        EncodedVec { data, start, len: len.try_into().unwrap(), phantom: PhantomData }
    }

    pub fn get(&self, pos: usize) -> T {
        let offset = self.start + (pos * T::SIZE);
        T::read_from(self.data, ScalarOffset::new(offset))
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

pub struct RelevScore {
    pub relev_score: u8,
    pub coords: VecOffset<Coord>
}

impl Encodable for RelevScore {
    const SIZE: usize = 5;

    fn write_to(&self, buffer: &mut Vec<u8>) {
        buffer.push(self.relev_score);
        buffer.extend_from_slice(&(self.coords.addr as u32).to_le_bytes());
    }

    fn read_from(buffer: &[u8], offset: ScalarOffset<Self>) -> Self {
        let relev_score = buffer[offset.addr];
        let coords = VecOffset::from_pointer(buffer, offset.addr + 1);
        RelevScore { relev_score, coords }
    }
}

#[derive(Copy, Clone)]
pub struct Coord {
    pub coord: u32,
    pub ids: VecOffset<u32>
}

impl Encodable for Coord {
    const SIZE: usize = 8;
    fn write_to(&self, buffer: &mut Vec<u8>) {
        buffer.extend_from_slice(&(self.coord as u32).to_le_bytes());
        buffer.extend_from_slice(&(self.ids.addr as u32).to_le_bytes());
    }
    fn read_from(buffer: &[u8], offset: ScalarOffset<Self>) -> Self {
        let coord = u32::from_le_bytes(buffer[offset.addr..(offset.addr + 4)].try_into().unwrap());
        let ids = VecOffset::<u32>::from_pointer(buffer, offset.addr + 4);
        Coord { coord, ids }
    }
}

impl Encodable for u32 {
    const SIZE: usize = 4;
    fn write_to(&self, buffer: &mut Vec<u8>) {
        buffer.extend_from_slice(&self.to_le_bytes());
    }

    fn read_from(buffer: &[u8], offset: ScalarOffset<Self>) -> Self {
        u32::from_le_bytes(buffer[(offset.addr as usize)..offset.addr + 4].try_into().unwrap())
    }
}

pub struct PhraseRecord {
    pub relev_scores: VecOffset<RelevScore>
}

pub fn read_phrase_record_from<U: AsRef<[u8]>>(reader: &Reader<U>) -> PhraseRecord {
    reader.read_root()
}

impl Encodable for PhraseRecord {
    const SIZE: usize = 4;

    fn write_to(&self, buffer: &mut Vec<u8>) {
        buffer.extend_from_slice(&(self.relev_scores.addr as u32).to_le_bytes());
    }

    fn read_from(buffer: &[u8], offset: ScalarOffset<Self>) -> Self {
        let relev_scores = VecOffset::from_pointer(buffer, offset.addr);
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
            let w_ids = writer.write_vec(&ids);
            coords.push(Coord { coord, ids: w_ids });
        }
        let w_coords = writer.write_vec(&coords);
        rses.push(RelevScore { relev_score, coords: w_coords });
    }
    let w_rses = writer.write_vec(&rses);

    let record = PhraseRecord { relev_scores: w_rses };
    writer.write_scalar(record);

    let reader = Reader::new(writer.data);
    let r_reader = read_phrase_record_from(&reader);

    let mut out_grids = Vec::new();
    for rs in reader.read_vec(r_reader.relev_scores).iter() {
        for coord in reader.read_vec(rs.coords).iter() {
            for id in reader.read_vec(coord.ids).iter() {
                out_grids.push(Grid { relev_score: rs.relev_score, coord: coord.coord, id })
            }
        }
    }

    let deduped_grids: Vec<_> = grids.iter().cloned().dedup().collect();
    assert_eq!(deduped_grids, out_grids);
}