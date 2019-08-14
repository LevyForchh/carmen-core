use std::convert::TryInto;
use std::marker::PhantomData;

pub struct Offset(usize);

pub trait EncodableWrite {
    fn write_to(&self, buffer: &mut Vec<u8>) -> ();
}

pub trait EncodableRead<'a> {
    const SIZE: usize;
    fn read_from(buffer: &'a [u8], offset: Offset) -> Self;
}

pub struct Writer {
    data: Vec<u8>
}

impl Writer {
    pub fn new() -> Self {
        Writer { data: Vec::new() }
    }

    pub fn write_scalar<T: EncodableWrite>(&mut self, s: T) -> Offset {
        let loc = self.data.len();
        s.write_to(&mut self.data);
        Offset(loc)
    }

    pub fn write_vec<T: EncodableWrite>(&mut self, s: &[T]) -> Offset {
        let loc = self.data.len();
        self.data.extend_from_slice(&(s.len() as u32).to_le_bytes());
        for item in s {
            item.write_to(&mut self.data);
        }
        Offset(loc)
    }
}

pub struct Reader {
    data: Vec<u8>
}

impl Reader {
    pub fn new(data: Vec<u8>) -> Self {
        Reader { data }
    }

    pub fn read_scalar<'a, T: EncodableRead<'a>>(&'a self, offset: Offset) -> T {
        T::read_from(&self.data, offset)
    }

    pub fn read_root<'a, T: EncodableRead<'a>>(&'a self) -> T {
        let offset = Offset(self.data.len() - T::SIZE);
        self.read_scalar(offset)
    }
}

pub struct EncodedVec<'a, T> {
    data: &'a [u8],
    start: usize,
    len: usize,
    phantom: PhantomData<&'a T>
}

impl<'a, T: EncodableRead<'a>> EncodedVec<'a, T> {
    pub fn new(data: &'a [u8], offset: Offset) -> Self {
        let start = (offset.0 + 4) as usize;
        let len = u32::from_le_bytes(data[(offset.0 as usize)..start].try_into().unwrap());
        EncodedVec { data, start, len: len.try_into().unwrap(), phantom: PhantomData }
    }

    pub fn from_pointer(data: &'a [u8], offset: Offset) -> Self {
        let ptr = u32::from_le_bytes(data[(offset.0 as usize)..(offset.0 + 4)].try_into().unwrap());
        Self::new(data, Offset(ptr as usize))
    }

    pub fn get(&self, pos: usize) -> T {
        let offset = self.start + (pos * T::SIZE);
        T::read_from(self.data, Offset(offset))
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn iter(&self) -> impl Iterator<Item = T> + '_ {
        (0..self.len).map(move |idx| self.get(idx))
    }
}

pub struct RelevScore<V> {
    relev_score: u8,
    coords: V
}

impl EncodableWrite for RelevScore<Offset> {
    fn write_to(&self, buffer: &mut Vec<u8>) {
        buffer.push(self.relev_score);
        buffer.extend_from_slice(&(self.coords.0 as u32).to_le_bytes());
    }
}

impl<'a> EncodableRead<'a> for RelevScore<EncodedVec<'a, Coord<EncodedVec<'a, u32>>>> {
    const SIZE: usize = 5;
    fn read_from(buffer: &'a [u8], offset: Offset) -> Self {
        let relev_score = buffer[offset.0];
        let coords = EncodedVec::from_pointer(buffer, Offset(offset.0 + 1));
        RelevScore { relev_score, coords }
    }
}

pub struct Coord<V> {
    coord: u32,
    ids: V
}

impl EncodableWrite for Coord<Offset> {
    fn write_to(&self, buffer: &mut Vec<u8>) {
        buffer.extend_from_slice(&(self.coord as u32).to_le_bytes());
        buffer.extend_from_slice(&(self.ids.0 as u32).to_le_bytes());
    }
}

impl<'a> EncodableRead<'a> for Coord<EncodedVec<'a, u32>> {
    const SIZE: usize = 8;
    fn read_from(buffer: &'a [u8], offset: Offset) -> Self {
        let coord = u32::from_le_bytes(buffer[(offset.0 as usize)..(offset.0 + 4)].try_into().unwrap());
        let ids = EncodedVec::<'a, u32>::from_pointer(buffer, Offset(offset.0 + 4));
        Coord { coord, ids }
    }
}

impl EncodableWrite for u32 {
    fn write_to(&self, buffer: &mut Vec<u8>) {
        buffer.extend_from_slice(&self.to_le_bytes());
    }
}

impl<'a> EncodableRead<'a> for u32 {
    const SIZE: usize = 4;
    fn read_from(buffer: &'a [u8], offset: Offset) -> Self {
        u32::from_le_bytes(buffer[(offset.0 as usize)..offset.0 + 4].try_into().unwrap())
    }
}

pub struct PhraseRecord<V> {
    relev_scores: V
}

pub fn read_phrase_record_from<'a>(reader: &'a Reader) ->
    PhraseRecord<EncodedVec<'a, RelevScore<EncodedVec<'a, Coord<EncodedVec<'a, u32>>>>>>
{
    reader.read_root()
}

impl EncodableWrite for PhraseRecord<Offset> {
    fn write_to(&self, buffer: &mut Vec<u8>) {
        buffer.extend_from_slice(&(self.relev_scores.0 as u32).to_le_bytes());
    }
}

impl<'a> EncodableRead<'a> for PhraseRecord<EncodedVec<'a, RelevScore<EncodedVec<'a, Coord<EncodedVec<'a, u32>>>>>> {
    const SIZE: usize = 4;
    fn read_from(buffer: &'a [u8], offset: Offset) -> Self {
        let relev_scores = EncodedVec::from_pointer(buffer, offset);
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
    for rs in r_reader.relev_scores.iter() {
        for coord in rs.coords.iter() {
            for id in coord.ids.iter() {
                out_grids.push(Grid { relev_score: rs.relev_score, coord: coord.coord, id })
            }
        }
    }

    let deduped_grids: Vec<_> = grids.iter().cloned().dedup().collect();
    assert_eq!(deduped_grids, out_grids);
}