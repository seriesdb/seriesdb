use crate::updates::Updates;
use rocksdb::DBWALIterator;

pub struct UpdateIterator {
    inner: DBWALIterator,
}

impl Iterator for UpdateIterator {
    type Item = Updates;
    fn next(&mut self) -> Option<Self::Item> {
        let result = self.inner.next();
        if result.is_none() {
            None
        } else {
            let (sn, batch) = result.unwrap();
            let mut updates = Updates::new();
            updates.sn = sn;
            batch.iterate(&mut updates);
            Some(updates)
        }
    }
}

impl UpdateIterator {
    pub fn new(inner: DBWALIterator) -> Self {
        UpdateIterator { inner }
    }
}
