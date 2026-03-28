
pub trait DataSourcer {
    // Associated item type produced by the data sourcer
    type Item: Send + 'static;

    // Start or reset the source. Return true if ready to iterate.
    fn start(&mut self) -> bool;

    // Pull the next item from the source. Returns Some(item) while data is
    // available, or None when the source is exhausted.
    fn iterate(&mut self) -> Option<Self::Item>;
}