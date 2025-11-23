mod aggregate;
mod filter;
mod limit;
mod projection;
mod scan;

pub use aggregate::PhysicalUngroupedAggregate;
pub use filter::PhysicalFilter;
pub use limit::PhysicalLimit;
pub use projection::PhysicalProjection;
pub use scan::PhysicalScan;

use super::data_chunk::DataChunk;

/// result of executing a physical operator
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecuteResult {
    /// operator produced output and can produce more
    NeedMoreInput,
    /// operator is finished (no more data)
    Finished,
}

/// physical operator trait (push-based execution)
/// each operator transforms input DataChunk → output DataChunk
pub trait PhysicalOperator {
    /// execute the operator on the input chunk, producing output
    ///
    /// for source operators (e.g., Scan), input is empty
    /// for processing operators (e.g., Filter, Projection), input contains data from child
    ///
    /// returns:
    /// - ExecuteResult::NeedMoreInput if more data can be produced
    /// - ExecuteResult::Finished if no more data is available
    fn execute(&mut self, input: &DataChunk, output: &mut DataChunk) -> ExecuteResult;

    /// reset the operator state (for restarting execution)
    fn reset(&mut self);
}
