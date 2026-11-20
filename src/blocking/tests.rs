use alloc::string::String;
use core::num::NonZeroU64;

use super::{Config, Reason, Refused, Service};
use crate::capacity::Budget;
use crate::table::tests::allocations::each_refusal;

/// Each refusal of the allocator gives `Allocation` with the job, and keeps no charge and no
/// memory. A submission that boxes the job after the job moves aborts the process, and fails.
#[test]
fn a_submission_that_the_allocator_refuses_gives_the_job_back() {
    let budget = Budget::new(NonZeroU64::new(1 << 22).unwrap());
    let service = Service::new(&budget, Config::new().threads(1).jobs(1).stack(256 << 10)).unwrap();
    let submitter = service.submitter();
    let before = budget.available();
    let refusals = each_refusal(|| {
        submitter
            .submit(|| String::from("done"))
            .map_err(|refused| (refused, budget.available()))
    });
    assert!(!refusals.is_empty());
    for ((Refused { reason, job }, available), held) in refusals {
        assert_eq!((reason, available, held), (Reason::Allocation, before, 0));
        assert_eq!(job(), "done");
    }
    service.teardown();
}
