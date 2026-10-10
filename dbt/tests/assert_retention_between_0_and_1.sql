-- Retention is a share of the cohort: anything outside 0..1 means a join fanned out.
select cohort, months_since_signup, retention
from {{ ref('monthly_cohorts') }}
where retention < 0 or retention > 1
