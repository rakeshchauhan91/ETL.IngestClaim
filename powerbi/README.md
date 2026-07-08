# Power BI Reporting — Health Insurance Analytics

## Connecting

1. Power BI Desktop → **Get Data → PostgreSQL database**
2. Server: `<pg_host>` (local: `localhost:5432`, Azure: the Flexible Server FQDN from Bicep output `pgServerFqdn`)
3. Database: `health_dw`
4. Mode: **Import** for a small dataset like this (fast, cached); use **DirectQuery** once claim volume gets large enough that nightly refresh is too slow.
5. Import the six `gold.vw_*` views — **do not** connect directly to `gold.fact_*` / `gold.dim_*` tables from every report page; the views already encode the business logic so report authors don't reinvent it.

## Report pages (mapped to the views built by the pipeline)

| Page | Source view | Business question it answers |
|---|---|---|
| **Cost Trend & PMPM** | `vw_monthly_cost_trend` | Is our per-member cost rising or falling month over month? Are we tracking to budget? |
| **Chronic Disease Burden** | `vw_chronic_condition_cost` | Which conditions (diabetes, hypertension, CKD, etc.) drive the most spend — where should care-management investment go? |
| **Readmissions & Quality** | `vw_readmission_rate_by_facility` | Which facilities have abnormally high 30-day readmission rates — network/quality risk? |
| **High-Cost Member Concentration** | `vw_patient_cost_ranking` | What % of total spend comes from the top 5%/10% of members (classic 80/20 Pareto) — who needs case management? |
| **Enrollment Mix** | `vw_enrollment_mix` | How is our book of business shifting by plan type / age band — pricing & product implications? |
| **Denial Analysis** | `vw_denial_analysis` | Which providers/months have high denial rates — revenue leakage or coding/documentation issues? |

## Core DAX measures

```dax
Total Approved =
SUM ( vw_monthly_cost_trend[total_approved] )

PMPM =
DIVIDE ( [Total Approved], SUM ( vw_monthly_cost_trend[members_with_claims] ) )

MoM Cost Growth % =
VAR CurrentMonth = [Total Approved]
VAR PriorMonth =
    CALCULATE ( [Total Approved], DATEADD ( vw_monthly_cost_trend[month], -1, MONTH ) )
RETURN
    DIVIDE ( CurrentMonth - PriorMonth, PriorMonth )

Readmission Rate % =
DIVIDE (
    SUM ( vw_readmission_rate_by_facility[readmissions] ),
    SUM ( vw_readmission_rate_by_facility[total_encounters] )
)

Top 10pct Member Cost Share =
CALCULATE (
    SUM ( vw_patient_cost_ranking[total_approved] ),
    FILTER ( vw_patient_cost_ranking, vw_patient_cost_ranking[cumulative_cost_pct] <= 10 )
)

Denial Rate % =
DIVIDE (
    SUM ( vw_denial_analysis[denied_claims] ),
    SUM ( vw_denial_analysis[total_claims] )
)
```

## How historic data drives business decisions (what to put in the exec summary page)

- **Trend, not snapshot**: a single month's PMPM tells you little; the *12-month trend line* tells leadership whether medical cost trend is outpacing premium pricing — directly informs next renewal cycle pricing.
- **Chronic disease cost concentration**: historically, ~5 conditions typically drive 40-60% of claims spend. Once `vw_chronic_condition_cost` shows this pattern over several quarters, it justifies funding a disease-management program with a measurable before/after cost comparison.
- **Readmission trend by facility over time** separates a one-off bad quarter from a *systemic* quality problem at a facility — historic data is what turns a single data point into a network-contracting decision.
- **Pareto concentration drift**: if the top-10%-of-members cost share is *increasing* release over release, that's an early warning that risk is concentrating — informs stop-loss/reinsurance purchasing decisions.
- **Seasonality**: 2+ years of history (the pipeline seeds `gold.dim_date` for 2022-2027) lets Power BI separate real trend from seasonal noise (e.g., flu-season ER spikes) using DAX time-intelligence functions like `SAMEPERIODLASTYEAR`.

## Refresh

- **Local**: re-run `docker compose run pipeline`, then hit *Refresh* in Power BI Desktop.
- **Azure**: schedule a Power BI Service dataset refresh right after the Container Apps Job's cron run (e.g., job at 02:00 UTC, PBI refresh at 03:00 UTC), or trigger refresh via the Power BI REST API as a final step of your CI/CD pipeline.
