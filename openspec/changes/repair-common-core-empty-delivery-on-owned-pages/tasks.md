## 1. Deterministic facts from owned excerpts

- [ ] 1.1 Add a common-core projector that turns an owned overview/segment excerpt into BusinessOverview, Activity, Segment, or Measurement only when the excerpt already states that field
- [ ] 1.2 Feed those records as deterministic_candidates and stop emitting provider-unavailable / required_coverage_missing for fields the excerpt already satisfies
- [ ] 1.3 Add fixture tests from the 302132.SZ official p11–p14 excerpts proving accepted principal_business, products_services, and aviation-manufacturing revenue facts with provider=None

## 2. Bank and service heading ownership

- [ ] 2.1 Own 公司主要业务情况, 公司金融业务, and 经营范围 as business-overview titles after numbering/section prefixes, while keeping TOC and dotted page-number lines unowned
- [ ] 2.2 Add fixture tests from the 600000.SH official 经营范围 and 3.6 公司主要业务情况 pages proving extract_business_overview locates the section and does not record chapter_missing

## 3. Same-sample re-observation

- [ ] 3.1 Re-run the published company_profile_common_core path on 302132.SZ and 600000.SH only, still not_authorized and without enabling DCF, trading, or old writers
- [ ] 3.2 Independently re-read the same official reports, record source review, and stop if recall stays 0 or accuracy stays unassessed because delivery is still empty
- [ ] 3.3 Leave sw_l1 finance classification and M4 out of this change
