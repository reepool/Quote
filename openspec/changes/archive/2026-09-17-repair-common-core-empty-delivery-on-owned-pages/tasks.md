## 1. Deterministic facts from owned excerpts

- [x] 1.1 Add a common-core projector that turns an owned overview/segment excerpt into BusinessOverview, Activity, Segment, or Measurement only when the excerpt already states that field
- [x] 1.2 Feed those records as deterministic_candidates and stop emitting provider-unavailable / required_coverage_missing for fields the excerpt already satisfies
- [x] 1.3 Add fixture tests from the 302132.SZ official p11–p14 excerpts proving accepted principal_business, products_services, and aviation-manufacturing revenue facts with provider=None

## 2. Bank heading and 经营范围 same-line field value

- [x] 2.1 Own standalone titles 公司主要业务情况 and 公司金融业务 after numbering/section prefixes, while keeping TOC and dotted page-number lines unowned
- [x] 2.2 Own a 公司简介 line whose label is 经营范围 and whose same-line remainder is the business-scope value, using the official 600000.SH p22 line 经营范围 银行业务；证券投资基金托管；公募证券投资基金销售；经批准的其它业务。; do not require a standalone title or punctuation-only remainder
- [x] 2.3 Add fixture tests proving 3.6 公司主要业务情况 locates overview, the p22 经营范围 line owns 银行业务, incidental mid-sentence 经营范围 stays unowned, and chapter_missing is not recorded for those official pages

## 3. Controlled successor replay and authoritative query

- [x] 3.1 Publish a processing identity distinct from {"rules": "company_profile_common_core.v1"} on the unique company_profile_common_core owner
- [x] 3.2 Make enqueue insert successor work for 302132.SZ and 600000.SH instead of reusing the completed empty items; refuse to reuse a predecessor scope receipt that has no accepted records; do not delete the database or published JSON as the replay method
- [x] 3.3 Make query select the current-identity successor for the same report_id and document_version; add a fixture where the empty predecessor work_id sorts after the successor and query still returns the successor accepted facts
- [x] 3.4 Re-run the published path on only those two companies, still not_authorized, and assert query returns accepted facts that the empty deliveries lacked
- [x] 3.5 Independently re-read the same official reports, record source review, and stop if recall stays 0 or accuracy stays unassessed because query still returns an empty delivery
- [x] 3.6 Leave sw_l1 finance classification and M4 out of this change

## 4. A-class owned-page projection repair

- [x] 4.1 Project 经营范围 only as a field-label + same-line issuer value; official 中航 p11 经营范围内从事军品出口 must not become an issuer Activity
- [x] 4.2 Join PDF soft wraps before listing objects or segment rows; official 航空防务/装备 and 离岸业务等多个领域 must not keep newline fragments
- [x] 4.3 Parse official 中航 p14 wrapped 航空制造业 amount/share with 单位：元; do not invent 元 when the excerpt has no unit declaration
- [x] 4.4 Publish owned_page_facts=v2 successor identity; do not reuse, overwrite, or delete v1 completed work
- [x] 4.5 Re-run 302132.SZ and 600000.SH on v2, record source review, keep M4 and production closed

## 5. Per-row segment dimension and recall correction

- [x] 5.1 Bind each official 中航 p14 row to its nearest 分行业/分产品/分地区/分销售模式 heading; 直销 must not support products_services
- [x] 5.2 Do not treat 直销 75,358,958,001.86 as delivered 营业收入合计; keep 合计 rows unpublished unless the excerpt states that label as the measured object
- [x] 5.3 Publish owned_page_facts=v3 successor; do not reuse, overwrite, or delete v2; re-run both companies and record source review as 4/7 if 营业收入合计 is still unpublished
