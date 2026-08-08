# Static LLM Call-Site Scan

This scan was run from the repository root after the routed gateway migration.
The scan is a release-gate inventory, not a claim that every textual `model` or
`source_label` reference is an LLM profile reference.

## Commands

```text
rg -n "llm_config\.profiles|llm_config\.resource_for_profile|config\.profiles|resource_for_profile" --glob '*.py' --glob '!utils/llm/**' --glob '!tests/**'
rg -n "LlmClient|LlmClientProtocol|LlmRequest" --glob '*.py' --glob '!utils/llm/**' --glob '!tests/**'
rg -n "config/11_llm\.json|11_llm\.json" --glob '!openspec/changes/add-weighted-llm-pool-routing/**'
rg -n "QUOTE_LLM_API_KEY" --glob '*.json' --glob '*.md' --glob '*.py' --glob '!tests/**'
```

## Classification

| Match class | Current locations | Classification | Decision |
| --- | --- | --- | --- |
| `LlmClient`, `LlmClientProtocol`, `LlmRequest` | `data_manager.py`, `data_sources/cninfo_announcement_title_llm.py`, `data_sources/cninfo_corporate_action_llm.py`, `research/business_profile_semantic_extraction.py`, `research/business_profile_llm.py`, production/live/benchmark scripts | Application callers using stable logical profiles and the public gateway protocol | Allowed and covered by business regression tests |
| `llm_config.profiles`, `resource_for_profile` | No application-layer matches outside `utils/llm` and tests; the controlled live benchmark uses `LlmConfig.controlled_stage_config()` and `describe_logical_profile()` | No remaining concrete-profile inspection in business, scheduler, API, or scripts | Release gate passes |
| `api_key_env`, concrete `base_url`/`model` fields | `utils/llm` implementation; `research/business_profile_llm.py` legacy injectable adapter defaults | Gateway-internal configuration and offline compatibility contract; production adapter construction loads the shared project config and submits `semantic_extraction` | No production source selection or independent pool bypass |
| `config/11_llm.json` | Requirements and OpenSpec migration text only | Historical migration reference | Runtime/config references use `config/13_llm.json` |
| `QUOTE_LLM_API_KEY` | Compatibility defaults in public profile/legacy adapter models, unit fixtures, and migration documentation | Backward-compatible direct-profile/fake-fixture default; not used by the new Pipio routes | New deployment profiles use `QUOTE_LLM_PIPIO_GROK_API_KEY` and `QUOTE_LLM_PIPIO_LUNA_API_KEY`; no secret values are stored |

## Evidence

- `config/13_llm.json` is the sole project JSON owner of the top-level `llm` key.
- Business modules use logical names such as `semantic_extraction` and
  `corporate_action_title_classification`; concrete names occur only in the
  `utils/llm` configuration and routing implementation, tests, and non-secret
  benchmark fixtures.
- The live validator and benchmark report source/route lineage without printing
  API keys, headers, prompts, responses, or raw provider bodies.
- Application shutdown calls the public shared-registry shutdown path after
  client transports are closed; individual client close remains non-destructive
  to registries shared by other clients.
