# Business-profile offline evaluation set v1

These fixtures are local regression inputs, not a production review queue. Each
file contains a bounded annual-report section excerpt and the expected semantic
labels used by deterministic validators and mocked LLM responses. Tests must not
download reports or call a live provider when using this directory.

The initial corpus intentionally covers manufacturing, energy, pharmaceuticals,
finance, consumer, mining, and diversified groups. Labels cover the current
business-profile acceptance contract rather than every sentence in a report.
