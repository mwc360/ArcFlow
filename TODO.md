1. Support auto forEachBatch() to multiple targets (specify source stage in StageConfig, otherwise defaults to sequential)
1. ~~Support spawning next stage as availableNow via StreamQueryListener() rather than polling on all stages after the first~~ ✅ Implemented: `StageChainListener` + `event_driven_chaining` config
1. Rationalize and rename ZonePipeline and Controller (Controller == Pipeline, ZonePipeline == StagePipeline?)
1. Auto create DAG based on tables in StageConfig
1. ~~Queue to manage not calling stream availableNow multiple times (at the end of a downstream Stage, check queue and trigger again?)~~ ✅ Implemented: dedup + pending retrigger in `StageChainListener`
1. Support setting shuffle partitions based on compute size
    - would this need to be called at the start of every operation in case of scaling?