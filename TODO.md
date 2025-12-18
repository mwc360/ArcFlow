1. Support auto forEachBatch() to multiple targets (specify source stage in StageConfig, otherwise defaults to sequential)
1. Support spawning next stage as availableNow via StreamQueryListener() rather than polling on all stages after the first
1. Rationalize and rename ZonePipeline and Controller (Controller == Pipeline, ZonePipeline == StagePipeline?)
1. Auto create DAG based on tables in StageConfig
1. Queue to manage not calling stream availableNow multiple times (at the end of a downstream Stage, check queue and trigger again?)
1. Support query input to FlowConfig? 