\# StreamForge — Week 3 Project Status



\## Current Status



The local Kafka and Faust development environment is running successfully on Windows.



The Faust worker starts, connects to Kafka, creates the required internal topics, and initializes the telemetry processing pipeline.



\## Completed This Week



\- Verified the local Kafka connection.

\- Started the Faust worker successfully.

\- Confirmed creation of the repartition topic.

\- Confirmed creation of the window-state changelog topic.

\- Verified the planned telemetry-processing stages:

&#x20; - filtering invalid sensor values

&#x20; - grouping readings by customer and truck

&#x20; - calculating temperature statistics

&#x20; - detecting temperature breaches

&#x20; - maintaining five-minute hopping windows



\## Current Blocker



The planned RocksDB state store cannot currently be enabled on Windows.



The native RocksDB build completed successfully, but the `faust-streaming-rocksdb` wrapper fails during package setup.



This appears to be a dependency-wrapper compatibility issue rather than a problem with the local compiler, RocksDB installation, or Kafka setup.



\## Current Workaround



The worker is temporarily configured to use an in-memory state store.



This allows local development and testing to continue, but window state will be lost whenever the worker restarts.



\## Next Steps



1\. Run the worker in Docker or WSL using Linux.

2\. Install the Linux-compatible RocksDB Faust dependency.

3\. Replace the in-memory store with RocksDB.

4\. Verify that window state survives a worker restart.



\## Status



\*\*Overall:\*\* In progress  

\*\*Primary blocker:\*\* Windows RocksDB wrapper compatibility  

\*\*Recommended next environment:\*\* Docker or WSL

