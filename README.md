The following caveats do apply to this code base:

* Only stock configs were used for Kafka and Redis (apart fom a few network tweaks).
It's by no means production-ready. Given the fact that the Redis benchmark only uses a simplified stream class and the Python  language bindings are nowhere complete, do not try this at home! :-)
* I used Confluent's Python client as this apparently is the fastest one on the market if current lore is anything to by. 
* Andy's Python client has been pursuaded to understand Streams. As Salvatore's code is *not* GA yet (and I *do* expect a few changes on the server side), only a subset of the functionality has been implemented to make the benchmark work. Especially the Consumer Groups functionality is missing from the client implementation. Once the server-side code has been stabilized, I will complete the client side implementation alongside the test harnishes needed to Q/A this codebase and submit a corresponding PR.
* This is for fun only, and *not* for profit!
