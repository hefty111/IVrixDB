###### Copyright Notice
<p>
IVrixDB ©Ivri Faitelson 2020<br>
IVrixDB was developed by Ivri Faitelson and is a proprietary software product owned by Ivri Faitelson.<br>
All rights reserved.
</p>


# IVrixDB 
IVrixDB is a schema-less search application for time-series data at scale.
It is built upon Solr, an enterprise search platform written on top of Lucene,
which is a high-performance, full featured text search engine library.
IVrixDB's main feature is Search-Time Field Extraction, where it can run search and analytics
on Solr Fields that do not exist in the schema.

IVrixDB provides:
- A schema-less time-series index
- Time-based data retention
- Search-Time Field Extraction
- full-text search and analytics
- cloud support
- fault-tolerant architecture
- hardware resource management

IVrixDB is at the stage of an advanced proof-of-concept, and all the
features above have been successfully implemented with scale in mind.

Even though the code is embedded within Solr (for development ease), IVrixDB is an extension of Solr.
It is built upon Solr’s extensible plug-in architecture. Solr's existing functionality was not impacted,
and there are no degradation in performance or scalability compared to Solr.

The code can be located at package "org.apache.solr.ivrixdb" (directory path "IVrixDB/solr/core/src/java/org/apache/solr/ivrixdb/"),
and the resources containing IVrixDB documents, tools, configurations, and more can be found in the "IVrixDB Resources" folder.

To further understand IVrixDB's features, please read the "Key Features In-Depth Breakdown" document.

To further understand the current architecture and its guiding principles, please read the "IVrixDB Architecture" document.

To know what was not fully implemented and what are the current issues in the project,
please read the "Disclaimers, Warnings, and Bugs" document.

To start using this application, please read the "Quick Start" document.

To begin development, please read the documents in the order presented:
"IVrixDB Architecture", "Disclaimers, Warnings, and Bugs", "Quick Start", and "IVrixDB Tests".

To contact me, please email "ivrift@icloud.com".

## P.S.
I apologize if any part of the documentation or code is unclear. College was creeping up on me, and
I was feeling rushed. However, I managed to build a stable and advanced POC that can create and search
IVrixDB Indexes in the production environment of SolrCloud, and to fully write documentation and JavaDoc.
Additionally, I worked very hard to keep the code clean and of high-quality, though some parts of the code do not
live up to my standards. Over time, I will clean up both code and documentation. Please feel free to email me at any time.