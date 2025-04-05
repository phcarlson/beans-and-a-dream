
# For Milestone Due APRIL 14TH: 
### See: https://www.mongodb.com/docs/atlas/atlas-search/create-index/ after selecting Python as the language
### Also see how it differs from the regular MongoDB querying: https://www.mongodb.com/resources/basics/databases/database-search


### MUST HAVE: Implement Preprocessing For Conventional Structure 
- [ ] After filtering out the recipes that have matching ingredient parts/quantities lengths, it reduced to around 112k from like 520k or whatever, See if there is anything we can do to still include the other 400k or so, or if we want to augment some data to increase our count, or ask the prof if 112k is still enough. Do you think that we should still try to include the filtered out ones, by say, matching the subset of the arrays that are of equal length even if the amounts are innacurate? Discuss this.  

### MUST HAVE: Implement Preprocessing For Non-Conventional Structure 
- [ ] Implement the "less conventional" document structure in row_to_document_structure_2.py to produce documents to put into the database that are in a different structure from simply inserting the recipes as is. For our final proj paper we will compare the retrieval times between these 2. See row_to_document_structure_1.py for guidance, and the specific in-line TODOs. You'll basically be rearranging the cols of the dataframe by moving the data around and renaming stuff using https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html and UDFs (but avoid when possible)

### MUST HAVE: Preprocessing Analytics, for both Conventional and Non-Conventional Structure:
- [ ] Write the code to measure how reordering operations (that do equivalent things) impacts the preprocessing time taken, on average (this will possibly differ between the two preprocessing scripts) Track these times, say over 10 runs, and then make/save a histogram for our milestone report.
- [ ] Write the code to measure how the preprocessing time changes as the number of workers changes.
- [ ] Write the code to measure how the preprocessing time changes as the number of partitions changes.
- [ ] Write the code to measure how the preprocessing time changes as the number of recipes increases
- [ ] Based on the above tests, report/note the best performing settings, and then possibly see how it changes in combination with one another or not since they are not independent of one anohter. Shrug shrug
- [ ] Finally, report a summary when applied to the whole dataset

### PROB WANT: Retrieval With Atlas Search
- [ ] Create the backend Atlas search interface for various searches the user would make
- [ ] Write the code to automate trying out multiple different SEARCH indexes and gauge which is most efficient: https://www.mongodb.com/docs/atlas/atlas-search/define-field-mappings/

### PROB WANT: Retrieval with normal MongoDB
- [ ] Create the backend normal query interface for various searches the user would make
- [ ] Write the code to automate trying out multiple different regular indexes and gauge which is most efficient. 

### COULD SAVE FOR FINAL: Comparing Retrievals with Atlas Search VS normal MongoDB
- [ ] Create test cases of user queries, categorized from simple to complex
- [ ] Write the code to automate comparing retrieval times between these two systems, with plots when applicable

### COULD SAVE FOR FINAL: As Users Scale
- [ ] Caching techniques like caching common queries, possibly using Atlas analytics. IDK. 
- [ ] Play with multithreading for...
  - [ ] multiple users to query at once
  - [ ] displaying contents like requesting the images from the urls

### COULD SAVE FOR FINAL: Better Search
- [ ] Handling typos, fuzzy search, synonyms 
  - [ ] Experiment with impact on performance

# For Final Due May 5TH: 

## Interactivity: 

### MUST HAVE: UI Or Anything
- [ ] Construct simple user interface for the searches, to demonstrate in our video presentation

## Benchmarks/Analytics: 
### MUST HAVE: Query response time investigations:
- [ ] A line graph of query length vs response
time to see how the system handles sim-
ple to complex queries
- [ ] A line graph of query length vs response
time to see how much latency query ex-
pansion (done through Atlas search itself) adds
- [ ]  A line graph of query response times,
over time, with and without caching
- [ ]  Two bar charts to compare different
query types (exact matching, range
search, more complex joins) to average
response time before and after clustered
indices– or something similar.

### Profiling: 
- [ ] List the parts of the system that
take the most time: use diagrams, perhaps
with arrows weighted by average time, and
commentary on why we think so.

### Scalability: 
- [ ] Show how our system scales as more
recipes are incrementally included, with
a line graph of number of recipes vs av-
erage query time– or a line graph of the
number of queries the system can pro-
cess per unit of time as the number of
recipes grows AKA throughput
• Produce a line graph showing number of
users vs average query time or a simi-
lar throughput line graph of number of
queries it can processes per unit of time
as the number of users grows (after sim-
ulating multiple user requests at once
through an existing tool for load-testing
like Locust)

## FOR THE REPORT: 
- [ ] Compile the results as requested
- [ ] Analyze what went right or wrong, tradeoffs
- [ ] Finalize README
- [ ] Record the vid

## IF TIME: 

### Semantic Search
- [ ] Vector embeddings if free. IDFK. You read about it haha.
