Overview
This project demanded the creation of a data warehouse through a real-time stream. To implement this functionality I first joined my Master Data in main to build an extensive queue that would be joined to my data from the stream. 
The stream was implemented to load 5 rows at a time and add 5 chunks into a queue, with time gaps in between to ensure the emulation of a stream.This was the first thread. 
In the second thread, a mesh join was implemented to merge stream data with master data. This data was stored in a hashmap which was then used to upload data into the warehouse in the third thread. 
The third thread also calculated the necessary measures. After the data was sent to the warehouse, analytical queries were performed.

Mesh-Join Algorithm:
Input:
1. Primary Dataset (Chunk Dataset): Divided into smaller chunks.
2. Reference Dataset (Circular Dataset): Stored as rotating segments in memory.
3. Join Key: Key(s) used for joining records between datasets.
Output:
Joined and transformed data in a structured format.
1. Initialize:
- Divide the primary dataset into chunks.
- Load reference dataset into a circular buffer.
2. While there are chunks to process:
a. Retrieve the next chunk.
b. Retrieve the current segment of the reference dataset from the circular buffer.
c. Perform join:
- For each record in the chunk:
- Match the join key with records in the current segment.
- If a match is found, combine records and store the joined row.
d. Transform:
- Create composite keys for each joined row.
- Store the transformed data in an output structure.
e. Rotate the circular buffer:
- Move the current segment to the end of the buffer.
3. Output all transformed data

Problems in Mesh-Join
1. Some components of the data are stored in memory (circular buffers for
example), and can consume considerable memory when processing big amounts
of data.
2. The join for two datasets is based on the matching keys between the two
datasets. In the resulting tables, tab and new line characters from copying and
pasting will not work, and keys that are not identical will not guide the joining of
related data.
3. Because there are many similar records, when the data increases, the algorithm
takes much time to tally them. It becomes slow and becomes less sensible for
very big data.

What did I learn ?
1. Tasks splitted in subparts (using threads for example) may be faster than if not
split. It is thus crucial that performance be thought about right from the design
phase until the end of the project.
2. Throughout the development of a system like this, error messages and logs are
more helpful. An effective log can facilitate early and easy identification of
emerging issues in the application

File-Paths
In Main1.java, add file paths to firstCsvFilePath,secondCsvFilePath,thirdCsvFilePath in the
order “transaction.csv”,”customers.csv”,”products.csv”.

DB-Connection
Enter your jdbcUrl,User and Password to establish successful connection with database
