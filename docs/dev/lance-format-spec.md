# Lance Format-Spec Documentation

> Crawled from [https://lance.org](https://lance.org) on 2026-03-21.

---


---

<!-- Source: https://lance.org/format/ -->

# Lance Format Specification¶

Lance is a **Lakehouse Format** that spans three specification layers: file format, table format, and catalog spec.

## Understanding the Lakehouse Stack¶

To understand where Lance fits in the data ecosystem, let's first map out the complete lakehouse technology stack. The modern lakehouse architecture consists of six distinct layers:

### 1\. Object Store¶

At the foundation lies the **object store** —storage systems characterized by their object-based simple hierarchy, typically providing highly durable guarantees with HTTP-based communication protocols for data transfer. This includes systems like S3, GCS, and Azure Blob Storage.

### 2\. File Format¶

Above the storage layer, the **file format** describes how a single file should be stored on disk. This is where formats like Apache Parquet operate, defining the internal structure, encoding, and compression of individual data files.

### 3\. Table Format¶

The **table format** layer describes how multiple files work together to form a logical table. The key feature that modern table formats enable is transactional commits and read isolation to allow multiple writers and readers to safely operate against the same table. All major open source table formats including Iceberg and Lance implement these features through MVCC (Multi-Version Concurrency Control), where each commit atomically produces a new table version, and all table versions form a serializable history for the specific table. This also unlocks features like time travel and makes features like schema evolution easy to develop.

### 4\. Catalog Spec¶

The **catalog spec** defines how any system can discover and manage a collection of tables within storage. This is where the lower storage and format stack meets the upper service and compute stack.

Table formats require at least a way to list all available tables and to describe, add and drop tables in the list. This is necessary for actually building the so-called **connectors** in compute engines so they can discover and start working on the table according to the format. Historically, Hive has defined the Hive MetaStore spec that is sufficient for most table formats including Delta Lake, Hudi, Paimon, and also Lance. Iceberg offers its unique Iceberg REST Catalog spec.

From the top down, projects like Apache Polaris, Unity Catalog, and Apache Gravitino usually offer additional specification for operating against table derivatives (e.g. views, materialized views, user-defined table functions) and objects used in table operations (e.g. user-defined functions, policies).

This intersection between top and bottom stack is also why typically a catalog service would provide both the catalog specifications offered by the format side for easy connectivity to compute engines, as well as providing their own APIs for extended management features.

Another key differentiation of a catalog spec versus a catalog service is that there can be multiple different vendors implementing the same spec. For example, for Polaris REST spec we have open source Apache Polaris server, Snowflake Horizon Catalog, and Polaris-compatible services in AWS Glue, Azure OneLake, etc.

### 5\. Catalog Service¶

A **catalog service** implements one or more catalog specifications to provide both table metadata and optionally continuous background maintenance (compaction, optimization, index updates) that table formats require to stay performant. Catalog services typically implement multiple specifications to support different table formats. For example, Polaris, Unity and Gravitino all support the Iceberg REST catalog specification for Iceberg tables, and have their own generic table API for other table formats.

Since table formats are static specifications, catalog services supply the active operational work needed for production deployments. This is often where open source transitions to commercial offerings, as open source projects typically provide metadata functionality, while commercial solutions offer the full operational experience including automated maintenance. There are also open source solutions like Apache Amoro emerging to fill this gap with complete open source catalog service implementations that offer both table metadata access and continuous optimization.

### 6\. Compute Engine¶

Finally, **compute engines** are the workhorses that visit catalog services and leverage their knowledge of file formats, table formats, and catalog specifications to perform complex data workflows, including SQL queries, analytics processing, vector search, full-text search, and machine learning training. All sorts of applications can be built on top of compute engines to serve more concrete analytics, ML and AI use cases.

### The Overall Lakehouse Architecture¶

In the lakehouse architecture, compute power resides in the object store, catalog services, and compute engines. The middle three layers (file format, table format, catalog spec) are specifications without compute. This separation enables portability and interoperability.

## Understanding Lance as a Lakehouse Format¶

Lance spans all three specification layers:

  1. **File Format** : The Lance columnar file format, [read specification →](<file/>)
  2. **Table Format** : The Lance table format, [read specification →](<table/>)
  3. **Catalog Spec** : The Lance Namespace specification, [read specification →](<namespace/>)


For comparison:

  * **Apache Iceberg** operates at the table format and catalog spec layers, using Apache Parquet, Apache Avro and Apache ORC as the file format
  * **Delta Lake** and **Apache Hudi** operate at only the table format layer, using Apache Parquet as the file format


Back to top



---

<!-- Source: https://lance.org/format/file/ -->

# Lance File Format¶

## File Structure¶

A Lance file is a container for tabular data. The data is stored in "disk pages". Each disk page contains some rows for a single column. There may be one or more disk pages per column. Different columns may have different numbers of disk pages. Metadata at the end of the file describes where the pages are located and how the data is encoded.

Note

This page describes the container specification. We also have a set of default encodings that are used to encode data into disk pages. See the [Encoding Strategy](<encoding/>) page for more details.

### Disk Pages¶

Disk pages are designed to be large enough to justify a dedicated I/O operation, even on cloud storage, typically several megabytes. Using a larger page size may reduce the number of I/O operations required to read a file, but it also increases the amount of memory required to write the file. In practice, very large page sizes are not useful when high speed reads are required because large contiguous reads need to be broken into smaller reads for performance (particularly on cloud storage). As a result, a default of 8MB is recommended for the page size and should yield ideal performance on all storage systems.

Disk pages should not generally be opaque. It is possible to read a portion of a disk page when a subset of the rows are required. However, the specifics of this process depend on the column encoding which is described in a later section.

### No Row Groups¶

Unlike similar formats, there is no "row group" concept, only pages. We believe the concept of row groups to be fundamentally harmful to performance. If the row group size is too small then columns will be split into "runt pages" which yield poor read performance on cloud storage. If the row group size is too large then a file writer will need a large amount of RAM since an entire row group must be buffered in memory before it can be written. Instead, to split a file amongst multiple readers we rely on the fact that partial page reads are possible and have minimal read amplification. As a result, you can split the file at whatever row boundary you want.

### Buffer Alignment¶

The file format does not require that buffers be contiguous as buffers are referenced by absolute offsets. In practice, we always align buffers to 64 byte boundaries.

### External Buffers¶

Every page in the file is referenced by an absolute offset. This means that non-page data may be inserted amongst the pages. This can be useful for storing extremely large data types which might only fit a few rows per page otherwise. We can instead store the data out-of-line and store the locations in a page.

In addition, the file format supports "global buffers" which can be used for auxiliary data. This may be used to store a file schema, file indexes, column statistics, or other metadata. References to the global buffers are stored in a special spot in the footer.

### Column Descriptors¶

At the tail of the file is metadata that describes each page in the file, particularly the encoding strategy used. This metadata consists of a series of "column descriptors", which are standalone protobuf messages for each column in the file. Since each column has its own message there is no need to read all file metadata if you are only interested in a subset of the columns. However, in many cases, the column descriptors are small enough that it is cheaper to read the entire footer in a single read than split it into multiple reads.

### Offsets & Footer¶

After the column descriptors there are offset arrays for the column descriptors and global buffers. These simply point to the locations of each item. Finally, there is a fixed-size footer which describes the position of the offset arrays and start of the metadata section.

### Identifiers and Type Systems¶

This basic container format has no concept of types. These are added later by the encoding layer. All columns are referenced by an integer "column index". All global buffers are referenced by an integer "global buffer index". The schema is typically stored in the global buffers, but the file format is unaware of this.

## Reading Strategy¶

The file metadata will need to be known before reading the data. A simple approach for loading the footer is to read one sector from the end (sector depends on the filesystem, 4KiB for local disk, larger for cloud storage). Then parse the footer and read the rest of the metadata (at this point the size will be known). This requires 1-2 IOPS. By storing the metadata size in some other location (e.g. table manifest) it is possible to always read the footer in a single IOP. If there are _many_ columns in the file and only some are desired then it may be better to read individual columns instead of reading all column metadata, increasing the number of IOPS but decreasing the amount of data read.

Next, to read the data, scan through the pages for each column to determine which pages are needed. Each page stores the row offset of the first row in the page. This makes it easy to quickly determine the required pages. The encoding information for the page can then be used to determine exactly which byte ranges are needed from the page.

Disk pages should be large enough that there should no significant benefit to sequentially reading the file. However, if such a use case is desired then the file can be read sequentially once the metadata is known, assuming you want to read all columns in the file.

## Detailed Overview¶

A detailed description of the file layout follows:
    
    
    // Note: the number of buffers (BN) is independent of the number of columns (CN)
    //       and pages.
    //
    //       Buffers often need to be aligned.  64-byte alignment is common when
    //       working with SIMD operations.  4096-byte alignment is common when
    //       working with direct I/O.  In order to ensure these buffers are aligned
    //       writers may need to insert padding before the buffers.
    //
    //       If direct I/O is required then most (but not all) fields described
    //       below must be sector aligned.  We have marked these fields with an
    //       asterisk for clarity.  Readers should assume there will be optional
    //       padding inserted before these fields.
    //
    //       All footer fields are unsigned integers written with little endian
    //       byte order.
    //
    // ├──────────────────────────────────┤
    // | Data Pages                       |
    // |   Data Buffer 0*                 |
    // |   ...                            |
    // |   Data Buffer BN*                |
    // ├──────────────────────────────────┤
    // | Column Metadatas                 |
    // | |A| Column 0 Metadata*           |
    // |     Column 1 Metadata*           |
    // |     ...                          |
    // |     Column CN Metadata*          |
    // ├──────────────────────────────────┤
    // | Column Metadata Offset Table     |
    // | |B| Column 0 Metadata Position*  |
    // |     Column 0 Metadata Size       |
    // |     ...                          |
    // |     Column CN Metadata Position  |
    // |     Column CN Metadata Size      |
    // ├──────────────────────────────────┤
    // | Global Buffers Offset Table      |
    // | |C| Global Buffer 0 Position*    |
    // |     Global Buffer 0 Size         |
    // |     ...                          |
    // |     Global Buffer GN Position    |
    // |     Global Buffer GN Size        |
    // ├──────────────────────────────────┤
    // | Footer                           |
    // | A u64: Offset to column meta 0   |
    // | B u64: Offset to CMO table       |
    // | C u64: Offset to GBO table       |
    // |   u32: Number of global bufs     |
    // |   u32: Number of columns         |
    // |   u16: Major version             |
    // |   u16: Minor version             |
    // |   "LANC"                         |
    // ├──────────────────────────────────┤
    //
    // File Layout-End
    

### Column Metadata¶

The protobuf messages for the column metadata are as follows:
    
    
    message ColumnMetadata {
    
      // This describes a page of column data.
      message Page {
        // The file offsets for each of the page buffers
        //
        // The number of buffers is variable and depends on the encoding.  There
        // may be zero buffers (e.g. constant encoded data) in which case this
        // could be empty.
        repeated uint64 buffer_offsets = 1;
        // The size (in bytes) of each of the page buffers
        //
        // This field will have the same length as `buffer_offsets` and
        // may be empty.
        repeated uint64 buffer_sizes = 2;
        // Logical length (e.g. # rows) of the page
        uint64 length = 3;
        // The encoding used to encode the page
        Encoding encoding = 4;
        // The priority of the page
        //
        // For tabular data this will be the top-level row number of the first row
        // in the page (and top-level rows should not split across pages).
        uint64 priority = 5;
      }
      // Encoding information about the column itself.  This typically describes
      // how to interpret the column metadata buffers.  For example, it could
      // describe how statistics or dictionaries are stored in the column metadata.
      Encoding encoding = 1;
      // The pages in the column
      repeated Page pages = 2;   
      // The file offsets of each of the column metadata buffers
      //
      // There may be zero buffers.
      repeated uint64 buffer_offsets = 3;
      // The size (in bytes) of each of the column metadata buffers
      //
      // This field will have the same length as `buffer_offsets` and
      // may be empty.
      repeated uint64 buffer_sizes = 4;
    
    }
    

Back to top



---

<!-- Source: https://lance.org/format/file/encoding/ -->

# Lance Encoding Strategy¶

The encoding strategy determines how array data is encoded into a disk page. The encoding strategy tends to evolve more quickly than the file format itself.

## Older Encoding Strategies¶

The 0.1 and 2.0 encoding strategies are no longer documented. They were significantly different from future encoding strategies and describing them in detail would be a distraction.

## Terminology¶

An array is a sequence of values. An array has a data type which describes the semantic interpretation of the values. A layout is a way to encode an array into a set of buffers and child arrays. A buffer is a contiguous sequence of bytes. An encoding describes how the semantic interpretation of data is mapped to the layout. An encoder converts data from one layout to another.

Data types and layouts are orthogonal concepts. An integer array might be encoded into two completely different layouts which represent the same data.

### Data Types¶

Lance uses a subset of Arrow's type system for data types. An Arrow data type is both a data type and an encoding. When writing data Lance will often normalize Arrow data types. For example, a string array and a large string array might end up traveling down the same path (variable width data). In fact, most types fall into two general paths. One for fixed-width data and one for variable-width data (where we recognize both 32-bit and 64-bit offsets).

At read time, the Arrow data type is used to determine the target encoding. For example, a string array and large string array might both be stored in the same layout but, at read time, we will use the Arrow data type to determine the size of the offsets returned to the user. There is no requirement the output Arrow type matches the input Arrow type. For example, it is acceptable to write an array as "large string" and then read it back as "string".

## Search Cache¶

The search cache is a key component of the Lance file reader. Random access requires that we locate the physical location of the data in the file. To do so we need to know information such as the encoding used for a column, the location of the page, and potentially other information. This information is collectively known as the "search cache" and is implemented as a basic LRU cache. We define a "initialization phase" which is when we load the various indexing information into the search cache. The cost of initialization is assumed to be amortized over the lifetime of the reader.

When performing full scans (i.e. not random access), we should be able to ignore the search cache and sometimes can avoid loading it entirely. We _do_ want to optimize for cold scans as the initialization phase is often not amortized over the lifetime of the reader.

## Structural Encoding¶

The first step in encoding an array is to determine the structural encoding of the array. A structural encoding breaks the data into smaller units which can be independently decoded. Structural encodings are also responsible for encoding the "structure" (struct validity, list validity, list offsets, etc.) typically utilizing repetition levels and definition levels.

Structural encoding is fairly complicated! However, the goal is to suck out all the details related to I/O scheduling so that compression libraries can focus on compression. This keeps our compression traits simple without sacrificing our ability to perform random access.

There are only a few structural encodings. The structural encoding is described by the `PageLayout` message and is the top-level message for the encoding.
    
    
    message PageLayout {
      oneof layout {
        // A layout used for pages where the data is small
        MiniBlockLayout mini_block_layout = 1;
        // A layout used for pages where all (visible) values are the same scalar value or null.
        ConstantLayout constant_layout = 2;
        // A layout used for pages where the data is large
        FullZipLayout full_zip_layout = 3;
        // A layout where large binary data is encoded externally
        // and only the descriptions are put in the page
        BlobLayout blob_layout = 4;
      }
    
    }
    

### Repetition and Definition Levels¶

Repetition and definition levels are an alternative to validity bitmaps and offset arrays for expressing struct and list information. They have a significant advantage in that they combine all of these buffers into a single buffer which allows us to avoid multiple IOPS.

A more extensive explanation of repetition and definition levels can be found in the code. One particular note is that we use 0 to represent the "inner-most" item and Parquet uses 0 to represent the "outer-most" item. Here is an example:

#### Definition Levels¶

Consider the following array:
    
    
    [{"middle": {"inner": 1]}}, NULL, {"middle": NULL}, {"middle": {"inner": NULL}}]
    

In Arrow we would have the following validity arrays:
    
    
    Outer validity : 1, 0, 1, 1
    Middle validity: 1, ?, 0, 1
    Inner validity : 1, ?, ?, 0
    Values         : 1, ?, ?, ?
    

The ? values are undefined in the Arrow format. We can convert these into definition levels as follows:

Values | Definition | Notes  
---|---|---  
1 | 0 | Valid at all levels  
? | 3 | Null at outer level  
? | 2 | Null at middle level  
? | 1 | Null at inner level  
  
#### Repetition Levels¶

Consider the following list array with 3 rows
    
    
    [{<0,1>, <>, <2>}, {<3>}, {}], [], [{<4>}]
    

We would have three offsets arrays in Arrow:
    
    
    Outer-most ([]): [0, 3, 3, 4]
    Middle     ({}): [0, 3, 4, 4, 5]
    Inner      (<>): [0, 2, 2, 3, 4, 5]
    Values         : [0, 1, 2, 3, 4]
    

We can convert these into repetition levels as follows:

Values | Repetition | Notes  
---|---|---  
0 | 3 | Start of outer-most list  
1 | 0 | Continues inner-most list (no new lists)  
? | 1 | Start of new inner-most list (empty list)  
2 | 1 | Start of new inner-most list  
3 | 2 | Start of new middle list  
? | 2 | Start of new inner-most list (empty list)  
? | 3 | Start of new outer-most list (empty list)  
4 | 0 | Start of new outer-most list  
  
### Mini Block Page Layout¶

The mini block page layout is the default layout for smallish types. This fits most of the classical data types (integers, floats, booleans, small strings, etc.) that Parquet and related formats already handle well. As is no surprise, the approach used is pretty similar to those formats.

The data is divided into small mini-blocks. Each mini-block should contain a power-of-two number of values (except for the last mini-block) and should be less than 32KiB of compressed data. We have to read an entire mini-block to get a single value so we want to keep the mini-block size small. Mini blocks are padded to 8 byte boundaries. This helps to avoid alignment issues. Each mini-block starts with a small header which helps us figure out how much padding has been applied.

The repetition and definition levels are sliced up and stored in the mini-blocks along with the compressed buffers. Since we need to read an entire mini-block there is no need to zip up the various buffers and they are stored one after the other (repetition, definition, values, ...).

#### Buffer 1 (Mini Blocks)¶

Bytes | Meaning  
---|---  
1 | Number of buffers in the mini-block  
2 | Size of buffer 0  
2 | Size of buffer 1  
... | ...  
2 | Size of buffer N  
0-7 | Padding to ensure 8 byte alignment  
* | Buffer 0  
0-7 | Padding to ensure 8 byte alignment  
* | Buffer 1  
... | ...  
0-7 | Padding to ensure 8 byte alignment  
* | Buffer N  
0-7 | Padding to ensure 8 byte alignment  
  
Note: It is natural to explain this buffer first but it is actually the second buffer in the page.

#### Buffer 0 (Mini Block Metadata)¶

To enable random access we have a small metadata lookup which contains two bytes per mini-block. This lookup tells us how many bytes are in each mini block and how many items are in the mini block. This metadata lookup must be loaded at initialization time and placed in the search cache.

Bits (not bytes) | Meaning  
---|---  
12 | Number of 8-byte words in block 0  
4 | Log2 of number of values in block 0  
12 | Number of 8-byte words in block 1  
4 | Log2 of number of values in block 1  
... | ...  
12 | Number of 8-byte words in block N  
4 | Log2 of number of values in block N  
  
For all chunks except the last, the lower 4 bits store `log2(num_values)` and `num_values` must be a power of two. For the last chunk, these bits are set to `0`. The protobuf stores the total number of values in the page, so readers can derive the final chunk size by subtracting the values from earlier chunks.

#### Buffer 2 (Dictionary, optional)¶

Dictionary encoding is an encoding that can be applied at many different levels throughout a file. For example, it could be used as a compressive encoding or it could even be entirely external to the file. We've found the most convenient simple place to apply dictionary encoding is at the structural level. Since dictionary indices are small we always use the mini block layout for dictionary encoding. When we use dictionary encoding we store the dictionary in the buffer at index 2. We require the dictionary to be full loaded and decoded at initialization time. This means we don't have to load the dictionary during random access but it does require the dictionary be placed in the search cache.

Dictionary values are stored as a single buffer and compressed through the block compression path. The compression scheme for dictionary values can be configured separately (see `lance-encoding:dict-values-compression` below).

#### Buffer 2 (or 3) (Repetition Index, optional)¶

If there is repetition (list levels) then we need some way to translate row offsets into item offsets. The mini blocks always store items. During a full scan the list offsets are restored when we decode the repetition levels. However, to support random access, we don't have the repetition levels available. Instead we store a repetition index in the next available buffer (index 2 or 3 depending on whether the dictionary is present).

The repetition index is a flat buffer of u64 values. We have N * D values where N is the number of mini blocks and D Is the desired depth of random access plus one. For example, to support 1-dimensional lookups (random access by rows) then D is 2. To support two-dimensional lookups (e.g. rows[50][17]) then we could set D to 3.

Currently we only support 1-dimensional random access. Currently we do not compress the repetition index.

This may change in future versions.

Bytes | Meaning  
---|---  
8 | Number of rows in block 0  
8 | Number of partial items in block 0  
8 | Number of rows in block 1  
8 | Number of partial items in block 1  
... | ...  
8 | Number of rows in block N  
8 | Number of partial items in block N  
  
The last 8 bytes of each block stores the number of "partial" items. These are items leftover after the last complete row. We don't require rows to be bounded by mini-blocks so we need to keep track of this. For example, if we have 10,000 items per row then we might have several mini-blocks with only partial items and 0 rows.

At read time we can use this repetition index to translate row offsets into item offsets.

#### Mini Block Compression¶

The mini block layout relies on the compression algorithm to handle the splitting of data into mini-blocks. This is because the number of values per block will depend on the compressibility of the data. As a result, there is a special trait for mini block compression.

The data compression algorithm is the algorithm that decides chunk boundaries. The repetition and definition levels are then sliced appropriately and sent to a block compressor. This means there are no constraints on how the repetition and definition levels are compressed.

Beyond splitting the data into mini-blocks, there are no additional constraints. We expect to fully decode mini blocks as opaque chunks. This means we can use any compression algorithm that we deem suitable.

#### Protobuf¶
    
    
    message MiniBlockLayout {
      // Description of the compression of repetition levels (e.g. how many bits per rep)
      //
      // Optional, if there is no repetition then this field is not present
      CompressiveEncoding rep_compression = 1;
      // Description of the compression of definition levels (e.g. how many bits per def)
      //
      // Optional, if there is no definition then this field is not present
      CompressiveEncoding def_compression = 2;
      // Description of the compression of values
      CompressiveEncoding value_compression = 3;
      // Description of the compression of the dictionary data
      //
      // Optional, if there is no dictionary then this field is not present
      CompressiveEncoding dictionary = 4;
      // Number of items in the dictionary
      uint64 num_dictionary_items = 5;
      // The meaning of each repdef layer, used to interpret repdef buffers correctly
      repeated RepDefLayer layers = 6;
      // The number of buffers in each mini-block, this is determined by the compression and does
      // NOT include the repetition or definition buffers (the presence of these buffers can be determined
      // by looking at the rep_compression and def_compression fields)
      uint64 num_buffers = 7;
      // The depth of the repetition index.
      //
      // If there is repetition then the depth must be at least 1.  If there are many layers
      // of repetition then deeper repetition indices will support deeper nested random access.  For
      // example, given 5 layers of repetition then the repetition index depth must be at least
      // 3 to support access like `rows[50][17][3]`.
      //
      // We require `repetition_index_depth + 1` u64 values per mini-block to store the repetition
      // index if the `repetition_index_depth` is greater than 0.  The +1 is because we need to store
      // the number of "leftover items" at the end of the chunk.  Otherwise, we wouldn't have any way
      // to know if the final item in a chunk is valid or not.
      uint32 repetition_index_depth = 8;
      // The page already records how many rows are in the page.  For mini-block we also need to know how
      // many "items" are in the page.  A row and an item are the same thing unless the page has lists.
      uint64 num_items = 9;
    
      // Since Lance 2.2, miniblocks have larger chunk sizes (>= 64KB)
      bool has_large_chunk = 10;
    
    }
    

The protobuf for the mini block layout describes the compression of the various buffers. It also tells us some information about the dictionary (if present) and the repetition index (if present).

### Full Zip Page Layout¶

The full zip page layout is a layout for larger values (e.g. vector embeddings) which are large but not so large that we can justify a single IOP per value. In this case we are trying to avoid storing a large amount of "chunk overhead" (both in terms of buffer space and the RAM space in the search cache that we would need to store the repetition index). As a tradeoff, we are introducing a second IOP per-range for random access reads (unless the data is fixed-width such as vector embeddings).

We currently use 256 bytes as the cutoff for the full zip layout. At this point we would only be fitting 16 values in a 4KiB disk sector and so creating a mini-block descriptor for every 16 values would be too much overhead.

As a further consequence, we must ensure that the compression algorithm is "transparent" so that we can index individual values after compression has been applied. This prevents us from using compression algorithms such as delta encoding. If we want to apply general compression we have to apply them on a per-value basis. The way we enforce this is by requiring the compression to return either a flat fixed-width or variable-width layout so that we know the location of each element.

The repetition and definition levels, along with all compressed buffers, are all zipped together into a single buffer.

#### Data Buffer (Buffer 0)¶

The data buffer is a single buffer that contains the repetition, definition, and value data, all zipped into a single buffer. The repetition and definition information are combined and byte packed. This is referred to as a control word. If the value is null or an empty list, then the control word is all that is serialized. If there is no validity or repetition information then control words are not serialized. If the value is variable-width then we encode the size of the value. This is either a 4-byte or 8-byte integer depending on the width used in the offsets returned by the compression (in future versions this will likely be encoded with some kind of variable-width integer encoding). Finally the value buffers themselves are appended.

Bytes | Meaning  
---|---  
0-4 | Control word 0  
0/4/8 | Value 0 size  
* | Value 0 data  
... | ...  
0-4 | Control word N  
0/4/8 | Value N size  
* | Value N data  
  
Note: a fixed-width data type that has no validity information (e.g. non-nullable vector embeddings) is simply a flat buffer of data.

#### Repetition Index (Buffer 1)¶

If there is repetition information or the values are variable width then we need additional help to locate values in the disk page. The repetition index is an array of u64 values. There is one value per row and the value is an offset to the start of that row in the data buffer. To perform random access we require two IOPS. First we issue an IOP into the repetition index to determine the location and then a second IOP into the data buffer to load the data. Alternatively, the entire repetition index can be loaded into memory in the initialization phase though this can lead to high RAM usage by the search cache.

The repetition index must have a fixed width (or else we would need a repetition index to read the repetition index!) and be transparent. As a result the compression options are limited. That being said, there is little value (in terms of performance) in compressing the repetition index. It is never read in its entirety as it is not needed for full scans. Currently the repetition index is always compressed with simple (non-chunked) byte packing into 1,2,4, or 8 byte values.

#### Protobuf¶
    
    
    message FullZipLayout {
      // The number of bits of repetition info (0 if there is no repetition)
      uint32 bits_rep = 1;
      // The number of bits of definition info (0 if there is no definition)
      uint32 bits_def = 2;
      // The number of bits of value info
      //
      // Note: we use bits here (and not bytes) for consistency with other encodings.  However, in practice,
      // there is never a reason to use a bits per value that is not a multiple of 8.  The complexity is not
      // worth the small savings in space since this encoding is typically used with large values already.
      oneof details {
        // If this is a fixed width block then we need to have a fixed number of bits per value
        uint32 bits_per_value = 3;
        // If this is a variable width block then we need to have a fixed number of bits per offset
        uint32 bits_per_offset = 4;
      }
      // The number of items in the page
      uint32 num_items = 5;
      // The number of visible items in the page
      uint32 num_visible_items = 6;
      // Description of the compression of values
      CompressiveEncoding value_compression = 7;
      // The meaning of each repdef layer, used to interpret repdef buffers correctly
      repeated RepDefLayer layers = 8;
    
    }
    

The protobuf for the full zip layout describes the compression of the data buffer. It also tells us the size of the control words and how many bits we have per value (for fixed-width data) or how many bits we have per offset (for variable-width data).

### Constant Page Layout¶

This layout is used when all (visible) values in the page are the same scalar value.

The all-null case is represented by a constant page without an inline scalar value. Surprisingly, this does not mean there is no data. If there are any levels of struct or list then we need to store the rep/def levels so that we can distinguish between null structs, null lists, empty lists, and null values.

#### Repetition and Definition Levels (Buffers 0 and 1)¶

Note: We currently store rep levels in the first buffer with a flat layout of 16-bit values and def levels in the second buffer with a flat layout of 16-bit values. This will likely change in future versions.

#### Protobuf¶
    
    
    message ConstantLayout {
      // The meaning of each repdef layer, used to interpret repdef buffers correctly
      repeated RepDefLayer layers = 5;
    
      // Inline fixed-width scalar value bytes.
      //
      // This MUST only be used for types where a single non-null element is represented by a single
      // fixed-width Arrow value buffer (i.e. no offsets buffer, no child data).
      //
      // Constraints:
      // - MUST be absent for an all-null page
      // - MUST be <= 32 bytes if present
      optional bytes inline_value = 6;
    
      // Optional compression algorithm used for the repetition buffer.
      // If absent, repetition levels are stored as raw u16 values.
      CompressiveEncoding rep_compression = 7;
      // Optional compression algorithm used for the definition buffer.
      // If absent, definition levels are stored as raw u16 values.
      CompressiveEncoding def_compression = 8;
      // Number of values in repetition buffer after decompression.
      uint64 num_rep_values = 9;
      // Number of values in definition buffer after decompression.
      uint64 num_def_values = 10;
    
    }
    

All we need to know is the meaning of each rep/def level and (when present) the inline scalar value bytes.

### Blob Page Layout¶

The blob page layout is a layout for large binary values where we would only have a few values per disk page. The actual data is stored out-of-line in external buffers. The disk page stores a "description" which is a struct array of two fields: `position` and `size`. The `position` is the absolute file offset of the blob and the `size` is the size (in bytes) of the blob. The inner page layout describes how the descriptions are encoded.

The validity information (definition levels) is smuggled into the descriptions. If the size and position are both zero then the value is empty. Otherwise, if the size is zero and the position is non-zero then the value is null and the position is the definition level.

This layout is only recommended when you can justify a single IOP per value. For example, when values are 1MiB or larger.

This layout has no buffers of its own and merely wraps an inner layout.

#### Protobuf¶
    
    
    message BlobLayout {
      // The inner layout used to store the descriptions
      PageLayout inner_layout = 1;
      // The meaning of each repdef layer, used to interpret repdef buffers correctly
      //
      // The inner layout's repdef layers will always be 1 all valid item layer
      repeated RepDefLayer layers = 2;
    
    }
    

Since we smuggle the validity into the descriptions we don't need to store it in the inner layout and so the rep/def meaning is stored in the blob layout and the rep/def meaning in the inner layout will be 1 all valid item layer.

## Semi-Structural Transformations¶

There are some data transformations that are applied to the data before (or during) the structural encoding process. These are described here.

### Dictionary Encoding¶

Dictionary encoding is a technique that can be applied to any kind of array. It is useful when there are not very many unique values in the array. First, a "dictionary" of unique values is created. Then we create a second array of indices into the dictionary.

Dictionary encoding is also known as "categorical encoding" in other contexts.

Dictionary encoding could be treated as simply another compression technique but, when applied, it would be an opaque compression technique which would limit its usability (e.g. in a full zip context). As a result, we apply it before any structural encoding takes place. This allows us to place the dictionary in the search cache for random access.

### Struct Packing¶

Struct packing is an alternative representation to apply to struct values. Instead of storing that struct in a columnar fashion it will be stored in a row-major fashion. This will reduce the number of IOPS needed for random access but will prevent the ability to read a single field at a time. This is useful when all fields in the struct are always accessed together.

Packed struct is always opt-in (see section on configuration below).

In Lance 2.1, packed struct is limited to fixed-width children (`PackedStruct`). Starting with Lance 2.2, variable-width children are also supported via `VariablePackedStruct`.

### Fixed Size List¶

Fixed size lists are an Arrow data type that needs specialized handling at the structural level. If the underlying data type is primitive then the fixed size list will be primitive (e.g. a tensor). If the underlying data type is structural (struct/list) then the fixed size list is structural and should be treated the same as a variable-size list.

We don't want compression libraries to need to worry about the intricacies of fixed-size lists. As a result we flatten the list as part of structural encoding. This complicates random access as we must translate between rows (an entire fixed size list) and items (a single item in the list).

If the items in a fixed size list are nullable then we do not treat that validity array as a repetition or definition level. Instead, we store the validity as a separate buffer. For example, when encoding nullable fixed size lists with mini-block encoding the validity buffer is another buffer in the mini-block. When encoding nullable fixed size lists with full-zip encoding the validity buffer is zipped together with the values.

The good news is that fixed size lists are entirely a structural encoding concern. Compression techniques are free to pretend that the fixed-size list data type does not exist.

## Compression¶

Once a structural encoding is chosen we must determine how to compress the data. There are various buffers that might be compressed (e.g. data, repetition, definition, dictionary, etc.). The available compression algorithms are also constrained by the structural encoding chosen. For example, when using the full zip layout we require transparent compression. As a result, each encoding technique may or may not be usable in a given scenario. In addition, the same technique may be applied in a different way depending on the encoding chosen.

In implementation terms we have a trait for each compression constraint. The techniques then implement the traits that they can be applied to. To start with, here is a summary of compression techniques which are implemented in at least one scenario and a list of which traits the technique implements. A ❓ is used to indicate that the technique should be usable in that context but we do not yet do so while a ❌ indicates that the technique is not usable because it is not transparent. Note, even though a technique is not transparent it can still be applied on a per-value basis. We use ☑️ to mark a technique that is applied on a per-value basis:

Compression | Used in Block Context | Used in Full Zip Context | Used in Mini-Block Context  
---|---|---|---  
Flat | ✅ (2.1) | ✅ (2.1) | ✅ (2.1)  
Variable | ✅ (2.1) | ✅ (2.1) | ✅ (2.1)  
Constant | ✅ (2.1) | ❓ | ❓  
Bitpacking | ✅ (2.1) | ❓ | ✅ (2.1)  
Fsst | ❓ | ✅ (2.1) | ✅ (2.1)  
Rle | ✅ (2.2) | ❌ | ✅ (2.1)  
ByteStreamSplit | ❓ | ❌ | ✅ (2.1)  
General | ✅ (2.2) | ☑️ (2.1) | ✅ (2.1)  
  
In the following sections we will describe each technique in a bit more detail and explain how it is utilized in various contexts.

### Flat¶

Flat compression is the uncompressed representation of fixed-width data. There is a single buffer of data with a fixed number of bits per value.

When applied in a mini-block context we find the largest power of 2 number of values that will be less than 8,186 bytes and use that as the block size.

### Variable¶

Variable compression is the uncompressed representation of variable-width data. There is a buffer of values and a buffer of offsets.

When applied in a mini-block context each block may have a different number of values. We walk through the values until we find the point that would exceed 4,096 bytes and then use the most recent power of 2 number of values that we have passed.

### Constant¶

Constant compression is currently only utilized in a few specialized scenarios such as all-null arrays.

This will likely change in future versions.

### Bitpacking¶

Bitpacking is a compression technique that removes the unused bits from a set of values. For example, if we have a u32 array and the maximum value is 5000 then we only need 13 bits to store each value.

When used in a mini-block context we always use 1024 values per block. In addition, we store the compressed bit width inline in the block itself.

Bitpacking is, in theory, usable in a full zip context. However, values in this context are so large that shaving off a few bits is unlikely to have any meaningful impact. Also, the full-zip context keeps things byte-aligned and so we would have to remove at least 8 bits per value.

### Fsst¶

Fsst is a fast and transparent compression algorithm for variable-width data. It is the primary compression algorithm that we apply to variable-width data.

Currently we use a single FSST symbol table per disk page and store that symbol table in the protobuf description. This is for historical reasons and is not ideal and will likely change in future versions.

When FSST is applied in a mini-block context we simply compress the data and let the underlying compressor (always `Variable` at the moment) handle the chunking.

### Run Length Encoding (RLE)¶

Run length encoding is a compression technique that compresses large runs of identical values into an array of values and an array of run lengths. This is currently used in the mini-block context. To determine if we should apply run-length encoding we look at the number of runs divided by the number of values. If the ratio is below a threshold (by default 0.5) then we apply run-length encoding.

### Byte Stream Split (BSS)¶

Byte stream split is a compression technique that splits multi-byte values by byte position, creating separate streams for each byte position across all values. This is a rudimentary and simple form of translating floating point values into a more compressible format because it tends to cluster the mantissa bits together which are often consistent across a column of floating point values. It does not actually make the data smaller by itself. As a result, BSS is only applied if general compression is also applied on the column.

We currently determine whether or not to apply BSS by looking at an entropy statistics. There is a configurable sensitivity parameter. A sensitivity of 0.0 means never apply BSS and a sensitivity of 1.0 means always apply BSS.

### General¶

General compression is a catch-all term for classical opaque compression techniques such as LZ4, ZStandard, Snappy, etc. These techniques are typically back-referencing compressors which replace values with a "back reference" to a spot where we already saw the value.

When applied in a mini-block context we run general compression after all other compression and compress the entire mini-block.

When applied in a full zip context we run general compression on each value.

The only time general compression is automatically applied is in a full-zip context when we have values that are at least 32KiB large. This is because general compression can be CPU intensive.

However, general compression is highly effective and we allow it to be opted into in other contexts via configuration.

## Compression Configuration¶

The following section lists the available configuration options. These can be set programmatically through writer options. However, they can also be set in the field metadata in the schema.

Key | Values | Default | Description  
---|---|---|---  
`lance-encoding:compression` | `lz4`, `zstd`, `none`, ... | `none` | Opt-in to general compression. The value indicates the scheme.  
`lance-encoding:compression-level` | Integers (range is scheme dependent) | Varies by scheme | Higher indicates more work should be done to compress the data.  
`lance-encoding:rle-threshold` | `0.0-1.0` | `0.5` | See below  
`lance-encoding:bss` | `off`, `on`, `auto` | `auto` | See below  
`lance-encoding:dict-divisor` | Integers greater than 1 | `2` | See below  
`lance-encoding:dict-size-ratio` | `0.0-1.0` | `0.8` | See below  
`lance-encoding:dict-values-compression` | `lz4`, `zstd`, `none` | `lz4` | Select general compression scheme for dictionary values  
`lance-encoding:dict-values-compression-level` | Integers (scheme dependent) | Varies by scheme | Compression level for dictionary values general compression  
`lance-encoding:general` | `off`, `on` | `off` | Whether to apply general compression.  
`lance-encoding:packed` | Any string | Not set | Whether to apply packed struct encoding (see above).  
`lance-encoding:structural-encoding` | `miniblock`, `fullzip` | Not set | Force a particular structural encoding to be applied (only useful for testing purposes)  
  
### Configuration Details¶

#### Compression Scheme¶

The `lance-encoding:compression` setting enables general-purpose compression algorithms to be applied. Available schemes:

  * **`lz4`** : Fast compression with good compression ratios. Default compression level is fast mode.
  * **`zstd`** : High compression ratios with configurable levels (0-22). Better compression than LZ4 but slower.
  * **`none`** : No general compression applied (default).
  * **`fsst`** : Fast Static Symbol Table compression for string data.


General compression is applied on top of other encoding techniques (RLE, BSS, bitpacking, etc.) to further reduce data size. For mini-block layouts, compression is applied to entire mini-blocks. For full-zip layouts with large values (≥32KiB), compression is automatically applied per-value.

#### Compression Level¶

The compression level is scheme dependent. Currently the following schemes support the following levels:

Scheme | Crate Used | Levels | Default  
---|---|---|---  
`zstd` | [`zstd`](<https://crates.io/crates/zstd>) | `0-22` | `crate dependent` (3 as of this writing)  
`lz4` | [`lz4`](<https://crates.io/crates/lz4>) | N/A | The LZ4 crate has two modes (fast and high compression) and currently this is not exposed to configuration. The LZ4 crate wraps a C library and the default is dependent on the C library. The default as of this writing is fast  
  
Higher compression levels generally provide better compression at the cost of slower encoding speed. Decoding speed is typically less affected by the compression level.

#### Run Length Encoding (RLE) Threshold¶

The RLE threshold is used to determine whether or not to apply run-length encoding. The threshold is a ratio calculated by dividing the number of runs by the number of values. If the ratio is less than the threshold then we apply run-length encoding. The default is 0.5 which means we apply run-length encoding if the number of runs is less than half the number of values.

**Key points:** \- RLE is automatically selected when data has sufficient repetition (run_count / num_values < threshold) \- Supported types: All fixed-width primitives (u8, i8, u16, i16, u32, i32, f32, u64, i64, f64) \- Maximum chunk size: 2048 values per mini-block \- Setting threshold to `0.0` effectively disables RLE \- Setting threshold to `1.0` makes RLE very aggressive (used whenever any runs exist)

RLE is particularly effective for: \- Sorted or partially sorted data \- Columns with many repeated values (status codes, categories, etc.) \- Low-cardinality columns

#### Byte Stream Split (BSS)¶

The configuration variable for BSS is a simple enum. A value of `off` means to never apply BSS, a value of `on` means to always apply BSS, and a value of `auto` means to apply BSS based on an entropy calculation (see code for details).

**Important:** BSS is only applied when the `lance-encoding:compression` variable is also set (to a non-`none` value). BSS is a data transformation that makes floating-point data more compressible; it does not reduce size on its own.

**Key points:** \- Supported types: Only 32-bit and 64-bit data (f32, f64, timestamps) \- Maximum chunk sizes: 1024 values (f32), 512 values (f64) \- `auto` mode: Uses entropy analysis with 0.5 sensitivity threshold \- `on` mode: Always applies BSS for supported types \- `off` mode: Never applies BSS

BSS works by splitting multi-byte values by byte position, creating separate byte streams. This clusters similar bits together (especially mantissa bits in floating-point numbers), which general compression algorithms can then compress more effectively.

BSS is particularly effective for: \- Floating-point measurements with similar ranges \- Time-series data with consistent precision \- Scientific data with correlated mantissa patterns

#### Dictionary Encoding Controls¶

Dictionary encoding is gated by a few heuristics. The decision is made on the leaf value page, so nested types can still benefit. For example, `List<u32>` can use dictionary encoding for its `u32` values.

Two field-level metadata keys control when dictionary encoding is attempted:

  * `lance-encoding:dict-divisor` (default `2`): the encoder computes a unique-value budget as `num_values / divisor`
  * `lance-encoding:dict-size-ratio` (default `0.8`): the estimated dictionary-encoded representation must stay below this ratio of the raw page size


There are additional global guards available as environment variables:

  * `LANCE_ENCODING_DICT_TOO_SMALL` (minimum page size before trying dictionary encoding, default `100` values)
  * `LANCE_ENCODING_DICT_DIVISOR` (fallback divisor when field metadata is not set, default `2`)
  * `LANCE_ENCODING_DICT_MAX_CARDINALITY` (upper cap for dictionary entries, default `100000`)
  * `LANCE_ENCODING_DICT_SIZE_RATIO` (fallback ratio when field metadata is not set, default `0.8`)


Dictionary encoding is effective when values repeat frequently and the number of distinct values stays low.

#### Dictionary Values Compression¶

Dictionary values are compressed through the block-compression path and have their own configuration:

  * `lance-encoding:dict-values-compression`: `lz4`, `zstd`, `none`
  * `lance-encoding:dict-values-compression-level`: optional scheme-specific level


Environment-variable fallbacks:

  * `LANCE_ENCODING_DICT_VALUES_COMPRESSION`
  * `LANCE_ENCODING_DICT_VALUES_COMPRESSION_LEVEL`


Priority order is:

  1. Field metadata (`dict-values-*`)
  2. Environment variables (`LANCE_ENCODING_DICT_VALUES_*`)
  3. Default (`lz4`)


`none` disables general (opaque) compression for dictionary values. For fixed-width dictionary values, structural encodings such as RLE or bitpacking may still be selected when beneficial.

#### Packed Struct Encoding¶

Packed struct encoding is a semi-structural transformation described above. When enabled, struct values are stored in row-major format rather than the default columnar format. This reduces the number of I/O operations needed for random access but prevents reading individual fields independently.

This is always opt-in and should only be used when all struct fields are typically accessed together.

Back to top



---

<!-- Source: https://lance.org/format/file/versioning/ -->

# Versioning¶

The Lance file format has a single version number for both the overall file format and the encoding strategy. The major number is changed when the file format itself is modified while the minor number is changed when only the encoding strategy is modified. Newer versions will typically have better performance and compression but may not be readable by older versions of Lance.

In addition, the `next` alias points to an unstable format version and should not be used for production use cases. Breaking changes could be made to unstable encodings and that would mean that files written with these encodings are no longer readable by any newer versions of Lance. The `next` version should only be used for experimentation and benchmarking upcoming features.

The `stable` and `next` aliases are resolved by the specific Lance release you are using. During a format rollout (for example, 2.2), prefer explicit version pinning for deterministic behavior across environments.

The following values are supported:

Version | Minimal Lance Version | Maximum Lance Version | Description  
---|---|---|---  
0.1 | Any | 0.34 (write) | This is the initial Lance format. It is no longer writable.  
2.0 | 0.16.0 | Any | Rework of the Lance file format that removed row groups and introduced null support for lists, fixed size lists, and primitives  
2.1 | 0.38.1 | Any | Enhances integer and string compression, adds support for nulls in struct fields, and improves random access performance with nested fields.  
2.2 (unstable) | None | Any | Adds support for newer nested type/encoding capabilities (including map support) and 2.2-era storage features.  
legacy | N/A | N/A | Alias for 0.1  
stable | N/A | N/A | Alias for the latest stable version in the Lance release you are running.  
next | N/A | N/A | Alias for the latest unstable version in the Lance release you are running.  
  
Back to top



---

<!-- Source: https://lance.org/format/table/ -->

# Lance Table Format¶

## Overview¶

The Lance table format organizes datasets as versioned collections of fragments and indices. Each version is described by an immutable manifest file that references data files, deletion files, transaction file and indices. The format supports ACID transactions, schema evolution, and efficient incremental updates through Multi-Version Concurrency Control (MVCC).

## Manifest¶

A manifest describes a single version of the dataset. It contains the complete schema definition including nested fields, the list of data fragments comprising this version, a monotonically increasing version number, and an optional reference to the index section that describes a list of index metadata.

Manifest protobuf message
    
    
    message Manifest {
      // All fields of the dataset, including the nested fields.
      repeated lance.file.Field fields = 1;
    
      // Schema metadata.
      map<string, bytes> schema_metadata = 5;
    
      // Fragments of the dataset.
      repeated DataFragment fragments = 2;
    
      // Snapshot version number.
      uint64 version = 3;
    
      // The file position of the version auxiliary data.
      //  * It is not inheritable between versions.
      //  * It is not loaded by default during query.
      uint64 version_aux_data = 4;
    
      message WriterVersion {
        // The name of the library that created this file.
        string library = 1;
        // The version of the library that created this file. Because we cannot assume
        // that the library is semantically versioned, this is a string. However, if it
        // is semantically versioned, it should be a valid semver string without any 'v'
        // prefix. For example: `2.0.0`, `2.0.0-rc.1`.
        //
        // For forward compatibility with older readers, when writing new manifests this
        // field should contain only the core version (major.minor.patch) without any
        // prerelease or build metadata. The prerelease/build info should be stored in
        // the separate prerelease and build_metadata fields instead.
        string version = 2;
        // Optional semver prerelease identifier.
        //
        // This field stores the prerelease portion of a semantic version separately
        // from the core version number. For example, if the full version is "2.0.0-rc.1",
        // the version field would contain "2.0.0" and prerelease would contain "rc.1".
        //
        // This separation ensures forward compatibility: older readers can parse the
        // clean version field without errors, while newer readers can reconstruct the
        // full semantic version by combining version, prerelease, and build_metadata.
        //
        // If absent, the version field is used as-is.
        optional string prerelease = 3;
        // Optional semver build metadata.
        //
        // This field stores the build metadata portion of a semantic version separately
        // from the core version number. For example, if the full version is
        // "2.0.0-rc.1+build.123", the version field would contain "2.0.0", prerelease
        // would contain "rc.1", and build_metadata would contain "build.123".
        //
        // If absent, no build metadata is present.
        optional string build_metadata = 4;
      }
    
      // The version of the writer that created this file.
      //
      // This information may be used to detect whether the file may have known bugs
      // associated with that writer.
      WriterVersion writer_version = 13;
    
      // If present, the file position of the index metadata.
      optional uint64 index_section = 6;
    
      // Version creation Timestamp, UTC timezone
      google.protobuf.Timestamp timestamp = 7;
    
      // Optional version tag
      string tag = 8;
    
      // Feature flags for readers.
      //
      // A bitmap of flags that indicate which features are required to be able to
      // read the table. If a reader does not recognize a flag that is set, it
      // should not attempt to read the dataset.
      //
      // Known flags:
      // * 1: deletion files are present
      // * 2: row ids are stable and stored as part of the fragment metadata.
      // * 4: use v2 format (deprecated)
      // * 8: table config is present
      uint64 reader_feature_flags = 9;
    
      // Feature flags for writers.
      //
      // A bitmap of flags that indicate which features must be used when writing to the
      // dataset. If a writer does not recognize a flag that is set, it should not attempt to
      // write to the dataset.
      //
      // The flag identities are the same as for reader_feature_flags, but the values of
      // reader_feature_flags and writer_feature_flags are not required to be identical.
      uint64 writer_feature_flags = 10;
    
      // The highest fragment ID that has been used so far.
      //
      // This ID is not guaranteed to be present in the current version, but it may
      // have been used in previous versions.
      //
      // For a single fragment, will be zero. For no fragments, will be absent.
      optional uint32 max_fragment_id = 11;
    
      // Path to the transaction file, relative to `{root}/_transactions`. The file at that
      // location contains a wire-format serialized Transaction message representing the
      // transaction that created this version.
      //
      // This string field "transaction_file" may be empty if no transaction file was written.
      //
      // The path format is "{read_version}-{uuid}.txn" where {read_version} is the version of
      // the table the transaction read from (serialized to decimal with no padding digits),
      // and {uuid} is a hyphen-separated UUID.
      string transaction_file = 12;
    
      // The file position of the transaction content. None if transaction is empty
      // This transaction content begins with the transaction content length as u32
      // If the transaction proto message has a length of `len`, the message ends at `len` + 4
      optional uint64 transaction_section = 21;
    
      // The next unused row id. If zero, then the table does not have any rows.
      //
      // This is only used if the "stable_row_ids" feature flag is set.
      uint64 next_row_id = 14;
    
      message DataStorageFormat {
        // The format of the data files (e.g. "lance")
        string file_format = 1;
        // The max format version of the data files. The format of the version can vary by
        // file_format and is not required to follow semver.
        //
        // Every file in this version of the dataset has the same file_format version.
        string version = 2;
      }
    
      // The data storage format
      //
      // This specifies what format is used to store the data files.
      DataStorageFormat data_format = 15;
    
      // Table config.
      //
      // Keys with the prefix "lance." are reserved for the Lance library. Other
      // libraries may wish to similarly prefix their configuration keys
      // appropriately.
      map<string, string> config = 16;
    
      // Metadata associated with the table.
      //
      // This is a key-value map that can be used to store arbitrary metadata
      // associated with the table.
      //
      // This is different than configuration, which is used to tell libraries how
      // to read, write, or manage the table.
      //
      // This is different than schema metadata, which is used to describe the
      // data itself and is attached to the output schema of scans.
      map<string, string> table_metadata = 19;
    
      // Field number 17 (`blob_dataset_version`) was used for a secondary blob dataset.
      reserved 17;
      reserved "blob_dataset_version";
    
      // The base paths of data files.
      //
      // This is used to determine the base path of a data file. In common cases data file paths are under current dataset base path.
      // But for shallow cloning, importing file and other multi-tier storage cases, the actual data files could be outside of the current dataset.
      // This field is used with the `base_id` in `lance.file.File` and `lance.file.DeletionFile`.
      //
      // For example, if we have a dataset with base path `s3://bucket/dataset`, we have a DataFile with base_id 0, we get the actual data file path by:
      // base_paths[id = 0] + /data/ + file.path
      // the key(a.k.a index) starts from 0, increased by 1 for each new base path.
      repeated BasePath base_paths = 18;
    
      // The branch of the dataset. None means main branch.
      optional string branch = 20;
    
    }
    

## Schema & Fields¶

The schema of the table is written as a series of fields, plus a schema metadata map. The data types generally have a 1-1 correspondence with the Apache Arrow data types. Each field, including nested fields, have a unique integer id. At initial table creation time, fields are assigned ids in depth-first order. Afterwards, field IDs are assigned incrementally for newly added fields.

Column encoding configurations are specified through field metadata using the `lance-encoding:` prefix. See [File Format Encoding Specification](<../file/encoding/>) for details on available encodings, compression schemes, and configuration options.

For complete schema specification details including supported data types, field ID assignment, and metadata handling, see the [Schema Format Specification](<schema/>).

Field protobuf message
    
    
    message Field {
      enum Type {
        PARENT = 0;
        REPEATED = 1;
        LEAF = 2;
      }
      Type type = 1;
    
      // Fully qualified name.
      string name = 2;
      /// Field Id.
      ///
      /// See the comment in `DataFile.fields` for how field ids are assigned.
      int32 id = 3;
      /// Parent Field ID. If not set, this is a top-level column.
      int32 parent_id = 4;
    
      // Logical types, support parameterized Arrow Type.
      //
      // PARENT types will always have logical type "struct".
      //
      // REPEATED types may have logical types:
      // * "list"
      // * "large_list"
      // * "list.struct"
      // * "large_list.struct"
      // The final two are used if the list values are structs, and therefore the
      // field is both implicitly REPEATED and PARENT.
      //
      // LEAF types may have logical types:
      // * "null"
      // * "bool"
      // * "int8" / "uint8"
      // * "int16" / "uint16"
      // * "int32" / "uint32"
      // * "int64" / "uint64"
      // * "halffloat" / "float" / "double"
      // * "string" / "large_string"
      // * "binary" / "large_binary"
      // * "date32:day"
      // * "date64:ms"
      // * "decimal:128:{precision}:{scale}" / "decimal:256:{precision}:{scale}"
      // * "time:{unit}" / "timestamp:{unit}" / "duration:{unit}", where unit is
      // "s", "ms", "us", "ns"
      // * "dict:{value_type}:{index_type}:false"
      string logical_type = 5;
      // If this field is nullable.
      bool nullable = 6;
    
      // optional field metadata (e.g. extension type name/parameters)
      map<string, bytes> metadata = 10;  
    
      bool unenforced_primary_key = 12;
    
      // Position of this field in the primary key (1-based).
      // 0 means the field is part of the primary key but uses schema field id for ordering.
      // When set to a positive value, primary key fields are ordered by this position.
      uint32 unenforced_primary_key_position = 13;
    
      // DEPRECATED ----------------------------------------------------------------
    
      // Deprecated: Only used in V1 file format. V2 uses variable encodings defined
      // per page.
      //
      // The global encoding to use for this field.
      Encoding encoding = 7;
    
      // Deprecated: Only used in V1 file format. V2 dynamically chooses when to
      // do dictionary encoding and keeps the dictionary in the data files.
      //
      // The file offset for storing the dictionary value.
      // It is only valid if encoding is DICTIONARY.
      //
      // The logic type presents the value type of the column, i.e., string value.
      Dictionary dictionary = 8;
    
      // Deprecated: optional extension type name, use metadata field
      // ARROW:extension:name
      string extension_name = 9;
    
      // Field number 11 was previously `string storage_class`.
      // Keep it reserved so older manifests remain compatible while new writers
      // avoid reusing the slot.
      reserved 11;
      reserved "storage_class";
    
    }
    

### Unenforced Primary Key¶

Lance supports defining an unenforced primary key through field metadata. This is useful for deduplication during merge-insert operations and other use cases that benefit from logical row identity. The primary key is "unenforced" meaning Lance does not always validate uniqueness constraints. Users can use specific workloads like merge-insert to enforce it if necessary. The primary key is fixed after initial setting and must not be updated or removed.

A primary key field must satisfy:

  * The field, and all its ancestors, must not be nullable.
  * The field must be a leaf field (primitive data type without children).
  * The field must not be within a list or map type.


When using an Arrow schema to create a Lance table, add the following metadata to the Arrow field to mark it as part of the primary key:

  * `lance-schema:unenforced-primary-key`: Set to `true`, `1`, or `yes` (case-insensitive) to indicate the field is part of the primary key.
  * `lance-schema:unenforced-primary-key:position` (optional): A 1-based integer specifying the position within a composite primary key.


For composite primary keys with multiple columns, the position determines the primary key field ordering:

  * When positions are specified, fields are ordered by their position values (1, 2, 3, ...).
  * When positions are not specified, fields are ordered by their schema field id.
  * Fields with explicit positions are ordered before fields without.


## Fragments¶

A fragment represents a horizontal partition of the dataset containing a subset of rows. Each fragment has a unique `uint32` identifier assigned incrementally based on the dataset's maximum fragment ID. Each fragment consists of one or more data files storing columns, plus an optional deletion file. If present, the deletion file stores the positions (0-based) of the rows that have been deleted from the fragment. The fragment tracks the total row count including deleted rows in its physical rows field. Column subsets can be read without accessing all data files, and each data file is independently compressed and encoded.

DataFragment protobuf message
    
    
    message DataFragment {
      // The ID of a DataFragment is unique within a dataset.
      uint64 id = 1;
    
      repeated DataFile files = 2;
    
      // File that indicates which rows, if any, should be considered deleted.
      DeletionFile deletion_file = 3;
    
      // TODO: What's the simplest way we can allow an inline tombstone bitmap?
    
      // A serialized RowIdSequence message (see rowids.proto).
      //
      // These are the row ids for the fragment, in order of the rows as they appear.
      // That is, if a fragment has 3 rows, and the row ids are [1, 42, 3], then the
      // first row is row 1, the second row is row 42, and the third row is row 3.
      oneof row_id_sequence {
        // If small (< 200KB), the row ids are stored inline.
        bytes inline_row_ids = 5;
        // Otherwise, stored as part of a file.
        ExternalFile external_row_ids = 6;
      } // row_id_sequence
    
      oneof last_updated_at_version_sequence {
        // If small (< 200KB), the row latest updated versions are stored inline.
        bytes inline_last_updated_at_versions = 7;
        // Otherwise, stored as part of a file.
        ExternalFile external_last_updated_at_versions = 8;
      } // last_updated_at_version_sequence
    
      oneof created_at_version_sequence {
        // If small (< 200KB), the row created at versions are stored inline.
        bytes inline_created_at_versions = 9;
        // Otherwise, stored as part of a file.
        ExternalFile external_created_at_versions = 10;
      } // created_at_version_sequence
    
      // Number of original rows in the fragment, this includes rows that are now marked with
      // deletion tombstones. To compute the current number of rows, subtract
      // `deletion_file.num_deleted_rows` from this value.
      uint64 physical_rows = 4;
    
    }
    

### Data Evolution¶

This fragment design enables a new concept called data evolution, which means efficient schema evolution (add column, update column, drop column) with backfill. For example, when adding a new column, new column data are added by appending new data files to each fragment, with values computed for all existing rows in the fragment. There is no need to rewrite the entire table to just add data for a single column. This enables efficient feature engineering and embedding updates for ML/AI workloads.

Each data file should contain a distinct set of field ids. It is not required that all field ids in the dataset schema are found in one of the data files. If there is no corresponding data file, that column should be read as entirely `NULL`.

Field ids might be replaced with `-2`, a tombstone value. In this case that column should be ignored. This used, for example, when rewriting a column: The old data file replaces the field id with `-2` to ignore the old data, and a new data file is appended to the fragment.

## Data Files¶

Data files store column data for a fragment using the Lance file format. Each data file stores a subset of the columns in the fragment. Field IDs are assigned either sequentially based on schema position (for Lance file format v1) or independently of column indices due to variable encoding widths (for Lance file format v2).

DataFile protobuf message
    
    
    message DataFile {
      // Path to the root relative to the dataset's URI.
      string path = 1;
      // The ids of the fields/columns in this file.
      //
      // When a DataFile object is created in memory, every value in fields is assigned -1 by
      // default. An object with a value in fields of -1 must not be stored to disk. -2 is
      // used for "tombstoned", meaning a field that is no longer in use. This is often
      // because the original field id was reassigned to a different data file.
      //
      // In Lance v1 IDs are assigned based on position in the file, offset by the max
      // existing field id in the table (if any already). So when a fragment is first created
      // with one file of N columns, the field ids will be 1, 2, ..., N. If a second fragment
      // is created with M columns, the field ids will be N+1, N+2, ..., N+M.
      //
      // In Lance v1 there is one field for each field in the input schema, this includes
      // nested fields (both struct and list).  Fixed size list fields have only a single
      // field id (these are not considered nested fields in Lance v1).
      //
      // This allows column indices to be calculated from field IDs and the input schema.
      //
      // In Lance v2 the field IDs generally follow the same pattern but there is no
      // way to calculate the column index from the field ID.  This is because a given
      // field could be encoded in many different ways, some of which occupy a different
      // number of columns.  For example, a struct field could be encoded into N + 1 columns
      // or it could be encoded into a single packed column.  To determine column indices
      // the column_indices property should be used instead.
      //
      // In Lance v1 these ids must be sorted but might not always be contiguous.
      repeated int32 fields = 2;
      // The top-level column indices for each field in the file.
      //
      // If the data file is version 1 then this property will be empty
      //
      // Otherwise there must be one entry for each field in `fields`.
      //
      // Some fields may not correspond to a top-level column in the file.  In these cases
      // the index will -1.
      //
      // For example, consider the schema:
      //
      // - dimension: packed-struct (0):
      //   - x: u32 (1)
      //   - y: u32 (2)
      // - path: `list<u32>` (3)
      // - embedding: `fsl<768>` (4)
      //   - fp64
      // - borders: `fsl<4>` (5)
      //   - simple-struct (6)
      //     - margin: fp64 (7)
      //     - padding: fp64 (8)
      //
      // One possible column indices array could be:
      // [0, -1, -1, 1, 3, 4, 5, 6, 7]
      //
      // This reflects quite a few phenomenon:
      // - The packed struct is encoded into a single column and there is no top-level column
      //   for the x or y fields
      // - The variable sized list is encoded into two columns
      // - The embedding is encoded into a single column (common for FSL of primitive) and there
      //   is not "FSL column"
      // - The borders field actually does have an "FSL column"
      //
      // The column indices table may not have duplicates (other than -1)
      repeated int32 column_indices = 3;
      // The major file version used to create the file
      uint32 file_major_version = 4;
      // The minor file version used to create the file
      //
      // If both `file_major_version` and `file_minor_version` are set to 0,
      // then this is a version 0.1 or version 0.2 file.
      uint32 file_minor_version = 5;
    
      // The known size of the file on disk in bytes.
      //
      // This is used to quickly find the footer of the file.
      //
      // When this is zero, it should be interpreted as "unknown".
      uint64 file_size_bytes = 6;
    
      // The base path index of the data file. Used when the file is imported or referred from another dataset.
      // Lance use it as key of the base_paths field in Manifest to determine the actual base path of the data file.
      optional uint32 base_id = 7;
    
    }
    

## Deletion Files¶

Deletion files (a.k.a. deletion vectors) track deleted rows without rewriting data files. Each fragment can have at most one deletion file per version.

Deletion files support two storage formats. The Arrow IPC format (`.arrow` extension) stores a flat Int32Array of deleted row offsets and is efficient for sparse deletions. The Roaring Bitmap format (`.bin` extension) stores a compressed roaring bitmap and is efficient for dense deletions. Readers must filter rows whose offsets appear in the deletion file for the fragment.

Deletions can be materialized by rewriting data files with deleted rows removed. However, this invalidates row addresses and requires rebuilding indices, which can be expensive.

DeletionFile protobuf message
    
    
    message DeletionFile {
      // Type of deletion file, intended as a way to increase efficiency of the storage of deleted row
      // offsets. If there are sparsely deleted rows, then ARROW_ARRAY is the most efficient. If there
      // are densely deleted rows, then BITMAP is the most efficient.
      enum DeletionFileType {
        // A single Int32Array of deleted row offsets, stored as an Arrow IPC file with one batch and
        // one column. Has a .arrow extension.
        ARROW_ARRAY = 0;
        // A Roaring Bitmap of deleted row offsets. Has a .bin extension.
        BITMAP = 1;
      }
    
      // Type of deletion file.
      DeletionFileType file_type = 1;
      // The version of the dataset this deletion file was built from.
      uint64 read_version = 2;
      // An opaque id used to differentiate this file from others written by concurrent
      // writers.
      uint64 id = 3;
      // The number of rows that are marked as deleted.
      uint64 num_deleted_rows = 4;
      // The base path index of the deletion file. Used when the file is imported or referred from another
      // dataset. Lance uses it as key of the base_paths field in Manifest to determine the actual base
      // path of the deletion file.
      optional uint32 base_id = 7;
    
    }
    

## Related Specifications¶

### Storage Layout¶

File organization, base path system, and multi-location storage.

See [Storage Layout Specification](<layout/>)

### Transactions¶

MVCC, commit protocol, transaction types, and conflict resolution.

See [Transaction Specification](<transaction/>)

### Row Lineage¶

Row address, Stable row ID, row version tracking, and change data feed.

See [Row ID & Lineage Specification](<row_id_lineage/>)

### Indices¶

Vector indices, scalar indices, full-text search, and index management.

See [Index Specification](<index/>)

### Versioning¶

Feature flags and format version compatibility.

See [Format Versioning Specification](<versioning/>)

Back to top



---

<!-- Source: https://lance.org/format/table/schema/ -->

# Schema Format Specification¶

## Overview¶

The schema describes the structure of a Lance table, including all fields, their data types, and metadata. Schemas use a logical type system where data types are represented as strings that map to Apache Arrow data types. Each field in the schema has a unique identifier (field ID) that enables robust schema evolution and version tracking.

Note

Logical types are currently being simplified through discussion [#5864](<https://github.com/lance-format/lance/discussions/5864>). Proposed changes include consolidating encoding-specific variants (e.g., `large_string` and `string`, `large_binary` and `binary`) into single logical types with runtime optimization. Additionally, [#5817](<https://github.com/lance-format/lance/discussions/5817>) proposes adding `string_view` and `binary_view` types. This document describes the current implementation.

## Data Types¶

Lance supports a comprehensive set of data types that map to Apache Arrow types. Data types are represented as strings in the schema and can be grouped into several categories.

### Primitive Types¶

Logical Type | Arrow Type | Description  
---|---|---  
`null` | `Null` | Null type (no values)  
`bool` | `Boolean` | Boolean (true/false)  
`int8` | `Int8` | Signed 8-bit integer  
`uint8` | `UInt8` | Unsigned 8-bit integer  
`int16` | `Int16` | Signed 16-bit integer  
`uint16` | `UInt16` | Unsigned 16-bit integer  
`int32` | `Int32` | Signed 32-bit integer  
`uint32` | `UInt32` | Unsigned 32-bit integer  
`int64` | `Int64` | Signed 64-bit integer  
`uint64` | `UInt64` | Unsigned 64-bit integer  
  
### Floating Point Types¶

Logical Type | Arrow Type | Description  
---|---|---  
`halffloat` | `Float16` | IEEE 754 half-precision floating point (16-bit)  
`float` | `Float32` | IEEE 754 single-precision floating point (32-bit)  
`double` | `Float64` | IEEE 754 double-precision floating point (64-bit)  
  
### String and Binary Types¶

Logical Type | Arrow Type | Description  
---|---|---  
`string` | `Utf8` | Variable-length UTF-8 encoded string  
`binary` | `Binary` | Variable-length binary data  
`large_string` | `LargeUtf8` | Variable-length UTF-8 string (supports large offsets)  
`large_binary` | `LargeBinary` | Variable-length binary data (supports large offsets)  
  
### Decimal Types¶

Decimal types support arbitrary-precision numeric values. The format is: `decimal:<bit_width>:<precision>:<scale>`

Logical Type | Arrow Type | Precision | Example  
---|---|---|---  
`decimal:128:P:S` | `Decimal128` | Up to 38 digits | `decimal:128:10:2` (10 total digits, 2 after decimal)  
`decimal:256:P:S` | `Decimal256` | Up to 76 digits | `decimal:256:20:5`  
  
  * **Precision (P)** : Total number of digits (1-38 for Decimal128, up to 76 for Decimal256)
  * **Scale (S)** : Number of digits after the decimal point (0 ≤ S ≤ P)


### Date and Time Types¶

Logical Type | Arrow Type | Description  
---|---|---  
`date32:day` | `Date32` | Date (days since epoch)  
`date64:ms` | `Date64` | Date (milliseconds since epoch)  
`time32:s` | `Time32` | Time (seconds since midnight)  
`time32:ms` | `Time32` | Time (milliseconds since midnight)  
`time64:us` | `Time64` | Time (microseconds since midnight)  
`time64:ns` | `Time64` | Time (nanoseconds since midnight)  
`duration:s` | `Duration` | Duration (seconds)  
`duration:ms` | `Duration` | Duration (milliseconds)  
`duration:us` | `Duration` | Duration (microseconds)  
`duration:ns` | `Duration` | Duration (nanoseconds)  
  
### Timestamp Types¶

Timestamp types represent a point in time and may include timezone information. Format: `timestamp:<unit>:<timezone>`

  * **Unit** : `s` (seconds), `ms` (milliseconds), `us` (microseconds), `ns` (nanoseconds)
  * **Timezone** : IANA timezone string (e.g., `UTC`, `America/New_York`) or `-` for no timezone


Examples: \- `timestamp:us:UTC` \- Microsecond precision timestamp in UTC \- `timestamp:ms:America/New_York` \- Millisecond precision timestamp in America/New_York timezone \- `timestamp:ns:-` \- Nanosecond precision timestamp with no timezone

### Complex Types¶

#### Struct Type¶

A struct is a container for named fields with heterogeneous types.

Logical Type | Arrow Type | Description  
---|---|---  
`struct` | `Struct` | Composite type containing multiple named fields  
  
Struct fields are represented as child fields in the schema.

Example schema with a struct: 
    
    
    Field {
        name: "address"
        type: "struct"
        children: [
            Field { name: "street", type: "string" },
            Field { name: "city", type: "string" },
            Field { name: "zip", type: "int32" }
        ]
    }
    

#### List Types¶

Lists represent variable-length arrays of a single type.

Logical Type | Arrow Type | Description  
---|---|---  
`list` | `List` | Variable-length list of values  
`list.struct` | `List(Struct)` | Variable-length list of struct values  
`large_list` | `LargeList` | Variable-length list (supports large offsets)  
`large_list.struct` | `LargeList(Struct)` | Variable-length list of struct values (large offsets)  
  
The element type is specified as a child field.

#### Fixed-Size List Types¶

Fixed-size lists have a predetermined size known at schema definition time. Format: `fixed_size_list:<element_type>:<size>`

Logical Type | Description | Example  
---|---|---  
`fixed_size_list:float:128` | Fixed-size list of 128 floats | Vector embeddings (128-dimensional)  
`fixed_size_list:int32:10` | Fixed-size list of 10 integers |   
  
Special extension types: \- `fixed_size_list:lance.bfloat16:256` \- Fixed-size list of bfloat16 values

#### Fixed-Size Binary Type¶

Fixed-size binary data with a predetermined size in bytes. Format: `fixed_size_binary:<size>`

Logical Type | Description | Example  
---|---|---  
`fixed_size_binary:16` | Fixed-size binary of 16 bytes | MD5 hash  
`fixed_size_binary:32` | Fixed-size binary of 32 bytes | SHA-256 hash  
  
#### Dictionary Type¶

Dictionary-encoded data with separate keys and values. Format: `dict:<value_type>:<key_type>:<ordered>`

  * **Value type** : The type of dictionary values
  * **Key type** : The type used for dictionary indices (typically int8, int16, or int32)
  * **Ordered** : Boolean indicating if dictionary values are sorted (currently not fully supported)


Example: `dict:string:int16:false` \- Dictionary-encoded strings with int16 keys

#### Map Type¶

Key-value pairs stored in a structured format.

Logical Type | Arrow Type | Description  
---|---|---  
`map` | `Map` | Key-value pairs (currently supports unordered keys only)  
  
Maps have key and value types specified as child fields.

### Extension Types¶

Lance supports custom extension types that provide semantic meaning on top of Arrow types.

#### Blob Type¶

Represents large binary data stored externally.

Logical Type | Description  
---|---  
`blob` | Large binary data with external storage reference  
`json` | JSON-encoded data stored as binary  
  
Blob types are stored as large binary data with metadata describing storage location.

#### BFloat16 Type¶

Brain float (bfloat16) is a 16-bit floating point format optimized for ML. Used within fixed-size lists: `fixed_size_list:lance.bfloat16:SIZE`

## Field IDs¶

Field IDs are unique integer identifiers assigned to each field in a schema. They are essential for robust schema evolution, as they allow fields to be renamed or reordered without breaking references.

### Field ID Assignment¶

**Initial assignment (depth-first order):** When a table is created, field IDs are assigned to all fields in depth-first order, starting from 0.

Nested fields are linked via the `parent_id` field in the protobuf message. For example, if field "c" (id: 2) is a struct containing fields "x", "y", "z", those child fields will have `parent_id: 2`. Top-level fields have `parent_id: -1`.

Example with nested structure: 
    
    
    Field order: a, b, c.x, c.y, c.z, d
    
    Assigned IDs with parent relationships:
    - a: 0 (parent_id: -1)
    - b: 1 (parent_id: -1)
    - c: 2 (parent_id: -1, struct type)
    - c.x: 3 (parent_id: 2)
    - c.y: 4 (parent_id: 2)
    - c.z: 5 (parent_id: 2)
    - d: 6 (parent_id: -1)
    

Note: A `parent_id` of -1 indicates a top-level field. For nested fields, `parent_id` references the ID of the parent field. Child fields reference their parent via `parent_id` rather than being stored as separate "children" arrays in the protobuf message (though the Rust in-memory representation maintains a children vector for convenience).

**New field assignment (incremental):** When fields are added later (e.g., through schema evolution), they receive the next available ID incrementally. This preserves the history of field additions.

### Field ID Properties¶

  * **Immutable** : Once assigned, a field's ID never changes
  * **Unique** : Each field within a table has a unique ID
  * **Stable** : IDs are preserved across schema evolution operations
  * **Sparse** : Field IDs may not form a contiguous sequence after schema evolution


### Using Field IDs¶

When referencing fields internally within the format, use the field ids rather than field names or positions.

## Field Metadata¶

Fields can carry additional metadata as key-value pairs to configure encoding, primary key behavior, and other properties.

### Primary Key Metadata¶

Primary key configuration is handled by two protobuf fields in the Field message: \- **unenforced_primary_key** (bool): Whether this field is part of the primary key \- **unenforced_primary_key_position** (uint32): Position in primary key ordering (1-based for ordered, 0 for unordered)

For detailed discussion on primary key configuration, see [Unenforced Primary Key](<../#unenforced-primary-key>) in the table format overview.

### Encoding Metadata¶

Column encoding configurations are specified with the `lance-encoding:` prefix. See [File Format Encoding Specification](<../../file/encoding/>) for complete details on available encodings.

### Arrow Extension Type Metadata¶

Custom Arrow extension types may have metadata under the `ARROW:extension:` namespace (e.g., `ARROW:extension:name`).

## Schema Protobuf Definition¶

The schema is serialized using protobuf messages. Key messages include:

### Field Message¶
    
    
    message Field {
      enum Type {
        PARENT = 0;
        REPEATED = 1;
        LEAF = 2;
      }
      Type type = 1;
    
      // Fully qualified name.
      string name = 2;
      /// Field Id.
      ///
      /// See the comment in `DataFile.fields` for how field ids are assigned.
      int32 id = 3;
      /// Parent Field ID. If not set, this is a top-level column.
      int32 parent_id = 4;
    
      // Logical types, support parameterized Arrow Type.
      //
      // PARENT types will always have logical type "struct".
      //
      // REPEATED types may have logical types:
      // * "list"
      // * "large_list"
      // * "list.struct"
      // * "large_list.struct"
      // The final two are used if the list values are structs, and therefore the
      // field is both implicitly REPEATED and PARENT.
      //
      // LEAF types may have logical types:
      // * "null"
      // * "bool"
      // * "int8" / "uint8"
      // * "int16" / "uint16"
      // * "int32" / "uint32"
      // * "int64" / "uint64"
      // * "halffloat" / "float" / "double"
      // * "string" / "large_string"
      // * "binary" / "large_binary"
      // * "date32:day"
      // * "date64:ms"
      // * "decimal:128:{precision}:{scale}" / "decimal:256:{precision}:{scale}"
      // * "time:{unit}" / "timestamp:{unit}" / "duration:{unit}", where unit is
      // "s", "ms", "us", "ns"
      // * "dict:{value_type}:{index_type}:false"
      string logical_type = 5;
      // If this field is nullable.
      bool nullable = 6;
    
      // optional field metadata (e.g. extension type name/parameters)
      map<string, bytes> metadata = 10;  
    
      bool unenforced_primary_key = 12;
    
      // Position of this field in the primary key (1-based).
      // 0 means the field is part of the primary key but uses schema field id for ordering.
      // When set to a positive value, primary key fields are ordered by this position.
      uint32 unenforced_primary_key_position = 13;
    
      // DEPRECATED ----------------------------------------------------------------
    
      // Deprecated: Only used in V1 file format. V2 uses variable encodings defined
      // per page.
      //
      // The global encoding to use for this field.
      Encoding encoding = 7;
    
      // Deprecated: Only used in V1 file format. V2 dynamically chooses when to
      // do dictionary encoding and keeps the dictionary in the data files.
      //
      // The file offset for storing the dictionary value.
      // It is only valid if encoding is DICTIONARY.
      //
      // The logic type presents the value type of the column, i.e., string value.
      Dictionary dictionary = 8;
    
      // Deprecated: optional extension type name, use metadata field
      // ARROW:extension:name
      string extension_name = 9;
    
      // Field number 11 was previously `string storage_class`.
      // Keep it reserved so older manifests remain compatible while new writers
      // avoid reusing the slot.
      reserved 11;
      reserved "storage_class";
    
    }
    

The Field message contains: \- **id** : Unique field identifier (int32) \- **name** : Field name (string) \- **type** : Field type enum (PARENT, REPEATED, or LEAF) \- **logical_type** : Logical type string representation (string) - e.g., "int64", "struct", "list" \- **nullable** : Whether the field can be null (bool) \- **parent_id** : Parent field ID for nested fields; -1 for top-level fields (int32) \- **metadata** : Key-value pairs for additional configuration (map) \- **unenforced_primary_key** : Whether this field is part of the primary key (bool) \- **unenforced_primary_key_position** : Position in primary key ordering (uint32, 0 = unordered)

### Schema Message¶

The complete schema is represented as a collection of top-level fields plus metadata.

## Schema Evolution¶

Field IDs enable efficient schema evolution:

  * **Add Column** : Assign a new field ID and add to schema
  * **Drop Column** : Remove field from schema; its ID may be reused in some systems
  * **Rename Column** : Change field name; ID remains the same
  * **Reorder Columns** : Change field order in schema; IDs remain the same
  * **Type Evolution** : Data type can be changed. This might require rewriting the column in the data, depending on how the type was changed.


The use of field IDs ensures that data files can be correctly interpreted even as the schema changes over time.

## Example Schemas¶

The examples below use a simplified representation of the field structure. In the actual protobuf format, `type` refers to the field type enum (PARENT/REPEATED/LEAF) and `logical_type` contains the data type string representation.

### Simple Table¶
    
    
    Field {
        id: 0
        name: "id"
        logical_type: "int64"
        nullable: false
        parent_id: -1
    }
    Field {
        id: 1
        name: "name"
        logical_type: "string"
        nullable: true
        parent_id: -1
    }
    Field {
        id: 2
        name: "created_at"
        logical_type: "timestamp:us:UTC"
        nullable: true
        parent_id: -1
    }
    

### Nested Structure¶
    
    
    Field {
        id: 0
        name: "id"
        logical_type: "int64"
        nullable: false
        parent_id: -1  // Top-level field
    }
    Field {
        id: 1
        name: "user"
        logical_type: "struct"
        nullable: true
        parent_id: -1  // Top-level field
    }
    Field {
        id: 2
        name: "name"
        logical_type: "string"
        nullable: true
        parent_id: 1  // Nested under "user" struct (id: 1)
    }
    Field {
        id: 3
        name: "email"
        logical_type: "string"
        nullable: true
        parent_id: 1  // Nested under "user" struct (id: 1)
    }
    Field {
        id: 4
        name: "tags"
        logical_type: "list"
        nullable: true
        parent_id: -1  // Top-level field
    }
    Field {
        id: 5
        name: "item"
        logical_type: "string"
        nullable: true
        parent_id: 4  // Nested under "tags" list (id: 4)
    }
    

### With Vector Embeddings¶
    
    
    Field {
        id: 0
        name: "id"
        logical_type: "int64"
        nullable: false
        parent_id: -1  // Top-level field
        unenforced_primary_key: true
        unenforced_primary_key_position: 1  // Ordered position in primary key
    }
    Field {
        id: 1
        name: "text"
        logical_type: "string"
        nullable: true
        parent_id: -1  // Top-level field
    }
    Field {
        id: 2
        name: "embedding"
        logical_type: "fixed_size_list:lance.bfloat16:384"
        nullable: true
        parent_id: -1  // Top-level field
    }
    

## Type Conversion Reference¶

When converting between logical types and Arrow types, Lance uses the following mappings:

Arrow Type | Logical Type Format  
---|---  
`Arrow::Null` | `null`  
`Arrow::Boolean` | `bool`  
`Arrow::Int8` to `Int64` | `int8`, `int16`, `int32`, `int64`  
`Arrow::UInt8` to `UInt64` | `uint8`, `uint16`, `uint32`, `uint64`  
`Arrow::Float16` | `halffloat`  
`Arrow::Float32` | `float`  
`Arrow::Float64` | `double`  
`Arrow::Utf8` | `string`  
`Arrow::LargeUtf8` | `large_string`  
`Arrow::Binary` | `binary`  
`Arrow::LargeBinary` | `large_binary`  
`Arrow::Decimal128(p, s)` | `decimal:128:p:s`  
`Arrow::Decimal256(p, s)` | `decimal:256:p:s`  
`Arrow::Date32` | `date32:day`  
`Arrow::Date64` | `date64:ms`  
`Arrow::Time32(TimeUnit)` | `time32:s`, `time32:ms`  
`Arrow::Time64(TimeUnit)` | `time64:us`, `time64:ns`  
`Arrow::Timestamp(unit, tz)` | `timestamp:unit:tz`  
`Arrow::Duration(unit)` | `duration:s`, `duration:ms`, `duration:us`, `duration:ns`  
`Arrow::Struct` | `struct`  
`Arrow::List(Element)` | `list` or `list.struct` if element is Struct  
`Arrow::LargeList(Element)` | `large_list` or `large_list.struct`  
`Arrow::FixedSizeList(Element, Size)` | `fixed_size_list:type:size`  
`Arrow::FixedSizeBinary(Size)` | `fixed_size_binary:size`  
`Arrow::Dictionary(KeyType, ValueType)` | `dict:value_type:key_type:false`  
`Arrow::Map` | `map`  
  
Back to top



---

<!-- Source: https://lance.org/format/table/versioning/ -->

# Format Versioning¶

## Feature Flags¶

As the table format evolves, new feature flags are added to the format. There are two separate fields for checking for feature flags, depending on whether you are trying to read or write the table. Readers should check the `reader_feature_flags` to see if there are any flag it is not aware of. Writers should check `writer_feature_flags`. If either sees a flag they don't know, they should return an "unsupported" error on any read or write operation.

## Current Feature Flags¶

Flag Bit | Flag Name | Reader Required | Writer Required | Description  
---|---|---|---|---  
1 | `FLAG_DELETION_FILES` | Yes | Yes | Fragments may contain deletion files, which record the tombstones of soft-deleted rows.  
2 | `FLAG_STABLE_ROW_IDS` | Yes | Yes | Row IDs are stable for both moves and updates. Fragments contain an index mapping row IDs to row addresses.  
4 | `FLAG_USE_V2_FORMAT_DEPRECATED` | No | No | Files are written with the new v2 format. This flag is deprecated and no longer used.  
8 | `FLAG_TABLE_CONFIG` | No | Yes | Table config is present in the manifest.  
16 | `FLAG_BASE_PATHS` | Yes | Yes | Dataset uses multiple base paths (for shallow clones or multi-base datasets).  
  
Flags with bit values 32 and above are unknown and will cause implementations to reject the dataset with an "unsupported" error.

Back to top



---

<!-- Source: https://lance.org/format/table/transaction/ -->

# Transaction Specification¶

## Transaction Overview¶

Lance implements Multi-Version Concurrency Control (MVCC) to provide ACID transaction guarantees for concurrent readers and writers. Each commit creates a new immutable table version through atomic storage operations. All table versions form a serializable history, enabling features such as time travel and schema evolution.

Transactions are the fundamental unit of change in Lance. A transaction describes a set of modifications to be applied atomically to create a new table version. The transaction model supports concurrent writes through optimistic concurrency control with automatic conflict resolution.

## Commit Protocol¶

### Storage Primitives¶

Lance commits rely on atomic write operations provided by the underlying object store:

  * **rename-if-not-exists** : Atomically rename a file only if the target does not exist
  * **put-if-not-exists** : Atomically write a file only if it does not already exist (also known as PUT-IF-NONE-MATCH or conditional PUT)


These primitives guarantee that exactly one writer succeeds when multiple writers attempt to create the same manifest file concurrently.

### Manifest Naming Schemes¶

Lance supports two manifest naming schemes:

  * **V1** : `{version}.manifest` \- Monotonically increasing version numbers (e.g., `1.manifest`, `2.manifest`)
  * **V2** : `{u64::MAX - version:020}.manifest` \- Reverse-sorted lexicographic ordering (e.g., `18446744073709551614.manifest` for version 1)


The V2 scheme enables efficient discovery of the latest version through lexicographic object listing.

### Transaction Files¶

Transaction files store the serialized transaction protobuf message for each commit attempt. These files serve two purposes:

  1. Enable manifest reconstruction during commit retries when concurrent transactions have been committed
  2. Support conflict detection by describing the operation performed


### Commit Algorithm¶

The commit process attempts to atomically write a new manifest file using the storage primitives described above. When concurrent writers conflict, the system loads transaction files to detect conflicts and attempts to rebase the transaction if possible. If the atomic commit fails, the process retries with updated transaction state. For detailed conflict detection and resolution mechanisms, see the Conflict Resolution section.

## Transaction Types¶

The authoritative specification for transaction types is defined in [`protos/transaction.proto`](<https://github.com/lancedb/lance/blob/main/protos/transaction.proto>).

Each transaction contains a `read_version` field indicating the table version from which the transaction was built, a `uuid` field uniquely identifying the transaction, and an `operation` field specifying one of the following transaction types:

In the following section, we will describe each transaction type and its compatibility with other transaction types. This compatibility is not always bi-directional. We are describing it from the perspective of the operation being committed. For example, we say that an Append is not compatible with an Overwrite which means that if we are trying to commit an Append, and an Overwrite has already been committed (since we started the Append), then the Append will fail. On the other hand, when describing the Overwrite operation, we say that it does not conflict with Append. This is because, if we are trying to commit an Overwrite, and an Append operation has occurred in the meantime, we still allow the Overwrite to proceed.

### Append¶

Adds new fragments to the table without modifying existing data. Fragment IDs are not assigned at transaction creation time; they are assigned during manifest construction.

Append protobuf message
    
    
    message Append {
      // The new fragments to append.
      //
      // Fragment IDs are not yet assigned.
      repeated DataFragment fragments = 1;
    }
    

#### Append Compatibility¶

The append operation is one of the most common operations and is designed to be compatible with most other operations, even itself. This is to ensure that multiple writers can append without worry about conflicts. These are the operations that conflict with append:

  * Overwrite
  * Restore
  * UpdateMemWalState


### Delete¶

Marks rows as deleted using deletion vectors. May update fragments (adding deletion vectors) or delete entire fragments. The `predicate` field stores the deletion condition, enabling conflict detection with concurrent transactions.

Delete protobuf message
    
    
    message Delete {
      // The fragments to update
      //
      // The fragment IDs will match existing fragments in the dataset.
      repeated DataFragment updated_fragments = 1;
      // The fragments to delete entirely.
      repeated uint64 deleted_fragment_ids = 2;
      // The predicate that was evaluated
      //
      // This may be used to determine whether the delete would have affected 
      // files written by a concurrent transaction.
      string predicate = 3;
    }
    

#### Delete Compatibility¶

Delete modifies an existing fragment, so there may be conflicts with other operations on overlapping fragments. Generally these conflicts are rebaseable or retryable.

These are the operations that conflict with delete:

  * Overwrite
  * Restore
  * UpdateMemWalState


These operations conflict with delete but can be retried:

  * Merge (only if there are overlapping fragments)
  * Rewrite (only if there are overlapping fragments)
  * DataReplacement (only if there are overlapping fragments)


These operations conflict with delete but can potentially be rebased. The deletion masks from the two operations will be merged. However, if both operations modified the same rows, then the conflict becomes a retryable conflict.

  * Delete
  * Update


### Overwrite¶

Creates or completely overwrites the table with new data, schema, and configuration.

Overwrite protobuf message
    
    
    message Overwrite {
      // The new fragments
      //
      // Fragment IDs are not yet assigned.
      repeated DataFragment fragments = 1;
      // The new schema
      repeated lance.file.Field schema = 2;
      // Schema metadata.
      map<string, bytes> schema_metadata = 3;
      // Key-value pairs to merge with existing config.
      map<string, string> config_upsert_values = 4;
      // The base paths to be added for the initial dataset creation
      repeated BasePath initial_bases = 5;
    }
    

#### Overwrite Compatibility¶

An overwrite operation completely overwrites the table. Generally, we do not care what has happened since the read version.

However, the overwrite does not necessarily rewrite the table config. As a result, we consider the following to be retryable conflicts:

  * UpdateConfig (only if the two operations modify the same config key)
  * Overwrite (always)
  * UpdateMemWalState (always)


### CreateIndex¶

Adds, replaces, or removes secondary indices (vector indices, scalar indices, full-text search indices).

CreateIndex protobuf message
    
    
    message CreateIndex {
      repeated IndexMetadata new_indices = 1;
      repeated IndexMetadata removed_indices = 2;
    }
    

#### CreateIndex Compatibility¶

Indexes record which fragments are covered by the index and we don't require all fragments be covered. As a result, it is typically ok for an index to be created concurrently with the addition of new fragments. These new fragments will simply be unindexed.

Updates and deletes are also compatible with index creation. This is because it is ok for an index to refer to deleted rows. Those results will be filtered out after the index search. If an update occurs then the old value will be filtered out and the new value will be considered part of the unindexed set.

If two CreateIndex operations are committed concurrently then it is allowed. If the indexes have different names this is no problem. If the indexes have the same name then the second operation will win and replace the first.

These operations conflict with index creation:

  * Overwrite
  * Restore
  * UpdateMemWalState


Data replacement operations will conflict with index creation if the column being replaced is being indexed. Rewrite operations will conflict with index creation if the rewritten fragments are covered by the index. This is because an index refers to row addresses and the rewrite operation changes the row addresses. However, if a fragment reuse index is being used, or if the stable row ids feature is enable, then the rewrite operation is compatible with index creation. As a result, these are the operations that are retryable conflicts with index creation:

  * Rewrite (only if overlapping fragments, no stable row ids, and no fragment reuse index)
  * DataReplacement (only if overlapping fragments and the column being replaced is being indexed)


Some indices are special singleton indices. For example, the fragment reuse index and the mem wal index. If a conflict occurs between two operations that are modifying the same singleton index, then we must rebase the operation and merge the indexes. As a result, these are the operations that are rebaseable conflicts with index creation:

  * CreateIndex (only if both operations are modifying the same singleton index)


### Rewrite¶

Reorganizes data without semantic modification. This includes operations such as compaction, defragmentation, and re-ordering. Rewrite operations change row addresses, requiring index updates. New fragment IDs must be reserved via `ReserveFragments` before executing a `Rewrite` transaction.

Rewrite protobuf message
    
    
    message Rewrite {
      // The old fragments that are being replaced
      //
      // DEPRECATED: use groups instead.
      //
      // These should all have existing fragment IDs.
      repeated DataFragment old_fragments = 1;
      // The new fragments
      //
      // DEPRECATED: use groups instead.
      //
      // These fragments IDs are not yet assigned.
      repeated DataFragment new_fragments = 2;
    
      // During a rewrite an index may be rewritten.  We only serialize the UUID
      // since a rewrite should not change the other index parameters.
      message RewrittenIndex {
        // The id of the index that will be replaced
        UUID old_id = 1;
        // the id of the new index
        UUID new_id = 2;
        // the new index details
        google.protobuf.Any new_index_details = 3;
        // the version of the new index
        uint32 new_index_version = 4;
        // Files in the new index with their sizes.
        // Empty if file sizes are not available (e.g. older writers).
        repeated IndexFile new_index_files = 5;
      }
    
      // A group of rewrite files that are all part of the same rewrite.
      message RewriteGroup {
        // The old fragment that is being replaced
        //
        // This should have an existing fragment ID.
        repeated DataFragment old_fragments = 1;
        // The new fragment
        //
        // The ID should have been reserved by an earlier
        // reserve operation
        repeated DataFragment new_fragments = 2;
      }
    
      // Groups of files that have been rewritten
      repeated RewriteGroup groups = 3;
      // Indices that have been rewritten
      repeated RewrittenIndex rewritten_indices = 4;
    }
    

#### Rewrite Compatibility¶

Rewrite operations do not change data but they can materialize deletions and they do replace fragments. As a result, they can potentially conflict with other operations that modify the fragments being rewritten.

These are the operations that conflict with rewrite:

  * Overwrite
  * Restore


Rewrite is not compatible with CreateIndex by default because the operation will change the row addresses that the CreateIndex refers to. However, a fragment reuse index or the stable row ids feature can allow these operations to be compatible.

Several operations modify existing fragments. As a result, they can potentially conflict with Rewrite if they modify the same fragments. However, Merge is overly general and so no conflict detection is possible. As a result, here are the operations that are retryable conflicts with Rewrite:

  * Merge (always)
  * DataReplacement (only if overlapping fragments)
  * Delete (only if overlapping fragments)
  * Update (only if overlapping fragments)
  * Rewrite (if overlapping fragments or both carry a fragment reuse index)
  * CreateIndex (overlapping fragments and no fragment reuse index or stable row ids)


There is one case where a Rewrite will rebase. This is when the Rewrite operation has a fragment reuse index and there is a CreateIndex operation that is writing the fragment reuse index. In this case the Rewrite will rebase and update its fragment reuse index to include the conflicting fragment reuse index.

As a result, these are the operations that are rebaseable conflicts with Rewrite:

  * CreateIndex (if the CreateIndex is writing the fragment reuse index and the Rewrite is carrying a fragment reuse index)


### Merge¶

Adds new columns to the table, modifying the schema. All fragments must be updated to include the new columns.

Merge protobuf message
    
    
    message Merge {
      // The updated fragments
      //
      // These should all have existing fragment IDs.
      repeated DataFragment fragments = 1;
      // The new schema
      repeated lance.file.Field schema = 2;
      // Schema metadata.
      map<string, bytes> schema_metadata = 3;
    }
    

#### Overly General Operation¶

The Merge operation is a very generic operation. The set of fragments provided in the operation will be the final set of fragments in the resulting dataset. As a result, it has a high potential for conflicts with other operations. If possible, more restrictive operations such as Rewrite, DataReplacement, or Append should be preferred over Merge.

#### Merge Compatibility¶

As mentioned above, Merge is a very generic operation, as a result it has a high potential for conflicts with other operations. The following operations conflict with Merge:

  * Overwrite
  * Restore
  * UpdateMemWalState
  * Project


These operations are retryable conflicts with Merge:

  * Update (always)
  * Append (always)
  * Delete (always)
  * Merge (always)
  * Rewrite (always)
  * DataReplacement (always)


### Project¶

Removes columns from the table, modifying the schema. This is a metadata-only operation; data files are not modified.

Project protobuf message
    
    
    message Project {
      // The new schema
      repeated lance.file.Field schema = 1;
    }
    

#### Project Compatibility¶

Since project only modifies the schema, it is compatible with most other operations. However, it is not compatible with Merge because the Merge operation modifies the schema (can potentially add columns) and the logic to rebase those changes does not currently exist (project is cheap and easy enough to retry).

These are the operations that conflict with Project:

  * Overwrite
  * Restore
  * UpdateMemWalState


The following operations are retryable conflicts with Project:

  * Project (always)
  * Merge (always)


### Restore¶

Reverts the table to a previous version.

Restore protobuf message
    
    
    message Restore {
      // The version to restore to
      uint64 version = 1;
    }
    

#### Restore Compatibility¶

The Restore operation reverts the table to a previous version. It's generally assumed this trumps any other operation. Here are the operations that conflict with Restore:

  * UpdateMemWalState


### ReserveFragments¶

Pre-allocates fragment IDs for use in future `Rewrite` operations. This allows rewrite operations to reference fragment IDs before the rewrite transaction is committed.

ReserveFragments protobuf message
    
    
    message ReserveFragments {
      uint32 num_fragments = 1;
    }
    

#### ReserveFragments Compatibility¶

The ReserveFragments operation is fairly trivial. The only thing it changes is the max fragment id. So this only conflicts with operations that modify the max fragment id. Here are the operations that conflict with ReserveFragments:

  * Overwrite
  * Restore


### Clone¶

Creates a shallow or deep copy of the table. Shallow clones are metadata-only copies that reference original data files through `base_paths`. Deep clones are full copies using object storage native copy operations (e.g., S3 CopyObject).

Clone protobuf message
    
    
    message Clone {
      // - true:  Performs a metadata-only clone (copies manifest without data files).
      //          The cloned dataset references original data through `base_paths`,
      //          suitable for experimental scenarios or rapid metadata migration.
      // - false: Performs a full deep clone using the underlying object storage's native
      //          copy API (e.g., S3 CopyObject, GCS rewrite). This leverages server-side
      //          bulk copy operations to bypass download/upload bottlenecks, achieving
      //          near-linear speedup for large datasets (typically 3-10x faster than
      //          manual file transfers). The operation maintains atomicity and data
      //          integrity guarantees provided by the storage backend.
      bool is_shallow = 1;
      // the reference name in the source dataset
      // in most cases it should be the branch or tag name in the source dataset
      optional string ref_name = 2;
      // the version of the source dataset for cloning
      uint64 ref_version = 3;
      // the absolute base path of the source dataset for cloning
      string ref_path = 4;
      // if the target dataset is a branch, this is the branch name of the target dataset
      optional string branch_name = 5;
    }
    

#### Clone Compatibility¶

The Clone operation can only be the first operation in a dataset. If there is an existing dataset, then the Clone operation will fail. As a result, there is no such thing as a conflict with Clone.

### Update¶

Modifies row values without adding or removing rows. Supports two execution modes: REWRITE_ROWS deletes rows in current fragments and rewrites them in new fragments, which is optimal when the majority of columns are modified or only a small number of rows are affected; REWRITE_COLUMNS fully rewrites affected columns within fragments by tombstoning old column versions, which is optimal when most rows are affected but only a subset of columns are modified.

Update protobuf message
    
    
    message Update {
      // The fragments that have been removed. These are fragments where all rows
      // have been updated and moved to a new fragment.
      repeated uint64 removed_fragment_ids = 1;
      // The fragments that have been updated.
      repeated DataFragment updated_fragments = 2;
      // The new fragments where updated rows have been moved to.
      repeated DataFragment new_fragments = 3;
      // The ids of the fields that have been modified.
      repeated uint32 fields_modified = 4;
      /// List of MemWAL region generations to mark as merged after this transaction
      repeated MergedGeneration merged_generations = 5;
      /// The fields that used to judge whether to preserve the new frag's id into
      /// the frag bitmap of the specified indices.
      repeated uint32 fields_for_preserving_frag_bitmap = 6;
      // The mode of update
      UpdateMode update_mode = 7;
      // Filter for checking existence of keys in newly inserted rows, used for conflict detection.
      // Only tracks keys from INSERT operations during merge insert, not updates.
      optional KeyExistenceFilter inserted_rows = 8;
    }
    

#### Update Compatibility¶

Here are the operations that conflict with Update:

  * Overwrite
  * Restore


An update operation is both a delete and an append operation. Like a Delete operation, it will modify fragments to change the deletion mask. As a result, there will be a retryable conflict with other operations that modify the same fragments. Here are the operations that are retryable conflicts with Update:

  * Rewrite (only if overlapping fragments)
  * DataReplacement (only if overlapping fragments)
  * Merge (always)


Similar to Delete, the Update operation can rebase other modifications to the deletion mask. Here are the operations that are rebaseable conflicts with Update:

  * Delete
  * Update


### UpdateConfig¶

Modifies table configuration, table metadata, schema metadata, or field metadata without changing data.

UpdateConfig protobuf message
    
    
    message UpdateConfig {
      UpdateMap config_updates = 6;
      UpdateMap table_metadata_updates = 7;
      UpdateMap schema_metadata_updates = 8;
      map<int32, UpdateMap> field_metadata_updates = 9;
    
      // Deprecated -------------------------------
      map<string, string> upsert_values = 1;
      repeated string delete_keys = 2;
      map<string, string> schema_metadata = 3;
      map<uint32, FieldMetadataUpdate> field_metadata = 4;
    
      message FieldMetadataUpdate {
        map<string, string> metadata = 5;
      }
    }
    

#### UpdateConfig Compatibility¶

An UpdateConfig operation only modifies table config and tends to be compatible with other operations. Here are the operations that conflict with UpdateConfig:

  * Overwrite
  * UpdateConfig (only if the two operations modify the same config)


### DataReplacement¶

Replaces data in specific column regions with new data files.

DataReplacement protobuf message
    
    
    message DataReplacement {
      repeated DataReplacementGroup replacements = 1;
    }
    

#### DataReplacement Compatibility¶

A DataReplacement operation only replaces a single column's worth of data. As a result, it can be safer and simpler than Merge or Update operations. Here are the operations that conflict with DataReplacement:

  * Overwrite
  * Restore
  * UpdateMemWalState


The following operations are retryable conflicts with DataReplacement:

  * DataReplacement (only if same field and overlapping fragments)
  * CreateIndex (only if the field being replaced is being indexed)
  * Rewrite (only if overlapping fragments)
  * Update (only if overlapping fragments)
  * Merge (always)


### UpdateMemWalState¶

Updates the state of MemWal indices (write-ahead log based indices).

UpdateMemWalState protobuf message
    
    
    message UpdateMemWalState {
      // Regions and generations being marked as merged.
      repeated MergedGeneration merged_generations = 1;
    }
    

### UpdateBases¶

Adds new base paths to the table, enabling reference to data files in additional locations.

UpdateBases protobuf message
    
    
    message UpdateBases {
      // The new base paths to add to the manifest.
      repeated BasePath new_bases = 1;
    }
    

#### UpdateBases Compatibility¶

An UpdateBases operation only modifies the base paths. As a result, it only conflicts with other UpdateBases operations and even then only conflicts if the two operations have base paths with the same id, name, or path.

## Conflict Resolution¶

### Terminology¶

When concurrent transactions attempt to commit against the same read version, Lance employs conflict resolution to determine whether the transactions can coexist. Three outcomes are possible:

  * **Rebasable** : The transaction can be modified to incorporate concurrent changes while preserving its semantic intent. The transaction is transformed to account for the concurrent modification, then the commit is retried automatically within the commit layer.

  * **Retryable** : The transaction cannot be rebased, but the operation can be re-executed at the application level with updated data. The implementation returns a retryable conflict error, signaling that the application should re-read the data and retry the operation. The retried operation is expected to produce semantically equivalent results.

  * **Incompatible** : The transactions conflict in a fundamental way where retrying would violate the operation's assumptions or produce semantically different results than expected. The commit fails with a non-retryable error. Callers should proceed with extreme caution if they decide to retry, as the transaction may produce different output than originally intended.


### Rebase Mechanism¶

The `TransactionRebase` structure tracks the state necessary to rebase a transaction against concurrent commits:

  1. **Fragment tracking** : Maintains a map of fragments as they existed at the transaction's read version, marking which require rewriting
  2. **Modification detection** : Tracks the set of fragment IDs that have been modified or deleted
  3. **Affected rows** : For Delete and Update operations, stores the specific rows affected by the operation for fine-grained conflict detection
  4. **Fragment reuse indices** : Accumulates fragment reuse index metadata from concurrent Rewrite operations


When a concurrent transaction is detected, the rebase process:

  1. Compares fragment modifications to determine if there is overlap
  2. For Delete/Update operations, compares `affected_rows` to detect whether the same rows were modified
  3. Merges deletion vectors when both transactions delete rows from the same fragment
  4. Accumulates fragment reuse index updates when concurrent Rewrites change fragment IDs
  5. Modifies the transaction if rebasable, or returns a retryable/incompatible conflict error


### Conflict Scenarios¶

#### Rebasable Conflict Example¶

The following diagram illustrates a rebasable conflict where two Delete operations modify different rows in the same fragment:
    
    
    gitGraph
        commit id: "v1"
        commit id: "v2"
        branch writer-a
        branch writer-b
        checkout writer-a
        commit id: "Delete rows 100-199" tag: "read_version=2"
        checkout writer-b
        commit id: "Delete rows 500-599" tag: "read_version=2"
        checkout main
        merge writer-a tag: "v3"
        checkout writer-b
        commit id: "Rebase: merge deletion vectors" type: HIGHLIGHT
        checkout main
        merge writer-b tag: "v4"

In this scenario:

  * Writer A deletes rows 100-199 and successfully commits version 3
  * Writer B attempts to commit but detects version 3 exists
  * Writer B's transaction is rebasable because it only modified deletion vectors (not data files) and `affected_rows` do not overlap
  * Writer B rebases by merging Writer A's deletion vector with its own, write it to storage
  * Writer B successfully commits version 4


#### Retryable Conflict Example¶

The following diagram illustrates a retryable conflict where an Update operation encounters a concurrent Rewrite (compaction) that prevents automatic rebasing:
    
    
    gitGraph
        commit id: "v1"
        commit id: "v2"
        branch writer-a
        branch writer-b
        checkout writer-a
        commit id: "Compact fragments 1-5" tag: "read_version=2"
        checkout writer-b
        commit id: "Update rows in fragment 3" tag: "read_version=2"
        checkout main
        merge writer-a tag: "v3: fragments compacted"
        checkout writer-b
        commit id: "Detect conflict: cannot rebase" type: REVERSE

In this scenario:

  * Writer A compacts fragments 1-5 into a single fragment and successfully commits version 3
  * Writer B attempts to update rows in fragment 3 but detects version 3 exists
  * Writer B's Update transaction is retryable but not rebasable: fragment 3 no longer exists after compaction
  * The commit layer returns a retryable conflict error
  * The application must re-execute the Update operation against version 3, locating the rows in the new compacted fragment


#### Incompatible Conflict Example¶

The following diagram illustrates an incompatible conflict where a Delete operation encounters a concurrent Restore that fundamentally invalidates the operation:
    
    
    gitGraph
        commit id: "v1"
        commit id: "v2"
        commit id: "v3"
        branch writer-a
        branch writer-b
        checkout writer-a
        commit id: "Restore to v1" tag: "read_version=3"
        checkout writer-b
        commit id: "Delete rows added in v2-v3" tag: "read_version=3"
        checkout main
        merge writer-a tag: "v4: restored to v1"
        checkout writer-b
        commit id: "Detect conflict: incompatible" type: REVERSE

In this scenario:

  * Writer A restores the table to version 1 and successfully commits version 4
  * Writer B attempts to delete rows that were added between versions 2 and 3
  * Writer B's Delete transaction is incompatible: the table has been restored to version 1, and the rows it intended to delete no longer exist
  * The commit fails with a non-retryable error
  * If the caller retries the deletion operation against version 4, it would either delete nothing (if those rows don't exist in v1) or delete different rows (if similar row IDs exist in v1), producing semantically different results than originally intended


## External Manifest Store¶

If the backing object store does not support atomic operations (rename-if-not-exists or put-if-not-exists), an external manifest store can be used to enable concurrent writers.

An external manifest store is a key-value store that supports put-if-not-exists operations. The external manifest store supplements but does not replace the manifests in object storage. A reader unaware of the external manifest store can still read the table, but may observe a version up to one commit behind the true latest version.

### Commit Process with External Store¶

The commit process follows a four-step protocol:

  1. **Stage manifest** : `PUT_OBJECT_STORE {dataset}/_versions/{version}.manifest-{uuid}`
  2. Write the new manifest to object storage under a unique path determined by a new UUID
  3. This staged manifest is not yet visible to readers

  4. **Commit to external store** : `PUT_EXTERNAL_STORE base_uri, version, {dataset}/_versions/{version}.manifest-{uuid}`

  5. Atomically commit the path of the staged manifest to the external store using put-if-not-exists
  6. The commit is effectively complete after this step
  7. If this operation fails due to conflict, another writer has committed this version

  8. **Finalize in object store** : `COPY_OBJECT_STORE {dataset}/_versions/{version}.manifest-{uuid} → {dataset}/_versions/{version}.manifest`

  9. Copy the staged manifest to the final path
  10. This makes the manifest discoverable by readers unaware of the external store

  11. **Update external store pointer** : `PUT_EXTERNAL_STORE base_uri, version, {dataset}/_versions/{version}.manifest`

  12. Update the external store to point to the finalized manifest path
  13. Completes the synchronization between external store and object storage


**Fault Tolerance:**

If the writer fails after step 2 but before step 4, the external store and object store are temporarily out of sync. Readers detect this condition and attempt to complete the synchronization. If synchronization fails, the reader refuses to load to ensure dataset portability.

### Reader Process with External Store¶

The reader follows a validation and synchronization protocol:

  1. **Query external store** : `GET_EXTERNAL_STORE base_uri, version` → `path`
  2. Retrieve the manifest path for the requested version
  3. If the path does not end with a UUID, return it directly (synchronization complete)
  4. If the path ends with a UUID, synchronization is required

  5. **Synchronize to object store** : `COPY_OBJECT_STORE {dataset}/_versions/{version}.manifest-{uuid} → {dataset}/_versions/{version}.manifest`

  6. Attempt to finalize the staged manifest
  7. This operation is idempotent

  8. **Update external store** : `PUT_EXTERNAL_STORE base_uri, version, {dataset}/_versions/{version}.manifest`

  9. Update the external store to reflect the finalized path
  10. Future readers will see the synchronized state

  11. **Return finalized path** : Return `{dataset}/_versions/{version}.manifest`

  12. Always return the finalized path
  13. If synchronization fails, return an error to prevent reading inconsistent state


This protocol ensures that datasets using external manifest stores remain portable: copying the dataset directory preserves all data without requiring the external store.

Back to top



---

<!-- Source: https://lance.org/format/table/layout/ -->

# Storage Layout Specification¶

## Overview¶

This specification defines how Lance datasets are organized on object storage. The layout design emphasizes portability, allowing datasets to be relocated or referenced across multiple storage systems with minimal metadata changes.

## Dataset Root¶

The dataset root is the location where the dataset was initially created. Every Lance dataset has exactly one dataset root, which serves as the primary storage location for the dataset's files. The dataset root contains the standard subdirectory structure (`data/`, `_versions/`, `_deletions/`, `_indices/`, `_refs/`, `tree/`) that organizes the dataset's files.

## Basic Layout¶

A Lance dataset in its basic form stores all files within the dataset root directory structure:
    
    
    {dataset_root}/
        data/
            *.lance           -- Data files containing column data
        _versions/
            *.manifest        -- Manifest files (one per version)
        _transactions/
            *.txn             -- Transaction files for commit coordination
        _deletions/
            *.arrow           -- Deletion vector files (arrow format)
            *.bin             -- Deletion vector files (bitmap format)
        _indices/
            {UUID}/
                ...           -- Index content (different for each index type)
        _refs/
            tags/
                *.json        -- Tag metadata
            branches/
                *.json        -- Branch metadata
        tree/
            {branch_name}/
                ...           -- Branch dataset
    

## Base Path System¶

### BasePath Message¶

The manifest's `base_paths` field contains an array of `BasePath` entries that define alternative storage locations for dataset files. Each base path entry has a unique numeric identifier that file metadata can reference to indicate where files are located. The `path` field specifies an absolute path interpretable by the object store. The `is_dataset_root` field determines how the path is interpreted: when true, the path points to a dataset root with standard subdirectories (`data/`, `_deletions/`, `_indices/`); when false, the path points directly to a file directory without subdirectories. An optional `name` field provides a human-readable alias, which is particularly useful for referencing tags in shallow clones.

BasePath protobuf message
    
    
    message BasePath {
      uint32 id = 1;
      optional string name = 2;
      bool is_dataset_root = 3;
      string path = 4;
    }
    

### File Metadata Base References¶

Three types of files can specify alternative base paths: data files, deletion files, and index metadata. Each of these file types includes an optional `base_id` field in their metadata that references a base path entry by its numeric identifier. When a file's `base_id` is absent, the file is located relative to the dataset root. When a file's `base_id` is present, readers must look up the corresponding base path entry in the manifest's `base_paths` array to determine where the file is stored.

At read time, path resolution follows a two-step process. First, the reader determines the base path: if `base_id` is absent, the base path is the dataset root; otherwise, the reader looks up the base path entry using the `base_id` to obtain the path and its `is_dataset_root` flag. Second, the reader constructs the full file path based on whether the base path represents a dataset root. For dataset roots (when `is_dataset_root` is true), the full path includes standard subdirectories: data files are located under `data/`, deletion files under `_deletions/`, and indices under `_indices/`. For non-root base paths (when `is_dataset_root` is false), the base path points directly to the file directory, and the file path is appended directly without subdirectory prefixes.

### Example Complex Layout Scenarios¶

#### Hot/Cold Tiering¶
    
    
    Manifest base_paths:
    [
      { id: 0, is_dataset_root: true, path: "s3://hot-bucket/dataset" },
      { id: 1, is_dataset_root: true, path: "s3://cold-bucket/dataset-archive" }
    ]
    
    Fragment 0 (recent data):
      DataFile { path: "fragment-0.lance", base_id: 0 }
      → resolves to: s3://hot-bucket/dataset/data/fragment-0.lance
    
    Fragment 100 (historical data):
      DataFile { path: "fragment-100.lance", base_id: 1 }
      → resolves to: s3://cold-bucket/dataset-archive/data/fragment-100.lance
    

This allows seamless querying across storage tiers without data movement.

#### Multi-Region Distribution¶
    
    
    Manifest base_paths:
    [
      { id: 0, is_dataset_root: true, path: "s3://us-east-bucket/dataset" },
      { id: 1, is_dataset_root: true, path: "s3://eu-west-bucket/dataset" },
      { id: 2, is_dataset_root: true, path: "s3://ap-south-bucket/dataset" }
    ]
    
    Fragments distributed by data locality:
      Fragment 0 (US users): base_id: 0
      Fragment 1 (EU users): base_id: 1
      Fragment 2 (Asia users): base_id: 2
    

Compute jobs can read data from the nearest region without data transfer.

#### Shallow Clone¶

Shallow clones create a new dataset that references data files from a source dataset without copying:

**Example: Shallow Clone**
    
    
    Source dataset: s3://production/main-dataset
    Clone dataset:  s3://experiments/test-variant
    
    Clone manifest base_paths:
    [
      { id: 0, is_dataset_root: true, path: "s3://experiments/test-variant" },
      { id: 1, is_dataset_root: true, path: "s3://production/main-dataset",
        name: "v1.0" }
    ]
    
    Original fragments (inherited):
      DataFile { path: "fragment-0.lance", base_id: 1 }
      → resolves to: s3://production/main-dataset/data/fragment-0.lance
    
    New fragments (clone-specific):
      DataFile { path: "fragment-new.lance", base_id: 0 }
      → resolves to: s3://experiments/test-variant/data/fragment-new.lance
    

The clone can append new data, modify schemas, or delete rows without affecting the source dataset. Only the manifest and new data files are stored in the clone location.

**Workflow:**

  1. [Clone transaction](<../transaction/#clone>) creates new manifest in target location
  2. Manifest includes base path pointing to source dataset
  3. Original fragments reference source via `base_id: 1`
  4. Subsequent writes reference clone location via `base_id: 0`
  5. Source dataset remains immutable and can be garbage collected independently


## Dataset Portability¶

The base path system combined with relative file references provides strong portability guarantees for Lance datasets. All file paths within Lance files are stored relative to their containing directory, enabling datasets to be relocated without file modifications.

To port a dataset to a new location, simply copy all contents from the dataset root directory. The copied dataset will function immediately at the new location without any manifest updates, as all file references within the dataset root resolve through relative paths.

When a dataset uses multiple base paths (such as in shallow clones or multi-bucket configurations), users have flexibility in how to port the dataset. The simplest approach is to copy only the dataset root, which preserves references to the original base path locations. Alternatively, users can copy additional base paths to the new location and update the manifest's `base_paths` array to reflect the new base paths. Since only the `base_paths` field in the manifest requires modification, this remains a lightweight metadata operation that does not require rewriting additional metadata or data files.

## File Naming Conventions¶

### Data Files¶

Pattern: `data/{uuid-based-filename}.lance`

Data files use UUID-based filenames optimized for S3 throughput. The filename is generated from a UUID (16 bytes) by converting the first 3 bytes to a 24-character binary string and the remaining 13 bytes to a 26-character hex string, resulting in a 50-character filename. The binary prefix (rather than hex) provides maximum entropy per character, allowing S3's internal partitioning to quickly recognize access patterns and scale appropriately, minimizing throttling.

Example: `data/101100101101010011010110a1b2c3d4e5f6g7h8i9j0.lance`

### Deletion Files¶

Pattern: `_deletions/{fragment_id}-{read_version}-{id}.{extension}`

Deletion files use two extensions: `.arrow` for Arrow IPC format (sparse deletions) and `.bin` for Roaring bitmap format (dense deletions).

Example: `_deletions/42-10-a1b2c3d4.arrow`

### Transaction Files¶

Pattern: `_transactions/{read_version}-{uuid}.txn`

Where `read_version` is the table version the transaction was built from.

Example: `_transactions/5-550e8400-e29b-41d4-a716-446655440000.txn`

### Manifest Files¶

Manifest files are stored in the `_versions/` directory with naming schemes that support atomic commits.

See [Manifest Naming Schemes](<../transaction/#manifest-naming-schemes>) for details on the V1 and V2 patterns and their implications for version discovery.

Back to top



---

<!-- Source: https://lance.org/format/table/branch_tag/ -->

# Branch and Tag Specification¶

## Overview¶

Lance supports branching and tagging for managing multiple independent version histories and creating named references to specific versions. Branches enable parallel development workflows, while tags provide stable named references for important versions.

## Branching¶

### Branch Name¶

Branch names must follow these validation rules:

  1. Cannot be empty
  2. Cannot start or end with `/`
  3. Cannot contain consecutive `//`
  4. Cannot contain `..` or `\`
  5. Segments must contain only alphanumeric characters, `.`, `-`, `_`
  6. Cannot end with `.lock`
  7. Cannot be named `main` (reserved for main branch)


### Branch Metadata Path¶

Branch metadata is stored at `_refs/branches/{branch-name}.json` in the dataset root. Since branch names support hierarchical naming with `/` characters, the `/` is URL-encoded as `%2F` in the filename to distinguish it from directory separators (e.g., `bugfix/issue-123` becomes `bugfix%2Fissue-123.json`):
    
    
    {dataset_root}/
        _refs/
            branches/
                feature-a.json
                bugfix%2Fissue-123.json  # Note: '/' encoded as '%2F'
    

### Branch Metadata File Format¶

Each branch metadata file is a JSON file with the following fields:

JSON Key | Type | Optional | Description  
---|---|---|---  
`parent_branch` | string | Yes | Name of the branch this was created from. `null` indicates branched from main.  
`parent_version` | number |  | Version number of the parent branch at the time this branch was created.  
`create_at` | number |  | Unix timestamp (seconds since epoch) when the branch was created.  
`manifest_size` | number |  | Size of the initial manifest file in bytes.  
  
### Branch Dataset Layout¶

Each branch dataset is technically a [shallow clone](<../layout/#shallow-clone>) of the source dataset. Branch datasets are organized using the `tree/` directory at the dataset root:
    
    
    {dataset_root}/
        tree/
            {branch_name}/
                _versions/
                    *.manifest
                _transactions/
                    *.txn
                _deletions/
                    *.arrow
                    *.bin
                _indices/
                    {UUID}/
                        index.idx
    

Named branches store their version-specific files under `tree/{branch_name}/`, resembling the GitHub branch path convention. It uses the branch name as is to form the path, which means `/` would create a logical subdirectory (e.g., `bugfix/issue-123`, `feature/user-auth`):
    
    
    {dataset_root}/
        tree/
            feature-a/
                _versions/
                    1.manifest
                    2.manifest
            bugfix/
                issue-123/
                    _versions/
                        1.manifest
    

## Tagging¶

### Tag Name¶

Tag names must follow these validation rules:

  1. Cannot be empty
  2. Must contain only alphanumeric characters, `.`, `-`, `_`
  3. Cannot start or end with `.`
  4. Cannot end with `.lock`
  5. Cannot contain consecutive `..`


Note that tag names do not support `/` characters, unlike branch names.

### Tag Storage¶

Tags are stored as JSON files under `_refs/tags/` at the dataset root:
    
    
    {dataset_root}/
        _refs/
            tags/
                v1.0.0.json
                v1.1.0.json
                production.json
    

Tags are always stored at the root dataset level, regardless of which branch they reference.

### Tag File Format¶

Each tag file is a JSON file with the following fields:

JSON Key | Type | Optional | Description  
---|---|---|---  
`branch` | string | Yes | Branch name being tagged. `null` or absent indicates main branch.  
`version` | number |  | Version number being tagged within that branch.  
`manifest_size` | number |  | Size of the manifest file in bytes. Used for efficient manifest loading.  
  
Back to top



---

<!-- Source: https://lance.org/format/table/row_id_lineage/ -->

# Row ID and Lineage Specification¶

## Overview¶

Lance provides row identification and lineage tracking capabilities. Row addressing enables efficient random access to rows within the table through a physical location encoding. Stable row IDs provide persistent identifiers that remain constant throughout a row's lifetime, even as its physical location changes. Row version tracking records when rows were created and last modified, enabling incremental processing, change data capture, and time-travel queries.

## Row Identifier Forms¶

A row in Lance has two forms of row identifiers:

  * **Row address** \- the current physical location of the row in the dataset.
  * **Row ID** \- a logical identifier of the row. When stable row IDs are enabled, this remains stable for the lifetime of a logical row. When disabled (default mode), it is exactly equal to the row address.


### Row Address¶

Row address is the physical location of a row in the table, represented as a 64-bit identifier composed of two 32-bit values:
    
    
    row_address = (fragment_id << 32) | local_row_offset
    

This addressing scheme enables efficient random access: given a row address, the fragment and offset are extracted with bit operations. Row addresses change when data is reorganized through compaction or updates.

Row address is currently the primary form of identifier used for indexing purposes. Secondary indices (vector indices, scalar indices, full-text search indices) reference rows by their row addresses.

Note

Work to support stable row IDs in indices is in progress.

### Row ID¶

Row ID is a logical identifier for a row.

#### Stable Row ID¶

When a dataset is created with stable row IDs enabled, each row is assigned a unique auto-incrementing `u64` identifier that remains constant throughout the row's lifetime, even when the row's physical location (row address) changes. The `_rowid` system column exposes this logical identifier to users. See the next section for more details on assignment and update semantics.

#### Historical/unstable usage¶

Historically, the term "row id" was often used to refer to the physical row address (`_rowaddr`), which is not stable across compaction or updates.

Warning

With the introduction of stable row IDs, there may still be places in code and documentation that mix the terms "row ID" and "row address" or "row ID" and "stable row ID". Please raise a PR if you find any place incorrect or confusing.

## Stable Row ID¶

### Row ID Assignment¶

Row IDs are assigned using a monotonically increasing `next_row_id` counter stored in the manifest.

**Assignment Protocol:**

  1. Writer reads the current `next_row_id` from the manifest at the read version
  2. Writer assigns row IDs sequentially starting from `next_row_id` for new rows
  3. Writer updates `next_row_id` in the new manifest to `next_row_id + num_new_rows`
  4. If commit fails due to conflict, writer rebases:
  5. Re-reads the new `next_row_id` from the latest version
  6. Reassigns row IDs to new rows using the updated counter
  7. Retries commit


This protocol mirrors fragment ID assignment and ensures row IDs are unique across all table versions.

### Enabling Stable Row IDs¶

Stable row IDs are a dataset-level feature recorded in the table manifest.

  * Stable row IDs **must be enabled when the dataset is first created**.
  * Currently, they **cannot be turned on later** for an existing dataset. Attempts to write with `enable_stable_row_ids = true` against a dataset that was created without stable row IDs will not change the dataset's configuration.
  * When stable row IDs are disabled, the `_rowid` column (if requested) is not stable and should not be used as a persistent identifier.


Row-level version tracking (`_row_created_at_version`, `_row_last_updated_at_version`) and the row ID index described below are only available when stable row IDs are enabled.

### Row ID Behavior on Updates¶

When stable row IDs are enabled, updates preserve the logical row ID and remap it to a new physical address instead of assigning a new ID.

**Update Workflow:**

  1. Original row with `_rowid = R` exists at address `(F1, O1)`.
  2. An update operation writes a new physical row with the updated values at address `(F2, O2)`.
  3. The new physical row is assigned the same `_rowid = R`, so the logical identifier is preserved.
  4. The original physical row at `(F1, O1)` is marked deleted using the deletion vector for fragment `F1`.
  5. The row ID index for the new dataset version maps `_rowid = R` to `(F2, O2)`, and uses deletion vectors and fragment bitmaps to avoid returning the tombstoned row at `(F1, O1)`.


This design keeps `_rowid` stable for the lifetime of a logical row while allowing physical storage and secondary indices to be maintained independently.

### Row ID Sequences¶

#### Storage Format¶

Row ID sequences are stored using the `RowIdSequence` protobuf message. The sequence is partitioned into segments, each encoded optimally based on the data pattern.

RowIdSequence protobuf message
    
    
    message RowIdSequence {
        repeated U64Segment segments = 1;
    
    }
    

#### Segment Encodings¶

Each segment uses one of five encodings optimized for different data patterns:

##### Range (Contiguous Values)¶

For sorted, contiguous values with no gaps. Example: Row IDs `[100, 101, 102, 103, 104]` → `Range{start: 100, end: 105}`. Used for new fragments where row IDs are assigned sequentially.

Range protobuf message
    
    
    message Range {
        /// The start of the range, inclusive.
        uint64 start = 1;
        /// The end of the range, exclusive.
        uint64 end = 2;
    }
    

##### Range with Holes (Sparse Deletions)¶

For sorted values with few gaps. Example: Row IDs `[100, 101, 103, 104]` (missing 102) → `RangeWithHoles{start: 100, end: 105, holes: [102]}`. Used for fragments with sparse deletions where maintaining the range is efficient.

RangeWithHoles protobuf message
    
    
    message RangeWithHoles {
        /// The start of the range, inclusive.
        uint64 start = 1;
        /// The end of the range, exclusive.
        uint64 end = 2;
        /// The holes in the range, as a sorted array of values;
        /// Binary search can be used to check whether a value is a hole and should
        /// be skipped. This can also be used to count the number of holes before a
        /// given value, if you need to find the logical offset of a value in the
        /// segment.
        EncodedU64Array holes = 3;
    }
    

##### Range with Bitmap (Dense Deletions)¶

For sorted values with many gaps. The bitmap encodes 8 values per byte, with the most significant bit representing the first value. Used for fragments with dense deletion patterns.

RangeWithBitmap protobuf message
    
    
    message RangeWithBitmap {
        /// The start of the range, inclusive.
        uint64 start = 1;
        /// The end of the range, exclusive.
        uint64 end = 2;
        /// A bitmap of the values in the range. The bitmap is a sequence of bytes,
        /// where each byte represents 8 values. The first byte represents values
        /// start to start + 7, the second byte represents values start + 8 to
        /// start + 15, and so on. The most significant bit of each byte represents
        /// the first value in the range, and the least significant bit represents
        /// the last value in the range. If the bit is set, the value is in the
        /// range; if it is not set, the value is not in the range.
        bytes bitmap = 3;
    }
    

##### Sorted Array (Sparse Values)¶

For sorted but non-contiguous values, stored as an `EncodedU64Array`. Used for merged fragments or fragments after compaction.

##### Unsorted Array (General Case)¶

For unsorted values, stored as an `EncodedU64Array`. Rare; most operations maintain sorted order.

#### Encoded U64 Arrays¶

The `EncodedU64Array` message supports bitpacked encoding to minimize storage. The implementation selects the most compact encoding based on the value range, choosing between base + 16-bit offsets, base + 32-bit offsets, or full 64-bit values.

EncodedU64Array protobuf message
    
    
    message EncodedU64Array {
        message U16Array {
            uint64 base = 1;
            /// The deltas are stored as 16-bit unsigned integers.
            /// (protobuf doesn't support 16-bit integers, so we use bytes instead)
            bytes offsets = 2;
        }
    
        message U32Array {
            uint64 base = 1;
            /// The deltas are stored as 32-bit unsigned integers.
            /// (we use bytes instead of uint32 to avoid overhead of varint encoding)
            bytes offsets = 2;
        }
    
        message U64Array {
            /// (We use bytes instead of uint64 to avoid overhead of varint encoding)
            bytes values = 2;
        }
    
        oneof array {
            U16Array u16_array = 1;
            U32Array u32_array = 2;
            U64Array u64_array = 3;
        }
    
    }
    

#### Inline vs External Storage¶

Row ID sequences are stored either inline in the fragment metadata or in external files. Sequences smaller than ~200KB are stored inline to avoid additional I/O, while larger sequences are written to external files referenced by path and offset. This threshold balances manifest size against the overhead of separate file reads.

DataFragment row_id_sequence field
    
    
    message DataFragment {
      oneof row_id_sequence {
        bytes inline_row_ids = 5;
        ExternalFile external_row_ids = 6;
      }
    }
    

### Row ID Index¶

#### Construction¶

The row ID index is built at table load time by aggregating row ID sequences from all fragments:
    
    
    For each fragment F with ID f:
      For each (position p, row_id r) in F.row_id_sequence:
        index[r] = (f, p)
    

This creates a mapping from row ID to current row address.

#### Index Invalidation with Updates¶

When rows are updated and stable row IDs are enabled, the row ID index for a given dataset version only contains mappings for live physical rows. Tombstoned rows are excluded using deletion vectors, and logical row IDs whose contents have changed simply map to new row addresses.

**Example Scenario:**

  1. Initial state (version V): Fragment 1 contains rows with IDs `[1, 2, 3]` at offsets `[0, 1, 2]`.
  2. An update operation modifies the row with `_rowid = 2`:
     * A new fragment 2 is created with a row for `_rowid = 2` at offset `0`.
     * In fragment 1, the original physical row at offset `1` is marked deleted in the deletion vector.
  3. Row ID index in version V+1:
     * `1 → (1, 0)` ✓ Valid
     * `2 → (2, 0)` ✓ Valid (updated row in fragment 2)
     * `3 → (1, 2)` ✓ Valid


The address `(1, 1)` is no longer reachable via the row ID index because it is filtered out by the deletion vector when the index is constructed.

#### Fragment Bitmaps for Index Masking¶

Secondary indices use fragment bitmaps to track which row IDs remain valid:

**Without Row Updates:**
    
    
    String Index on column "str":
      Fragment Bitmap: {1, 2}  (covers fragments 1 and 2)
      All indexed row addresses are valid
    

**With Row Updates:**
    
    
    Vector Index on column "vec":
      Fragment Bitmap: {1}  (only fragment 1)
      The row with _rowid = 2 was updated, so the index entry that points to its old physical address is stale
      Index queries filter out the stale address using deletion vectors while returning the row at its new address
    

This bitmap-based approach allows indices to remain immutable while accounting for row modifications.

## Row Version Tracking¶

Row version tracking is available for datasets that use stable row IDs. Version sequences are aligned with the stable `_rowid` ordering within each fragment.

### Created At Version¶

Each row tracks the version at which it was created. For rows that are later updated, this creation version remains the version in which the row first appeared; updates do not change it. The sequence uses run-length encoding for efficient storage, where each run specifies a span of consecutive rows and the version they were created in.

Example: Fragment with 1000 rows created in version 5: 
    
    
    RowDatasetVersionSequence {
      runs: [
        RowDatasetVersionRun { span: Range{start: 0, end: 1000}, version: 5 }
      ]
    }
    

DataFragment created_at_version_sequence field
    
    
    message DataFragment {
      oneof created_at_version_sequence {
        bytes inline_created_at_versions = 9;
        ExternalFile external_created_at_versions = 10;
      }
    }
    

RowDatasetVersionSequence protobuf messages
    
    
    message RowDatasetVersionSequence {
        repeated RowDatasetVersionRun runs = 1;
    
    }
    

### Last Updated At Version¶

Each row tracks the version at which it was last modified. When a row is created, `last_updated_at_version` equals `created_at_version`.

When stable row IDs are enabled and a row is updated, Lance writes a new physical row for the same logical `_rowid` while tombstoning the old physical row. The `created_at_version` for that logical row is preserved from the original row, and `last_updated_at_version` is set to the current dataset version at the time of the update.

Example: Row created in version 3, updated in version 7: 
    
    
    Old physical row (tombstoned):
      _rowid: R
      created_at_version: 3
      last_updated_at_version: 3
    
    New physical row (current):
      _rowid: R
      created_at_version: 3
      last_updated_at_version: 7
    

DataFragment last_updated_at_version_sequence field
    
    
    message DataFragment {
      oneof last_updated_at_version_sequence {
        bytes inline_last_updated_at_versions = 7;
        ExternalFile external_last_updated_at_versions = 8;
      }
    }
    

## Change Data Feed¶

Lance supports querying rows that changed between versions through version tracking columns. These queries can be expressed as standard SQL predicates on the `_row_created_at_version` and `_row_last_updated_at_version` columns.

### Inserted Rows¶

Rows created between two versions can be retrieved by filtering on `_row_created_at_version`:
    
    
    SELECT * FROM dataset
    WHERE _row_created_at_version > {begin_version}
      AND _row_created_at_version <= {end_version}
    

This query returns all rows inserted in the specified version range, including the version metadata columns `_row_created_at_version`, `_row_last_updated_at_version`, and `_rowid`.

### Updated Rows¶

Rows modified (but not newly created) between two versions can be retrieved by combining filters on both version columns:
    
    
    SELECT * FROM dataset
    WHERE _row_created_at_version <= {begin_version}
      AND _row_last_updated_at_version > {begin_version}
      AND _row_last_updated_at_version <= {end_version}
    

This query excludes newly inserted rows by requiring `_row_created_at_version <= {begin_version}`, ensuring only pre-existing rows that were subsequently updated are returned.

Back to top



---

<!-- Source: https://lance.org/format/table/mem_wal/ -->

# MemTable & WAL Specification (Experimental)¶

Lance MemTable & WAL (MemWAL) specification describes a Log-Structured-Merge (LSM) tree architecture for Lance tables, enabling high-performance streaming write workloads while maintaining indexed read performance for key workloads including scan, point lookup, vector search and full-text search.

## Overall Architecture¶

A Lance table is called a **base table** under the context of the MemWAL spec. It must have an [unenforced primary key](<../#unenforced-primary-key>) defined in the table schema.

On top of the base table, the MemWAL spec defines a set of regions. Writers write to regions, and data in each region is merged into the base table asynchronously. An index is kept in the base table for readers to quickly discover the state of all regions at a point of time.

### MemWAL Region¶

A **MemWAL Region** is the main unit to horizontally scale out writes.

Each region has exactly one active writer at any time. Writers claim a region and then write data to that region. Data in each region is expected to be merged into the base table asynchronously.

Rows of the same primary key must be written to one and only one region. If two regions contain rows with the same primary key, the following scenario can cause data corruption:

  1. Region A receives a write with primary key `pk=1` at time T1
  2. Region B receives a write with primary key `pk=1` at time T2 (T2 > T1)
  3. The row in region B is merged into the base table first
  4. The row in region A is merged into the base table second
  5. The row from Region A (older) now overwrites the row from Region B (newer)


This violates the expected "last write wins" semantics. By ensuring each primary key is assigned to exactly one region via the region spec, merge order between regions becomes irrelevant for correctness.

See MemWAL Region Architecture for the complete region architecture.

### MemWAL Index¶

A **MemWAL Index** is the centralized structure for all MemWAL metadata on top of a base table. A table has at most one MemWAL index. It stores:

  * **Configuration** : Region specs defining how rows map to regions, and which indexes to maintain
  * **Merge progress** : Last generation merged to base table for each region
  * **Index catchup progress** : Which merged generation each base table index has been rebuilt to cover
  * **Region snapshots** : Snapshot of all region states for read optimization


The index is the source of truth for **configuration** , **merge progress** and **index catchup progress** Writers and mergers read the MemWAL index to get these configurations before writing.

Each region's manifest is authoritative for its own state. Readers use **region snapshots** is a read-only optimization to see a point-in-time view of all regions without the need to open each region manifest.

See MemWAL Index Details for the complete structure.

## Region Architecture¶

Within a region, writes are stored in an **in-memory table (MemTable)**. It is also written to the region's **Write-Ahead Log (WAL)** for durability guarantee. The MemTable is periodically **flushed** to storage based on memory pressure and other conditions. **Flushed MemTables** in storage are then asynchronously **merged** into the base table.

### MemTable¶

A MemTable holds rows inserted into the region before flushing to storage. It serves 2 purposes:

  1. build up data and related indexes to be flushed to storage as a flushed MemTable
  2. allow a reader to potentially access data that is not flushed to storage yet


#### MemTable Format¶

The complete in-memory format of a MemTable is implementation-specific and out of the scope of this spec. The Lance core Rust SDK maintains one default implementation and is available through all its language binding SDKs, but integrations are free to build their own MemTable format depending on the specific use cases, as long as it follows the MemWAL storage layout, reader and writer requirements when flushing MemTable.

Conceptually, because Lance uses [Arrow as its in-memory data exchange format](<https://arrow.apache.org/docs/format/index.html>), for the ease of explanation in this spec, we will treat MemTable as a list of Arrow record batches, and each write into the MemTable is a new Arrow record batch.

#### MemTable Generation¶

Based on conditions like memory limit and durability requirements, a MemTable needs to be **flushed** to storage and discarded. When that happens, new writes go to a new MemTable and the cycle repeats. Each MemTable is assigned a monotonically increasing generation number starting from 1. When MemTable of generation `N` is discarded, the next MemTable gets assigned generation `N+1`.

### WAL¶

WAL serves as the durable storage of all MemTables in a region. It consists of data in MemTables ordered by generation. Every time we write to the WAL, we call it a **WAL Flush**.

#### WAL Durability¶

When a write is flushed to WAL, the specific write becomes durable. Otherwise, if the MemTable is lost, data is also lost.

Multiple writes can be batched together in a single WAL flush to reduce WAL flush frequency and improve throughput. The more writes a single WAL flush batches, the longer it takes for a write to be durable.

The whole LSM tree's durability is determined by the durability of the WAL. For example, if WAL is stored in Amazon S3, it has 99.999999999% durability. If it is stored in local disk, the data will be lost if the local disk is damaged.

#### WAL Entry¶

Each time a WAL flush happens, it adds a new **WAL Entry** to the WAL. In other words, a WAL consists of an ordered list of WAL entries starting from position 0. Writer must flush WAL entries in sequential order from lower to higher position. If WAL entry `N` is not flushed fully, WAL entry `N+1` must not exist in storage.

#### WAL Replay¶

**Replaying** a WAL means to read data in the WAL from a lower to a higher position. This is commonly used to recover the latest MemTable after it is lost, by reading from the start position of the latest MemTable generation till the highest position in the WAL, assuming proper fencing to guard against multiple writers to the same region.

See Writer Fencing for the full fencing mechanism.

#### WAL Entry Format¶

Each WAL entry is a file in storage following the [Apache Arrow IPC stream format](<https://arrow.apache.org/docs/format/Columnar.html#ipc-streaming-format>) to store the batch of writes in the MemTable. The writer epoch is stored in the stream's Arrow schema metadata with key `writer_epoch` for fencing validation during replay.

#### WAL Storage Layout¶

Each WAL entry is stored within the WAL directory of the region located at `_mem_wal/{region_id}/wal`.

WAL files use bit-reversed 64-bit binary naming to distribute files evenly across the directory keyspace. This optimizes S3 throughput by spreading sequential writes across S3's internal partitions, minimizing throttling. The filename is the bit-reversed binary representation of the entry ID with suffix `.lance`. For example, entry ID 5 (binary `000...101`) becomes `1010000000000000000000000000000000000000000000000000000000000000.arrow`.

### Flushed MemTable¶

A flushed MemTable is created by flushing the MemTable to storage. In Lance MemWAL spec, a flushed MemTable must be a Lance table following the Lance table format spec.

Note

This is called Sorted String Table (SSTable) or Sorted Run in many LSM-tree literatures and implementations. However, since our MemTable is not sorted, we just use the term flushed MemTable to avoid confusion.

#### Flushed MemTable Storage Layout¶

The MemTable of generation `i` is flushed to `_mem_wal/{region_uuid}/{random_hex}_gen_{i}/` directory, where `{random_hex}` is a random 8-character hex value generated at flush time. The random hex value is necessary to ensure if one MemTable flush attempt fails, The retry can use another directory. The content within the generation directory follows the [Lance table storage layout](<../layout/>).

#### Merging MemTable to Base Table¶

Generation numbers determine merge order of flushed MemTable into base table: lower numbers represent older data and must be merged to the base table first to preserve correct upsert semantics.

Within a single flushed MemTable, if there are multiple rows of the same primary key, the row that is last inserted wins.

### Region Manifest¶

Each region has a manifest file. This is the source of truth for the state of a region.

#### Region Manifest Contents¶

The manifest contains:

  * **Fencing state** : `writer_epoch` as the latest writer fencing token, see Writer Fencing for more details.
  * **WAL pointers** : `replay_after_wal_entry_position` (last entry position flushed to MemTable, 0-based), `wal_entry_position_last_seen` (last entry position seen at manifest update, 0-based)
  * **Generation trackers** : `current_generation` (next generation to flush), `flushed_generations` list of generation number and directory path pairs (e.g., generation 1 at `a1b2c3d4_gen_1`)


Note: `wal_entry_position_last_seen` is a hint that may be stale since it's not updated on WAL write. It is updated opportunistically by any reader that can update the region manifest. The manifest itself is atomically written, but recovery must try to get newer WAL files to find the actual state beyond this hint.

The manifest is serialized as a protobuf binary file using the `RegionManifest` message.

RegionManifest protobuf message
    
    
    message RegionManifest {
      // Region identifier (UUID v4).
      UUID region_id = 11;
    
      // Manifest version number.
      // Matches the version encoded in the filename.
      uint64 version = 1;
    
      // Region spec ID this region was created with.
      // Set at region creation and immutable thereafter.
      // A value of 0 indicates a manually-created region not governed by any spec.
      uint32 region_spec_id = 10;
    
      // Writer fencing token - monotonically increasing.
      // A writer must increment this when claiming the region.
      uint64 writer_epoch = 2;
    
      // The most recent WAL entry position (0-based) that has been flushed to a MemTable.
      // During recovery, replay starts from replay_after_wal_entry_position + 1.
      uint64 replay_after_wal_entry_position = 3;
    
      // The most recent WAL entry position (0-based) at the time manifest was updated.
      // This is a hint, not authoritative - recovery must list files to find actual state.
      uint64 wal_entry_position_last_seen = 4;
    
      // Next generation ID to create (incremented after each MemTable flush).
      uint64 current_generation = 6;
    
      // Field 7 removed: merged_generation moved to MemWalIndexDetails.merged_generations
      // which is the authoritative source for merge progress.
    
      // List of flushed MemTable generations and their directory paths.
      repeated FlushedGeneration flushed_generations = 8;
    
    }
    

#### Region Manifest Versioning¶

Manifests are versioned starting from 1 and immutable. Each update creates a new manifest file at the next version number. Updates use put-if-not-exists or file rename to ensure atomicity depending on the storage system. If two processes compete, one wins and the other retries.

To commit a manifest version:

  1. Compute the next version number
  2. Write the manifest to `{bit_reversed_version}.binpb` using put-if-not-exists
  3. In parallel best-effort write to `version_hint.json` with `{"version": <new_version>}` (failure is acceptable)


To read the latest manifest version:

  1. Read `version_hint.json` to get the latest version hint. If not found, start from version 1
  2. Check existence for subsequent versions from the starting version
  3. Continue until a version is not found
  4. The latest version is the last found version


Note

This works because the write rate to region manifests is significantly lower than read rates. Region manifests are only updated when region metadata changes (MemTable flush), not on every write. This ensures HEAD requests will eventually terminate and find the latest version.

#### Region Manifest Storage Layout¶

All region manifest versions are stored in `_mem_wal/{region_id}/manifest` directory.

Each region manifest version file uses bit-reversed 64-bit binary naming, the same scheme as WAL files. For example, version 5 becomes `1010000000000000000000000000000000000000000000000000000000000000.binpb`.

## MemWAL Index Details¶

The MemWAL Index uses the [standard index storage](<../index/#index-storage>) at `_indices/{UUID}/`.

The index stores its data in two parts:

  1. **Index details** (`index_details` in `IndexMetadata`): Contains configuration, merge progress, and snapshot metadata
  2. **Region snapshots** : Stored as a Lance file or inline, depending on region count


### Index Details¶

The `index_details` field in `IndexMetadata` contains a `MemWalIndexDetails` protobuf message with the following key fields:

  * **Configuration fields** (`region_specs`, `maintained_indexes`) are the source of truth for MemWAL configuration. Writers read these fields to determine how to partition data and which indexes to maintain.
  * **Merge progress** (`merged_generations`) tracks the last generation merged to the base table for each region. This field is updated atomically with merge-insert data commits, enabling conflict resolution when multiple mergers operate concurrently. Each entry contains the region UUID and generation number.
  * **Index catchup progress** (`index_catchup`) tracks which merged generation each base table index has been rebuilt to cover. When data is merged from a flushed MemTable to the base table, the base table's indexes may be rebuilt asynchronously. During this window, queries should use the flushed MemTable's pre-built indexes instead of scanning unindexed data in the base table. See Indexed Read Plan for details.
  * **Region snapshot fields** (`snapshot_ts_millis`, `num_regions`, `inline_snapshots`) provide a snapshot of region states. The actual region manifests remain authoritative for region state. When `num_regions` is 0, the `inline_snapshots` field may be `None` or an empty Lance file with 0 rows but proper schema.

MemWalIndexDetails protobuf message
    
    
    message MemWalIndexDetails {
      // Snapshot timestamp (Unix timestamp in milliseconds).
      int64 snapshot_ts_millis = 1;
    
      // Number of regions in the snapshot.
      // Used to determine storage format without reading the snapshot data.
      uint32 num_regions = 2;
    
      // Inline region snapshots for small region counts.
      // When num_regions <= threshold (implementation-defined, e.g., 100),
      // snapshots are stored inline as serialized bytes.
      // Format: Lance file bytes with the region snapshot schema.
      optional bytes inline_snapshots = 3;
    
      // Region specs defining how to derive region identifiers.
      // This configuration determines how rows are partitioned into regions.
      repeated RegionSpec region_specs = 7;
    
      // Indexes from the base table to maintain in MemTables.
      // These are index names referencing indexes defined on the base table.
      // The primary key btree index is always maintained implicitly and
      // should not be listed here.
      //
      // For vector indexes, MemTables inherit quantization parameters (PQ codebook,
      // SQ params) from the base table index to ensure distance comparability.
      repeated string maintained_indexes = 8;
    
      // Last generation merged to base table for each region.
      // This is updated atomically with merge-insert data commits, enabling
      // conflict resolution when multiple mergers operate concurrently.
      //
      // Note: This is separate from region snapshots because:
      // 1. merged_generations is updated by mergers (atomic with data commit)
      // 2. region snapshots are updated by background index builder
      repeated MergedGeneration merged_generations = 9;
    
      // Per-index catchup progress tracking.
      // When data is merged to the base table, base table indexes are rebuilt
      // asynchronously. This field tracks which generation each index covers.
      //
      // For indexed queries, if an index's caught_up_generation < merged_generation,
      // readers should use flushed MemTable indexes for the gap instead of
      // scanning unindexed data in the base table.
      //
      // If an index is not present in this list, it is assumed to be fully caught up.
      repeated IndexCatchupProgress index_catchup = 10;
    
    }
    

### Region Identifier¶

Each region has a unique identifier across all regions following UUID v4 standard. When a new region is created, it is assigned a new identifier.

### Region Spec¶

A **Region Spec** defines how all rows in a table are logically divided into different regions, enabling automatic region assignment and query-time region pruning.

Each region spec has:

  * **Spec ID** : A positive integer that uniquely identifies this spec within the MemWAL index. IDs are never reused.
  * **Region fields** : An array of field definitions that determine how to compute region values.


Each region is bound to a specific region spec ID, recorded in its manifest. Regions without a spec ID (`spec_id = 0`) are manually-created regions not governed by any spec.

A region spec's field array consists of **region field** definitions. Each region field has the following properties:

Property | Description  
---|---  
`field_id` | Unique string identifier for this region field  
`source_ids` | Array of field IDs referencing source columns in the schema  
`transform` | A well-known region expression, specify this or `expression`  
`expression` | A DataFusion SQL expression for custom logic, specify this or `transform`  
`result_type` | The output type of the region value  
  
#### Region Expression¶

A **Region Expression** is a [DataFusion SQL expression](<https://datafusion.apache.org/user-guide/sql/index.html>) that derives a region value from source column(s). Source columns are referenced as `col0`, `col1`, etc., corresponding to the order of field IDs in `source_ids`.

Region expressions must satisfy the following requirements:

  1. **Deterministic** : The same input value must always produce the same output value.
  2. **Stateless** : The expression must not depend on external state (e.g., current time, random values, session variables).
  3. **Type-promotion resistant** : The expression must produce the same result for equivalent values regardless of their numeric type (e.g., `int32(5)` and `int64(5)` must yield the same region value).
  4. **Column removal resistant** : If a source field ID is not found in the schema, the column should be interpreted as NULL.
  5. **NULL-safe** : The expression should properly handle NULL inputs and have defined behavior (e.g., return NULL if input is NULL for single-column expressions).
  6. **Consistent with result type** : The expression's return type must be consistent with `result_type` in non-NULL cases.


#### Region Transform¶

A **Region Transform** is a well-known region expression with a predefined name. When a transform is specified, the expression is derived automatically.

Transform | Parameters | Region Expression | Result Type  
---|---|---|---  
`identity` | (none) | `col0` | same as source  
`year` | (none) | `date_part('year', col0)` | `int32`  
`month` | (none) | `date_part('month', col0)` | `int32`  
`day` | (none) | `date_part('day', col0)` | `int32`  
`hour` | (none) | `date_part('hour', col0)` | `int32`  
`bucket` | `num_buckets` | `abs(murmur3(col0)) % N` | `int32`  
`multi_bucket` | `num_buckets` | `abs(murmur3_multi(col0, col1, ...)) % N` | `int32`  
`truncate` | `width` | `left(col0, W)` (string) or `col0 - (col0 % W)` (numeric) | same as source  
  
The `bucket` and `multi_bucket` transforms use Murmur3 hash functions:

  * **`murmur3(col)`** : Computes the 32-bit Murmur3 hash (x86 variant, seed 0) of a single column. Returns a signed 32-bit integer. Returns NULL if input is NULL.
  * **`murmur3_multi(col0, col1, ...)`** : Computes the Murmur3 hash across multiple columns. Returns a signed 32-bit integer. NULL fields are ignored during hashing; returns NULL only if all inputs are NULL.


The hash result is wrapped with `abs()` and modulo `N` to produce a non-negative bucket number in the range `[0, N)`.

### Region Snapshot Storage¶

Region snapshots are stored using one of two strategies based on the number of regions:

Region Count | Storage Strategy | Location  
---|---|---  
<= 100 (threshold) | Inline | `inline_snapshots` field in index details  
> 100 | External Lance file | `_indices/{UUID}/index.lance`  
  
The threshold (100 regions) is implementation-defined and may vary.

**Inline storage** : For small region counts, snapshots are serialized as a Lance file and stored in the `inline_snapshots` field. This keeps the index metadata compact while avoiding an additional file read for common cases.

**External Lance file** : For large region counts, snapshots are stored as a Lance file at `_indices/{UUID}/index.lance`. This file uses standard Lance format with the region snapshot schema, enabling efficient columnar access and compression.

### Region Snapshot Arrow Schema¶

Region snapshots are stored as a Lance file with one row per region. The schema has one column per `RegionManifest` field plus region spec columns:

Column | Type | Description  
---|---|---  
`region_id` | `fixed_size_binary(16)` | Region UUID bytes  
`version` | `uint64` | Region manifest version  
`region_spec_id` | `uint32` | Region spec ID (0 if manual)  
`writer_epoch` | `uint64` | Writer fencing token  
`replay_after_wal_entry_position` | `uint64` | Last WAL entry position (0-based) flushed to MemTable  
`wal_entry_position_last_seen` | `uint64` | Last WAL entry position (0-based) seen (hint)  
`current_generation` | `uint64` | Next generation to flush  
`flushed_generations` | `list<struct<generation: uint64, path: string>>` | Flushed MemTable paths  
`region_field_{field_id}` | varies | Region field value (one column per field in region spec)  
  
For example, with a region spec containing a field `user_bucket` of type `int32`:

Column | Type | Description  
---|---|---  
... | ... | (base columns above)  
`region_field_user_bucket` | `int32` | Bucket value for this region  
  
This schema directly corresponds to the fields in the `RegionManifest` protobuf message plus the computed region field values.

## Storage Layout¶

Here is a recap of the storage layout with all the files and concepts defined so far:
    
    
    {table_path}/
    ├── _indices/
    │   └── {index_uuid}/                    # MemWAL Index (uses standard index storage)
    │       └── index.lance                  # Serialized region snapshots (Lance file)
    │
    └── _mem_wal/
        └── {region_uuid}/                   # Region directory (UUID v4)
            ├── manifest/
            │   ├── {bit_reversed_version}.binpb     # Serialized region manifest (bit-reversed naming)
            │   └── version_hint.json                # Version hint file
            ├── wal/
            │   ├── {bit_reversed_entry_id}.lance    # WAL data files (bit-reversed naming)
            │   └── ...
            └── {random_hash}_gen_{i}/        # Flushed MemTable (generation i, random prefix)
                ├── _versions/
                │   └── {version}.manifest    # Table manifest (V2 naming scheme)
                ├── _indices/                 # Indexes
                │   ├── {vector_index}/
                │   └── {scalar_index}/
                └── bloom_filter.bin          # Primary key bloom filter
    

## Implementation Expectation¶

This specification describes the storage layout for the LSM tree architecture. Implementations are free to use any approach to fulfill the storage layout requirements. Once data is written to the expected storage layout, the reader and writer expectations apply.

The specification defines:

  * **Storage layout** : The directory structure, file formats, and naming conventions for WAL entries, flushed MemTables, region manifests, and the MemWAL index
  * **Durability guarantees** : How data is persisted through WAL entries and flushed MemTables
  * **Consistency model** : How readers and writers coordinate through manifests and epoch-based fencing


Implementations may choose different approaches for:

  * In-memory data structures and indexing
  * Buffering strategies before WAL flush
  * Background task scheduling and concurrency
  * Query execution strategies


As long as the storage layout is correct and the documented invariants are maintained, implementations can optimize for their specific use cases.

## Writer Expectations¶

A writer operates on a single region and is responsible for:

  1. Claiming the region using epoch-based fencing
  2. Writing data to WAL entries and flushed MemTables following the storage layout
  3. Maintaining the region manifest to track WAL and generation progress


### Writer Fencing¶

Writers use epoch-based fencing to ensure single-writer semantics per region.

To claim a region:

  1. Load the latest region manifest
  2. Increment `writer_epoch` by one
  3. Atomically write a new manifest version
  4. If the write fails (another writer claimed the epoch), reload and retry with a higher epoch


Before any manifest update, a writer must verify its `writer_epoch` remains valid:

  * If `local_writer_epoch == stored_writer_epoch`: The writer is still active and may proceed
  * If `local_writer_epoch < stored_writer_epoch`: The writer has been fenced and must abort


For a concrete example, see Appendix 1: Writer Fencing Example.

## Background Job Expectations¶

Background jobs handle merging flushed MemTables to the base table and garbage collection.

### MemTable Merger¶

Flushed MemTables must be merged to the base table in **ascending generation order** within each region. This ordering is essential for correct upsert semantics: newer generations must overwrite older ones.

The merge uses Lance's merge-insert operation with atomic transaction semantics:

  * `merged_generations[region_id]` is updated atomically with the data commit
  * On commit conflict, check the conflicting commit's `merged_generations` to determine if the generation was already merged


For a concrete example, see Appendix 2: Concurrent Merger Example.

### Garbage Collector¶

The garbage collector removes obsolete data from region directories. Flushed MemTables and their referenced WAL files may be deleted after:

  1. The generation has been merged to the base table (`generation <= merged_generations[region_id]`)
  2. All maintained indexes have caught up (`generation <= min(index_catchup[I].caught_up_generation)`)
  3. No retained base table version references the generation for time travel


## Reader Expectations¶

### LSM Tree Merging Read¶

Readers **MUST** merge results from multiple data sources (base table, flushed MemTables, in-memory MemTables) by primary key to ensure correctness.

When the same primary key exists in multiple sources, the reader must keep only the newest version based on:

  1. **Generation number** (`_gen`): Higher generation wins. The base table has generation 0, MemTables have positive integers starting from 1.
  2. **Row address** (`_rowaddr`): Within the same generation, higher row address wins (later writes within a batch overwrite earlier ones).


The ordering for "newest" is: highest `_gen` first, then highest `_rowaddr`.

This deduplication is essential because:

  * A row updated in a MemTable also exists (with older data) in the base table
  * A flushed MemTable that has been merged to the base table may not yet be garbage collected, causing the same row to appear in both
  * A single write batch may contain multiple updates to the same primary key


Without proper merging, queries would return duplicate or stale rows.

### Reader Consistency¶

Reader consistency depends on two factors:

  1. access to in-memory MemTables
  2. the source of region metadata (either through MemWAL index or region manifests)


Strong consistency requires access to in-memory MemTables for all regions involved in the query and reading region manifests directly. Otherwise, the query is eventually consistent due to missing unflushed data or stale MemWAL Index snapshots.

Note

Reading a stale MemWAL Index does not impact correctness, only freshness:
    
    
    - **Merged MemTable still in index**: If a flushed MemTable has been merged to the base table but still shows in the MemWAL index, readers query both. This results in some inefficiency for querying the same data twice, but [LSM-tree merging](#lsm-tree-merging-read) ensures correct results since both contain the same data. The inefficiency is also compensated by the fact that the data is covered by index and we rarely end up scanning both data.
    - **Garbage collected MemTable still in index**: If a flushed MemTable has been garbage collected, but is still in the MemWAL index, readers would fail to open it and skip it. This is also safe because if it is garbage collected, the data must already exist in the base table.
    - **Newly flushed MemTable not in index**: If a newly flushed MemTable is added after the snapshot was built, it is not queried. The result is eventually consistent but correct for the snapshot's point in time.
    

### Query Planning¶

#### MemTable Collection¶

The query planner collects datasets from multiple sources and assembles them for unified query execution. Datasets come from:

  1. base table (representing already-merged data)
  2. flushed MemTables (persisted but not yet merged)
  3. optionally in-memory MemTables (if accessible).


Each dataset is tagged with a generation number: 0 for the base table, and positive integers for MemTable generations. Within a region, the generation number determines data freshness, with higher numbers representing newer data. Rows from different regions do not need deduplication since each primary key maps to exactly one region.

The planner also collects bloom filters from each generation for staleness detection during search queries.

#### Region Pruning¶

Before executing queries, if region spec is available, the planner evaluates filter predicates against region specs to determine which regions may contain matching data. This pruning step reduces the number of regions to scan.

For each filter predicate:

  1. Extract predicates on columns used in region specs
  2. Evaluate which region values can satisfy the predicate
  3. Prune regions whose values cannot match


For example, with a region spec using `bucket(user_id, 10)` and a filter `user_id = 123`:

  1. Compute `bucket(123, 10) = 3`
  2. Only scan regions with bucket value 3
  3. Skip all other regions


Region pruning applies to both scan queries and prefilters in search queries.

#### Indexed Read Plan¶

When data is merged from a flushed MemTable to the base table, the base table's indexes are rebuilt asynchronously by the base table index builders. During this window, the merged data exists in the base table but is not yet covered by the base table's indexes.

Without special handling, indexed queries would fall back to expensive full scans for the unindexed part of the base table. To maintain indexed read performance, the query planner should use `index_catchup` progress to determine the optimal data source for each query.

The key insight is that flushed MemTables serve as a bridge between the base table's index catchup and the current merged state. For a query that requires a specific index for acceleration, when `index_gen < merged_gen`, the generations in the gap `(index_gen, merged_gen]` have data already merged in the base table but are not covered by the base table's index. Since flushed MemTables contain pre-built indexes (created during MemTable flush), queries can use these indexes instead of scanning unindexed data in the base table. This ensures all reads remain indexed regardless of how far behind the async index builder is.

## Appendices¶

### Appendix 1: Writer Fencing Example¶

This example demonstrates how epoch-based fencing prevents data corruption when two writers compete for the same region.

#### Initial State¶
    
    
    Region manifest (version 1):
      writer_epoch: 5
      replay_after_wal_entry_position: 10
      wal_entry_position_last_seen: 12
    

#### Scenario¶

Step | Writer A | Writer B | Manifest State  
---|---|---|---  
1 | Loads manifest, sees epoch=5 |  | epoch=5, version=1  
2 | Increments to epoch=6, writes manifest v2 |  | epoch=6, version=2  
3 | Starts writing WAL entries 13, 14, 15 |  |   
4 |  | Loads manifest v2, sees epoch=6 | epoch=6, version=2  
5 |  | Increments to epoch=7, writes manifest v3 | epoch=7, version=3  
6 |  | Starts writing WAL entries 16, 17 |   
7 | Tries to flush MemTable, loads manifest |  |   
8 | Sees epoch=7, but local epoch=6 |  |   
9 | **Writer A is fenced!** Aborts all operations |  |   
10 |  | Continues writing normally | epoch=7, version=3  
  
#### What Happens to Writer A's WAL Entries?¶

Writer A wrote WAL entries 13, 14, 15 with `writer_epoch=6` in their schema metadata.

When Writer B performs crash recovery or MemTable flush:

  1. Reads WAL entries sequentially starting from `replay_after_wal_entry_position + 1` (entry 11, since positions are 0-based)
  2. For each entry, checks existence using HEAD request on the bit-reversed filename
  3. Continues until an entry is not found (e.g., entry 18 doesn't exist)
  4. Finds entries 13, 14, 15, 16, 17
  5. Reads each file's `writer_epoch` from schema metadata
  6. Entries 13, 14, 15 have `writer_epoch=6` which is <= current epoch (7) -> **valid, will be replayed**
  7. Entries 16, 17 have `writer_epoch=7` -> **valid, will be replayed**


#### Key Points¶

  1. **No data loss** : Writer A's entries are not discarded. They were written with a valid epoch at the time and will be included in recovery.

  2. **Consistency preserved** : Writer A is prevented from making further writes that could conflict with Writer B.

  3. **Orphaned files are safe** : WAL files from fenced writers remain on storage and are replayed by the new writer. They are only garbage collected after being included in a flushed MemTable that has been merged.

  4. **Epoch validation timing** : Writers check their epoch before manifest updates (MemTable flush), not on every WAL write. This keeps the hot path fast while ensuring consistency at commit boundaries.


### Appendix 2: Concurrent Merger Example¶

This example demonstrates how MemWAL Index and conflict resolution handle concurrent mergers safely.

#### Initial State¶
    
    
    MemWAL Index:
      merged_generations: {region: 5}
    
    Region manifest (version 1):
      current_generation: 8
      flushed_generations: [(6, "abc123_gen_6"), (7, "def456_gen_7")]
    

#### Scenario 1: Racing on the Same Generation¶

Two mergers both try to merge generation 6 concurrently.

Step | Merger A | Merger B | MemWAL Index  
---|---|---|---  
1 | Reads index: merged_gen=5 |  | merged_gen=5  
2 | Reads region manifest |  |   
3 | Starts merging gen 6 |  |   
4 |  | Reads index: merged_gen=5 | merged_gen=5  
5 |  | Reads region manifest |   
6 |  | Starts merging gen 6 |   
7 | Commits (merged_gen=6) |  | **merged_gen=6**  
8 |  | Tries to commit |   
9 |  | **Conflict** : reads new index |   
10 |  | Sees merged_gen=6 >= 6, aborts |   
11 |  | Reloads, continues to gen 7 |   
  
Merger B's conflict resolution detected that generation 6 was already merged by checking the MemWAL Index in the conflicting commit.

#### Scenario 2: Crash After Table Commit¶

Merger A crashes after committing to the table.

Step | Merger A | Merger B | MemWAL Index  
---|---|---|---  
1 | Reads index: merged_gen=5 |  | merged_gen=5  
2 | Merges gen 6, commits |  | **merged_gen=6**  
3 | **CRASH** |  | merged_gen=6  
4 |  | Reads index: merged_gen=6 | merged_gen=6  
5 |  | Reads region manifest |   
6 |  | **Skips gen 6** (already merged) |   
7 |  | Merges gen 7, commits | **merged_gen=7**  
  
The MemWAL Index is the single source of truth. Merger B correctly used it to determine that generation 6 was already merged.

#### Key Points¶

  1. **Single source of truth** : `merged_generations` is the authoritative source for merge progress, updated atomically with data.

  2. **Conflict resolution uses MemWAL Index** : When a commit conflicts, the merger checks the conflicting commit's MemWAL Index.

  3. **No progress regression** : Because MemWAL Index is updated atomically with data, concurrent mergers cannot regress the merge progress.


Back to top



---

<!-- Source: https://lance.org/format/table/index/ -->

# Indices in Lance¶

Lance supports three main categories of indices to accelerate data access: scalar indices, vector indices, and system indices.

**Scalar indices** are traditional indices that speed up queries on scalar data types, such as integers and strings. Examples include [B-trees](<scalar/btree/>) and [full-text search indices](<scalar/fts/>). Typically, scalar indices receive a query predicate, such as equality or range conditions, and output a set of row addresses that satisfy the predicate.

**[Vector indices](<vector/>)** are specialized for approximate nearest neighbor (ANN) search on high-dimensional vector data, such as embeddings from machine learning models. Examples includes IVF (Inverted File) indices and HNSW (Hierarchical Navigable Small World) indices. These are separate from scalar indices because they use meaningfully different query patterns. Instead of sargable predicates, vector indices receive a query vector and return the nearest neighbor row addresses based on some distance metric, such as Euclidean distance or cosine similarity. They return row addresses and the corresponding distances.

**System indices** are auxiliary indices that help accelerate internal system operations. They are different from user-facing scalar and vector indices, as they are not directly used in user queries. Examples include the [Fragment Reuse Index](<system/frag_reuse/>), which supports efficient row address remapping after compaction.

## Design¶

Lance indices are designed with the following design choices in mind:

  1. **Indices are loaded on demand** : A dataset can be loaded and read without loading any indices. Indices are only loaded when a query can benefit from them. This design minimizes memory usage and speeds up dataset opening time.
  2. **Indices can be loaded progressively** : indices are designed so that only the necessary parts are loaded into memory during query execution. For example, when querying a B-tree index, it loads a small page table to figure out which pages of the index to load for the given query, and then only loads those pages to perform the indexed search. This amortizes the cost of cold index queries, since each query only needs to load a small portion of the index.
  3. **Indices can be coalesced to larger units than fragments.** Indices are much smaller than data files, so it is efficient to coalesce index segments to cover multiple fragments. This reduces the number of index files that need to be opened during query execution and then number of unique index data structures that need to be queried.
  4. **Index files are immutable once written, similar to data files.** They can be modified only by creating new files. This means they can be safely cached in memory or on disk without worrying about consistency issues.


## Basic Concepts¶

An index in Lance is defined over a specific column (or multiple columns) of a dataset. It is identified by its name.

An index is made up of multiple **index segments** , identified by their unique UUIDs. Each segment is an independent, self-contained index covering a subset of the data.

Each index segment covers a disjoint subset of fragments in the dataset. The segments must cover all rows in the fragments they cover, with one exception: if a fragment has delete markers at the time of index creation, the index segment is allowed to not contain the deleted rows. The fragments an index covers are those recorded in the `fragment_bitmap` field.

Index segments together **do not** need to cover all fragments. This means an index isn't required to be fully up-to-date. When this happens, engines can split their queries into indexed and unindexed subplans and merge the results.

  
Abstract layout of a typical dataset, with three fragments and two indices. 

Consider the example dataset in the figure above:

  * The dataset contains three fragments with ids 0, 1, 2. Fragment 1 has 10 deleted rows, indicated by the deletion file.
  * There is an index called "id_idx", which has two segments: one covering fragments 0 and another covering fragment 1. Fragment 2 is not covered by the index. Queries using this index will need to query both segments and then scan fragment 2 directly. Additionally, when querying the segment covering fragment 1, the engine will need to filter out the 10 deleted rows.
  * There is another index called "vec_idx", which has a single segment covering all three fragments. Because it covers all fragments, queries using this index do not need to scan any fragments directly. They do, however, need to filter out the 10 deleted rows from fragment 1.


## Index Storage¶

The content of each index is stored at the `_indices/{UUID}` directory under the [base path](<../layout/#base-path-system>). We call this location the **index directory**. The actual content stored in the index directory depends on the index type. These can be arbitrary files defined by the index implementation. However, often they are made up of Lance files containing the index data structures. This allows reuse of the existing Lance file format code for reading and writing index data.

## Creating and Updating Index Segments¶

Index segments are created and updated through a transactional process:

  1. **Build the index data** : Read the relevant column data from the fragments to be indexed and construct the index data structures. Write these to files in a new `_indices/{UUID}` directory, where `{UUID}` is a newly generated unique identifier.

  2. **Prepare the metadata** : Create an `IndexMetadata` message with:

  3. `uuid`: The newly generated UUID
  4. `name`: The index name (must match existing segments if adding to an existing index)
  5. `fields`: The column(s) being indexed
  6. `fragment_bitmap`: The set of fragment IDs covered by this segment
  7. `index_details`: Index-specific configuration and parameters
  8. `version`: The format version of this index type
  9. See the full protobuf definition in [table.proto](<https://github.com/lance-format/lance/blob/main/protos/table.proto>).

  10. **Commit the transaction** : Write a new manifest that includes the new index segment in its `IndexSection`. This is done atomically using the same transaction mechanism as data writes.


When updating an indexed column in place (without deleting the row), the engine must remove the affected fragment IDs from the `fragment_bitmap` field of any index segments that cover those fragments. This marks those fragments as needing re-indexing without invalidating the entire segment and prevents invalid data from being read from the index.

## Index Compatibility¶

Before using an index segment, engines must verify they support it:

  1. **Check the index type** : The `index_details` field contains a protobuf `Any` message whose type URL identifies the index type (e.g., B-tree, IVF, HNSW). If the engine does not recognize the type, it should skip this index segment.

  2. **Check the version** : The `version` field in `IndexMetadata` indicates the format version of the index segment. If the engine does not support this version, it should skip this index segment. This allows index formats to evolve over time while maintaining backwards compatibility.


When an engine cannot use an index segment, it should fall back to scanning the fragments that would have been covered by that segment.

## Loading an index¶

When loading an index:

  1. Get the offset to the index section from the `index_section` field in the [manifest](<../#manifest>).
  2. Read the index section from the manifest file. This is a protobuf message of type `IndexSection`, which contains a list of `IndexMetadata` messages, each describing an index segment.
  3. Read the index files from the `_indices/{UUID}` directory under the dataset directory, where `{UUID}` is the UUID of the index segment.


Optimizing manifest loading

When the manifest file is small, you can read and cache the index section eagerly. This avoids an extra file read when loading indices.

The `IndexMetadata` message contains important information about the index segment:

  * `uuid`: the unique identifier of the index segment.
  * `fields`: the column(s) the index is built on.
  * `fragment_bitmap`: the set of fragment IDs covered by this index segment.
  * `index_details`: a protobuf `Any` message that contains index-specific details, such as index type, parameters, and storage format. This allows different index types to store their own metadata.

Full protobuf definitions There are both part of the `table.proto` file in the Lance source code. 
    
    
    message IndexSection {
      repeated IndexMetadata indices = 1;
    
    }
    
    message IndexMetadata {
      // Unique ID of an index. It is unique across all the dataset versions.
      UUID uuid = 1;
    
      // The columns to build the index. These refer to file.Field.id.
      repeated int32 fields = 2;
    
      // Index name. Must be unique within one dataset version.
      string name = 3;
    
      // The version of the dataset this index was built from.
      uint64 dataset_version = 4;
    
      // A bitmap of the included fragment ids.
      //
      // This may by used to determine how much of the dataset is covered by the
      // index. This information can be retrieved from the dataset by looking at
      // the dataset at `dataset_version`. However, since the old version may be
      // deleted while the index is still in use, this information is also stored
      // in the index.
      //
      // The bitmap is stored as a 32-bit Roaring bitmap.
      bytes fragment_bitmap = 5;
    
      // Details, specific to the index type, which are needed to load / interpret the index
      //
      // Indices should avoid putting large amounts of information in this field, as it will
      // bloat the manifest.
      //
      // Indexes are plugins, and so the format of the details message is flexible and not fully
      // defined by the table format.  However, there are some conventions that should be followed:
      //
      // - When Lance APIs refer to indexes they will use the type URL of the index details as the
      //   identifier for the index type.  If a user provides a simple string identifier like
      //   "btree" then it will be converted to "/lance.table.BTreeIndexDetails"
      // - Type URLs comparisons are case-insensitive.  Thereform an index must have a unique type
      //   URL ignoring case.
      google.protobuf.Any index_details = 6;
    
      // The minimum lance version that this index is compatible with.
      optional int32 index_version = 7;
    
      // Timestamp when the index was created (UTC timestamp in milliseconds since epoch)
      //
      // This field is optional for backward compatibility. For existing indices created before
      // this field was added, this will be None/null.
      optional uint64 created_at = 8;
    
      // The base path index of the data file. Used when the file is imported or referred from another dataset.
      // Lance use it as key of the base_paths field in Manifest to determine the actual base path of the data file.
      optional uint32 base_id = 9;
    
      // List of files and their sizes for this index segment.
      // This enables skipping HEAD calls when opening indices and allows reporting
      // of index sizes without extra IO.
      // If this is empty, the index files sizes are unknown.
      repeated IndexFile files = 10;
    
    }
    

## Handling deleted and invalidated rows¶

Since index segments are immutable, they may contain references to rows that have been deleted or updated. These should be filtered out during query execution.

  
Representation of index segment covering fragments that have deleted rows, completely deleted fragments, and updated fragments. 

There are three situations to consider:

  1. **A fragment has some deleted rows.** A few of the rows in the fragment have been marked as deleted, but some of the rows are still present. The row addresses from the deletion file should be used to filter out results from the index.
  2. **A fragment has been completely deleted.** This can be detected by checking if a fragment ID present in the fragment bitmap is missing from the dataset. Any row addresses from this fragment should be filtered out.
  3. **A fragment has had the indexed column updated in place.** This cannot be detected just by examining metadata. To prevent reading invalid data, the engine should filter out any row addresses that are not in the index's current `fragment_bitmap`.


## Compaction and remapping¶

When fragments are compacted, the row addresses of the rows in the fragments change. This means that any index segments referencing those fragments will no longer point to existing row addresses. There are three ways to handle this:

  1. Do nothing and let the index segment not cover those fragments anymore. This approach is simple and valid, but it means compaction can immediately make an index out-of-date. This is the worst options for query performance.

  2. Immediately rewrite the index segments with the row addresses remapped. This approach ensures the index is kept up-to-date, but it incurs significant write amplification during compaction.

  3. Create a [Fragment Reuse Index](<system/frag_reuse/>) that maps old row addresses to new row addresses. This allows readers to remap the row addresses in memory upon reading the index segments. This approach adds some IO and computation overhead during query execution, but avoids write amplification during compaction.


## Stable Row ID for Index¶

Indices can optionally use stable row IDs instead of row addresses. A stable row ID is a logical identifier that remains constant even when rows are moved during compaction.

**Benefits:**

  * No remapping needed after compaction
  * Updates only invalidate the index if the indexed column data changes


**Tradeoffs:**

  * Requires an additional lookup to translate stable row IDs to physical row addresses at query time


This feature is currently experimental. Performance evaluation is ongoing to determine when the tradeoff is worthwhile.

Back to top



---

<!-- Source: https://lance.org/format/table/index/scalar/btree/ -->

# BTree Index¶

The BTree index is a two-level structure that provides efficient range queries and sorted access. It strikes a balance between an expensive memory structure containing all values and an expensive disk structure that can't be efficiently searched.

The upper layers of the BTree are designed to be cached in memory and stored in a BTree structure (`page_lookup.lance`), while the leaves are searched using sub-indices (`page_data.lance`, currently just a flat file). This design enables efficient memory usage - for example, with 1 billion values, the index can store 256K leaves of size 4K each, requiring only a few MiB of memory (depending on data type) for the BTree metadata while narrowing any search to just 4K values.

## Index Details¶
    
    
    message BTreeIndexDetails {
    
    }
    

## Storage Layout¶

The BTree index consists of two files:

  1. `page_lookup.lance` \- The BTree structure mapping value ranges to page numbers
  2. `page_data.lance` \- The actual sub-indices (flat file) containing sorted values and row IDs


### Page Lookup File Schema (BTree Structure)¶

Column | Type | Nullable | Description  
---|---|---|---  
`min` | {DataType} | true | Minimum value in the page (forms BTree keys)  
`max` | {DataType} | true | Maximum value in the page (for range pruning)  
`null_count` | UInt32 | false | Number of null values in the page  
`page_idx` | UInt32 | false | Page number pointing to the sub-index in page_data.lance  
  
### Schema Metadata¶

Key | Type | Description  
---|---|---  
`batch_size` | String | Number of rows per page (default: "4096")  
  
### Page Data File Schema (Sub-indices)¶

Column | Type | Nullable | Description  
---|---|---|---  
`values` | {DataType} | true | Sorted values from the indexed column (flat file)  
`ids` | UInt64 | false | Row IDs corresponding to each value  
  
## Accelerated Queries¶

The BTree index provides exact results for the following query types:

Query Type | Description | Operation  
---|---|---  
**Equals** | `column = value` | BTree lookup to find relevant pages, then search within sub-indices  
**Range** | `column BETWEEN a AND b` | BTree traversal for pages overlapping the range, then search each sub-index  
**IsIn** | `column IN (v1, v2, ...)` | Multiple BTree lookups, union results from all matching sub-indices  
**IsNull** | `column IS NULL` | Returns rows from all pages where null_count > 0  
  
Back to top



---

<!-- Source: https://lance.org/format/table/index/scalar/bitmap/ -->

# Bitmap Index¶

Bitmap indices use bit arrays to represent the presence or absence of values, providing extremely fast query performance for low-cardinality columns.

## Index Details¶
    
    
    message BitmapIndexDetails {
    
    }
    

## Storage Layout¶

The bitmap index consists of a single file `bitmap_page_lookup.lance` that stores the mapping from values to their bitmaps.

### File Schema¶

Column | Type | Nullable | Description  
---|---|---|---  
`keys` | {DataType} | true | The unique value from the indexed column  
`bitmaps` | Binary | true | Serialized RowAddrTreeMap containing row addrs where this value appears  
  
## Accelerated Queries¶

Query Type | Description | Operation  
---|---|---  
**Equals** | `column = value` | Returns the bitmap for the specific value  
**Range** | `column BETWEEN a AND b` | Unions all bitmaps for values in the range  
**IsIn** | `column IN (v1, v2, ...)` | Unions bitmaps for all specified values  
**IsNull** | `column IS NULL` | Returns the pre-computed null bitmap  
  
Back to top



---

<!-- Source: https://lance.org/format/table/index/scalar/label_list/ -->

# Label List Index¶

Label list indices are optimized for columns containing multiple labels or tags per row. They provide efficient set-based queries on multi-value columns using an underlying bitmap index.

## Index Details¶
    
    
    message LabelListIndexDetails {
    
    }
    

## Storage Layout¶

The label list index uses a bitmap index internally and stores its data in:

  1. `bitmap_page_lookup.lance` \- Bitmap index mapping unique labels to row IDs


### File Schema¶

Column | Type | Nullable | Description  
---|---|---|---  
`keys` | {DataType} | true | The unique label value from the indexed column  
`bitmaps` | Binary | true | Serialized RowAddrTreeMap containing row addr where this label appears  
  
## Accelerated Queries¶

The label list index provides exact results for the following query types:

Query Type | Description | Operation | Result Type  
---|---|---|---  
**array_has / array_contains** | Array contains the specified value | Bitmap lookup for a single label | Exact  
**array_has_all** | Array contains all specified values | Intersects bitmaps for all specified labels | Exact  
**array_has_any** | Array contains any of specified values | Unions bitmaps for all specified labels | Exact  
  
Back to top



---

<!-- Source: https://lance.org/format/table/index/scalar/zonemap/ -->

# Zone Map Index¶

Zone maps are a columnar database technique for predicate pushdown and scan pruning. They break data into fixed-size chunks called "zones" and maintain summary statistics (min, max, null count) for each zone, enabling efficient filtering by eliminating zones that cannot contain matching values.

Zone maps are "inexact" filters - they can definitively exclude zones but may include false positives that require rechecking.

## Index Details¶
    
    
    message ZoneMapIndexDetails {
    
    }
    

## Storage Layout¶

The zone map index stores zone statistics in a single file:

  1. `zonemap.lance` \- Zone statistics for query pruning


### Zone Statistics File Schema¶

Column | Type | Nullable | Description  
---|---|---|---  
`min` | {DataType} | true | Minimum value in the zone  
`max` | {DataType} | true | Maximum value in the zone  
`null_count` | UInt32 | false | Number of null values in the zone  
`nan_count` | UInt32 | false | Number of NaN values (for float types)  
`fragment_id` | UInt64 | false | Fragment containing this zone  
`zone_start` | UInt64 | false | Starting row offset within the fragment  
`zone_length` | UInt32 | false | Number of rows in this zone  
  
### Schema Metadata¶

Key | Type | Description  
---|---|---  
`rows_per_zone` | String | Number of rows per zone (default: "8192")  
  
## Accelerated Queries¶

The zone map index provides inexact results for the following query types:

Query Type | Description | Operation | Result Type  
---|---|---|---  
**Equals** | `column = value` | Includes zones where min ≤ value ≤ max | AtMost  
**Range** | `column BETWEEN a AND b` | Includes zones where ranges overlap | AtMost  
**IsIn** | `column IN (v1, v2, ...)` | Includes zones that could contain any value | AtMost  
**IsNull** | `column IS NULL` | Includes zones where null_count > 0 | AtMost  
  
Back to top



---

<!-- Source: https://lance.org/format/table/index/scalar/bloom_filter/ -->

# Bloom Filter Index¶

Bloom filters are probabilistic data structures that allow for fast membership testing. They are space-efficient and can test whether an element is a member of a set. It's an inexact filter - they may include false positives but never false negatives.

## Index Details¶
    
    
    message BloomFilterIndexDetails {
    
    }
    

## Storage Layout¶

The bloom filter index stores zone-based bloom filters in a single file:

  1. `bloomfilter.lance` \- Bloom filter statistics and data for each zone


### Bloom Filter File Schema¶

Column | Type | Nullable | Description  
---|---|---|---  
`fragment_id` | UInt64 | false | Fragment containing this zone  
`zone_start` | UInt64 | false | Starting row offset within the fragment  
`zone_length` | UInt64 | false | Number of rows in this zone  
`has_null` | Boolean | false | Whether this zone contains any null values  
`bloom_filter_data` | Binary | false | Serialized SBBF (Split Block Bloom Filter) data  
  
### Schema Metadata¶

Key | Type | Description  
---|---|---  
`bloomfilter_item` | String | Expected number of items per zone (default: "8192")  
`bloomfilter_probability` | String | False positive probability (default: "0.00057", ~1 in 1754)  
  
## Bloom Filter Spec¶

The bloom filter index uses a Split Block Bloom Filter (SBBF) implementation, which is optimized for SIMD operations.

### SBBF Structure¶

The SBBF divides the bit array into blocks of 256 bits, where each block consists of 8 contiguous 32-bit words. This structure enables efficient SIMD operations and cache-friendly memory access patterns. The block layout is the following:

  * **Block size** : 256 bits (32 bytes)
  * **Words per block** : 8 × 32-bit integers
  * **Minimum filter size** : 32 bytes (1 block)
  * **Maximum filter size** : 128 MiB


### Hashing Mechanism¶

The SBBF uses xxHash64 with seed=0 for primary hashing, combined with a salt-based secondary hashing scheme:

  1. **Primary hash** : xxHash64(value) → 64-bit hash
  2. **Block selection** : Upper 32 bits determine which block to use
  3. **Bit selection** : Lower 32 bits combined with 8 salt values set 8 bits in the block


#### Salt Values¶
    
    
    0x47b6137b
    0x44974d91
    0x8824ad5b
    0xa2b7289d
    0x705495c7
    0x2df1424b
    0x9efc4947
    0x5c6bfb31
    

Each salt value generates one bit position within the block, ensuring uniform distribution.

### Filter Sizing Algorithm¶

The SBBF automatically determines optimal filter size based on: \- **NDV** (Number of Distinct Values): Expected unique items \- **FPP** (False Positive Probability): Target error rate

The implementation uses binary search to find the minimum log₂(bytes) that achieves the desired FPP, using Putze et al.'s cache-efficient bloom filter formula.

#### FPP Convergence¶

The implementation uses up to 750 iterations of Poisson distribution calculations to ensure accurate FPP estimation, particularly for dense filters where NDV approaches filter capacity.

### Serialization¶

The SBBF is serialized as a contiguous byte array stored in the `bloom_filter_data` column:
    
    
    [Block 0][Block 1]...[Block N-1]
    

Where each block is 32 bytes:
    
    
    [Word 0][Word 1][Word 2][Word 3][Word 4][Word 5][Word 6][Word 7]
    

Each word is a 32-bit little-endian integer (4 bytes), with:

  * **Total size** : Must be a multiple of 32 bytes
  * **Byte order** : Little-endian for all 32-bit words
  * **Block alignment** : Each block starts at offset `i * 32`
  * **Word offset** : Word `j` in block `i` is at byte offset `i * 32 + j * 4`


#### Example¶

For a filter with 2 blocks (64 bytes total): 
    
    
    Offset  0-3:   Block 0, Word 0 (32-bit LE)
    Offset  4-7:   Block 0, Word 1 (32-bit LE)
    ...
    Offset 28-31:  Block 0, Word 7 (32-bit LE)
    Offset 32-35:  Block 1, Word 0 (32-bit LE)
    ...
    Offset 60-63:  Block 1, Word 7 (32-bit LE)
    

## Accelerated Queries¶

The bloom filter index provides inexact results for the following query types:

Query Type | Description | Operation | Result Type  
---|---|---|---  
**Equals** | `column = value` | Tests if value exists in bloom filter | AtMost  
**IsIn** | `column IN (v1, v2, ...)` | Tests if any value exists in bloom filter | AtMost  
**IsNull** | `column IS NULL` | Returns zones where has_null is true | AtMost  
  
Back to top



---

<!-- Source: https://lance.org/format/table/index/scalar/fts/ -->

# Full Text Search Index¶

The full text search (FTS) index (a.k.a. inverted index) provides efficient text search by mapping terms to the documents containing them. It's designed for high-performance text search with support for various scoring algorithms and phrase queries.

## Index Details¶
    
    
    message InvertedIndexDetails {
      // Marking this field as optional as old versions of the index store blank details and we
      // need to make sure we have a proper optional field to detect this.
      optional string base_tokenizer = 1;
      string language = 2;
      bool with_position = 3;
      optional uint32 max_token_length = 4;
      bool lower_case = 5;
      bool stem = 6;
      bool remove_stop_words = 7;
      bool ascii_folding = 8;
      uint32 min_ngram_length = 9;
      uint32 max_ngram_length = 10;
      bool prefix_only = 11;
    
    }
    

## Storage Layout¶

The FTS index consists of multiple files storing the token dictionary, document information, and posting lists:

  1. `tokens.lance` \- Token dictionary mapping tokens to token IDs
  2. `docs.lance` \- Document metadata including token counts
  3. `invert.lance` \- Compressed posting lists for each token
  4. `metadata.lance` \- Index metadata and configuration


An FTS index may contain multiple partitions. Each partition has its own set of token, document, and posting list files, prefixed with the partition ID (e.g. `part_0_tokens.lance`, `part_0_docs.lance`, `part_0_invert.lance`). The `metadata.lance` file lists all partition IDs in the index. At query time, every partition must be searched and the results combined to produce the final ranked output. Fewer partitions generally means better query performance, since each partition requires its own token dictionary lookup and posting list scan. The number of partitions is controlled by the training configuration -- specifically `LANCE_FTS_TARGET_SIZE` determines how large each merged partition can grow (see Training Process for details).

### Token Dictionary File Schema¶

Column | Type | Nullable | Description  
---|---|---|---  
`_token` | Utf8 | false | The token string  
`_token_id` | UInt32 | false | Unique identifier for the token  
  
### Document File Schema¶

Column | Type | Nullable | Description  
---|---|---|---  
`_rowid` | UInt64 | false | Document row ID  
`_num_tokens` | UInt32 | false | Number of tokens in the document  
  
### FTS List File Schema¶

Column | Type | Nullable | Description  
---|---|---|---  
`_posting` | List | false | Compressed posting lists (delta-encoded row IDs and frequencies)  
`_max_score` | Float32 | false | Maximum score for the token (for query optimization)  
`_length` | UInt32 | false | Number of documents containing the token  
`_compressed_position` | List> | true | Optional compressed position lists for phrase queries  
  
### Metadata File Schema¶

The metadata file contains JSON-serialized configuration and partition information:

Key | Type | Description  
---|---|---  
`partitions` | Array | List of partition IDs for distributed index organization  
`params` | JSON Object | Serialized InvertedIndexParams with tokenizer config  
  
#### InvertedIndexParams Structure¶

Field | Type | Default | Description  
---|---|---|---  
`base_tokenizer` | String | "simple" | Base tokenizer type (see Tokenizers section)  
`language` | String | "English" | Language for stemming and stop words  
`with_position` | Boolean | false | Store term positions for phrase queries (increases index size)  
`max_token_length` | UInt32? | None | Maximum token length (tokens longer than this are removed)  
`lower_case` | Boolean | true | Convert tokens to lowercase  
`stem` | Boolean | false | Apply language-specific stemming  
`remove_stop_words` | Boolean | false | Remove common stop words for the specified language  
`ascii_folding` | Boolean | true | Convert accented characters to ASCII equivalents  
`min_gram` | UInt32 | 2 | Minimum n-gram length (only for ngram tokenizer)  
`max_gram` | UInt32 | 15 | Maximum n-gram length (only for ngram tokenizer)  
`prefix_only` | Boolean | false | Generate only prefix n-grams (only for ngram tokenizer)  
  
## Tokenizers¶

The full text search index supports multiple tokenizer types for different text processing needs:

### Base Tokenizers¶

Tokenizer | Description | Use Case  
---|---|---  
**simple** | Splits on whitespace and punctuation, removes non-alphanumeric characters | General text (default)  
**whitespace** | Splits only on whitespace characters | Preserve punctuation  
**raw** | No tokenization, treats entire text as single token | Exact matching  
**ngram** | Breaks text into overlapping character sequences | Substring/fuzzy search  
**jieba/** * | Chinese text tokenizer with word segmentation | Chinese text  
**lindera/** * | Japanese text tokenizer with morphological analysis | Japanese text  
  
#### Jieba Tokenizer (Chinese)¶

Jieba is a popular Chinese text segmentation library that uses a dictionary-based approach with statistical methods for word segmentation.

  * **Configuration** : Uses a `config.json` file in the model directory
  * **Models** : Must be downloaded and placed in the Lance home directory under `jieba/`
  * **Usage** : Specify as `jieba/<model_name>` or just `jieba` for the default model
  * **Config Structure** : 
        
        {
          "main": "path/to/main/dictionary",
          "users": ["path/to/user/dict1", "path/to/user/dict2"]
        }
        

  * **Features** :
  * Accurate word segmentation for Simplified and Traditional Chinese
  * Support for custom user dictionaries
  * Multiple segmentation modes (precise, full, search engine)


#### Lindera Tokenizer (Japanese)¶

Lindera is a morphological analysis tokenizer specifically designed for Japanese text. It provides proper word segmentation for Japanese, which doesn't use spaces between words.

  * **Configuration** : Uses a `config.yml` file in the model directory
  * **Models** : Must be downloaded and placed in the Lance home directory under `lindera/`
  * **Usage** : Specify as `lindera/<model_name>` where `<model_name>` is the subdirectory containing the model files
  * **Features** :
  * Morphological analysis with part-of-speech tagging
  * Dictionary-based tokenization
  * Support for custom user dictionaries


### Token Filters¶

Token filters are applied in sequence after the base tokenizer:

Filter | Description | Configuration  
---|---|---  
**RemoveLong** | Removes tokens exceeding max_token_length | `max_token_length`  
**LowerCase** | Converts tokens to lowercase | `lower_case` (default: true)  
**Stemmer** | Reduces words to their root form | `stem`, `language`  
**StopWords** | Removes common words like "the", "is", "at" | `remove_stop_words`, `language`  
**AsciiFolding** | Converts accented characters to ASCII | `ascii_folding` (default: true)  
  
### Supported Languages¶

For stemming and stop word removal, the following languages are supported: Arabic, Danish, Dutch, English, Finnish, French, German, Greek, Hungarian, Italian, Norwegian, Portuguese, Romanian, Russian, Spanish, Swedish, Tamil, Turkish

## Document Type¶

Lance supports 2 kinds of documents: text and json. Different document types have different tokenization rules, and parse tokens in different format.

### Text Type¶

Text type includes text and list of text. Tokens are generated by base_tokenizer.

The example below shows how text document is parsed into tokens. 
    
    
    Tom lives in San Francisco.
    

The tokens are below. 
    
    
    Tom
    lives
    in
    San
    Francisco
    

### Json Type¶

Json is a nested structure, lance breaks down json document into tokens in triplet format `path,type,value`. The valid types are: str, number, bool, null.

In scenarios where the triplet value is a str, the text value will be further tokenized using the base_tokenizer, resulting in multiple triplet tokens.

During querying, the Json Tokenizer uses the triplet format instead of the json format, which simplifies the query syntax.

The example below shows how the json document is tokenized. Assume we have the following json document: 
    
    
    {
      "name": "Lance",
      "legal.age": 30,
      "address": {
        "city": "San Francisco",
        "zip:us": 94102
      }
    }
    

After parsing, the document will be tokenized into the following tokens: 
    
    
    name,str,Lance
    legal.age,number,30
    address.city,str,San
    address.city,str,Francisco
    address.zip:us,number,94102
    

Then we do full text search in triplet format. To search for "San Francisco," we can search with one of the triplets below: 
    
    
    address.city:San Francisco
    address.city:San
    address.city:Francisco
    

## Training Process¶

Building an FTS index is a multi-phase pipeline: the source column is scanned, documents are tokenized in parallel, intermediate results are spilled to part files on disk, and the part files are merged into final output partitions.

### Phase 1: Tokenization¶

The input column is read as a stream of record batches and dispatched to a pool of tokenizer worker tasks. Each worker tokenizes documents independently, accumulating tokens, posting lists, and document metadata in memory.

When a worker's accumulated data reaches the partition size limit or the document count hits `u32::MAX`, it flushes the data to disk as a set of part files (`part_<id>_tokens.lance`, `part_<id>_invert.lance`, `part_<id>_docs.lance`). A single worker may produce multiple part files if it processes enough data.

### Phase 2: Merge¶

After all workers finish, the part files are merged into output partitions. Part files are streamed with bounded buffering so that not all data needs to be loaded into memory at once. For each part file, the token dictionaries are unified, document sets are concatenated, and posting lists are rewritten with adjusted IDs.

When a merged partition reaches the target size, it is written to the destination store and a new one is started. After all part files are consumed the final partition is flushed, and a `metadata.lance` file is written listing the partition IDs and index parameters.

### Configuration¶

Environment Variable | Default | Description  
---|---|---  
`LANCE_FTS_NUM_SHARDS` | Number of compute-intensive CPUs | Number of parallel tokenizer worker tasks. Higher values increase indexing throughput but use more memory.  
`LANCE_FTS_PARTITION_SIZE` | 256 (MiB) | Maximum uncompressed size of a worker's in-memory buffer before it is spilled to a part file.  
`LANCE_FTS_TARGET_SIZE` | 4096 (MiB) | Target uncompressed size for merged output partitions. Fewer, larger partitions improve query performance.  
  
### Memory and Performance Considerations¶

Memory usage is primarily determined by two factors:

  * **`LANCE_FTS_NUM_SHARDS`** \-- Each worker holds an independent in-memory buffer. Peak memory is roughly `NUM_SHARDS * PARTITION_SIZE` plus the overhead of token dictionaries and posting list structures.
  * **`LANCE_FTS_PARTITION_SIZE`** \-- Larger values reduce the number of part files and make the merge phase cheaper. Smaller values reduce per-worker memory at the cost of more part files.


Merge phase memory is bounded by the streaming approach: part files are loaded one at a time with a small concurrency buffer. The merged partition's in-memory size is bounded by `LANCE_FTS_TARGET_SIZE`.

Building an FTS index requires temporary disk space to store the part files generated during tokenization. The amount of temporary space depends heavily on whether position information is enabled. An index with `with_position: true` stores the position of every token occurrence in every document, which can easily require 10x the size of the original column or more in temporary disk space. An index without positions tends to be smaller than the original column and will typically need less than 2x the size of the column in total disk space.

Performance tips:

  * Larger `LANCE_FTS_TARGET_SIZE` produces fewer output partitions, which is beneficial for query performance because queries must scan every partition's token dictionary. When memory allows, prefer fewer, larger partitions.
  * `with_position: true` significantly increases index size because term positions are stored for every occurrence. Only enable it when phrase queries are needed.
  * The ngram tokenizer generates many more tokens per document than word-level tokenizers, so expect larger index sizes and higher memory usage.


### Distributed Training¶

The FTS index supports distributed training where different worker nodes each index a subset of the data and the results are assembled afterward.

  1. Each distributed worker is assigned a **fragment mask** (`(fragment_id as u64) << 32`) that is OR'd into the partition IDs it generates, ensuring globally unique IDs across workers.
  2. Workers set `skip_merge: true` so they write their part files directly without running the merge phase.
  3. Instead of a single `metadata.lance`, each worker writes per-partition metadata files named `part_<id>_metadata.lance`.
  4. After all workers finish, a coordinator merges the metadata files: it collects all partition IDs, remaps them to a sequential range starting from 0 (renaming the corresponding data files), and writes the final unified `metadata.lance`.


This allows each worker to operate independently during the tokenization phase. Only the final metadata merge requires a single-node step, and it is lightweight since it only renames files and writes a small metadata file.

## Accelerated Queries¶

Lance SDKs provide dedicated full text search APIs to leverage the FTS index capabilities. These APIs support complex query types beyond simple token matching, enabling sophisticated text search operations. Here are the query types enabled by the FTS index:

Query Type | Description | Example Usage | Result Type  
---|---|---|---  
**contains_tokens** | Basic token-based search (UDF) with BM25 scoring and automatic result ranking | SQL: `contains_tokens(column, 'search terms')` | AtMost  
**match** | Match query with configurable AND/OR operators and relevance scoring | `{"match": {"query": "text", "operator": "and/or"}}` | AtMost  
**phrase** | Exact phrase matching with position information (requires `with_position: true`) | `{"phrase": {"query": "exact phrase"}}` | AtMost  
**boolean** | Complex boolean queries with must/should/must_not clauses for sophisticated search logic | `{"boolean": {"must": [...], "should": [...]}}` | AtMost  
**multi_match** | Search across multiple fields simultaneously with unified scoring | `{"multi_match": [{"field1": "query"}, ...]}` | AtMost  
**boost** | Boost relevance scores for specific terms or queries by a configurable factor | `{"boost": {"query": {...}, "factor": 2.0}}` | AtMost  
  
Back to top



---

<!-- Source: https://lance.org/format/table/index/scalar/ngram/ -->

# N-gram Index¶

N-gram indices break text into overlapping sequences (trigrams) for efficient substring matching. They provide fast text search by indexing all 3-character sequences in the text after applying ASCII folding and lowercasing.

## Index Details¶
    
    
    message NGramIndexDetails {
    
    }
    

## Storage Layout¶

The N-gram index stores tokenized text as trigrams with their posting lists:

  1. `ngram_postings.lance` \- Trigram tokens and their posting lists


### File Schema¶

Column | Type | Nullable | Description  
---|---|---|---  
`tokens` | UInt32 | true | Hashed trigram token  
`posting_list` | Binary | false | Compressed bitmap of row IDs containing the token  
  
## Accelerated Queries¶

The N-gram index provides inexact results for the following query types:

Query Type | Description | Operation | Result Type  
---|---|---|---  
**contains** | Substring search in text | Finds all trigrams in query, intersects posting lists | AtMost  
  
Back to top



---

<!-- Source: https://lance.org/format/table/index/scalar/rtree/ -->

# R-Tree Index¶

The R-Tree index is a static, immutable 2D spatial index. It is built on bounding boxes to organize the data. This index is intended to accelerate rectangle-based pruning.

It is designed as a multi-level hierarchical structure: leaf pages store tuples `(bbox, id=rowid)` for indexed geometries; branch pages aggregate child bounding boxes and store `id=pageid` pointing to child pages; a single root page encloses the entire tree. Conceptually, it can be thought of as an extension of the B+-tree to multidimensional objects, where bounding boxes act as keys for spatial pruning.

The index uses a packed-build strategy where items are first sorted and then grouped into fixed-size leaf pages.

This packed-build flow is: \- Sort items (bboxes) according to the sorting algorithm. \- Pack consecutive items into leaf pages of `page_size` entries; then build parent pages bottom-up by aggregating child page bboxes.

## Sorting¶

Sorting does not change the R-Tree data structure, but it is critical to performance. Currently, Hilbert sorting is implemented, but the design is extensible to other spatial sorting algorithms.

### Hilbert Curve Sorting¶

Hilbert sorting imposes a linear order on 2D items using a space-filling Hilbert curve to maximize locality in both axes. This improves leaf clustering, which benefits query pruning.

Hilbert sorting is performed in three steps:

  1. **Global bounding box** : compute the global bbox `[xmin_g, ymin_g, xmax_g, ymax_g]` over all items for training index.
  2. **Normalize and compute Hilbert value** :
     * For each item bbox `[xmin_i, ymin_i, xmax_i, ymax_i]`, compute its center:
       * `cx = (xmin_i + xmax_i) / 2`
       * `cy = (ymin_i + ymax_i) / 2`
     * Map the center to a 16‑bit grid per axis using the global bbox. Let `W = xmax_g - xmin-g` and `H = ymax_g - ymin_g`. The normalized integer coordinates are:
       * `xi = round(((cx - xmin_g) / W) * (2^16 - 1))`
       * `yi = round(((cy - ymin_g) / H) * (2^16 - 1))`
     * If the global width or height is effectively zero, the corresponding axis is treated as degenerate and set to `0` for all items (the ordering then degenerates to 1D on the other axis).
     * For each `(xi, yi)` in `[0 .. 2^16-1] × [0 .. 2^16-1]`, compute a 32‑bit Hilbert value using a standard 2D Hilbert algorithm. In pseudocode (with `bits = 16`): 
           
           fn hilbert_value(x, y, bits):
               # x, y: integers in [0 .. 2^bits - 1]
               h = 0
               mask = (1 << bits) - 1
           
               for s from bits-1 down to 0:
                   rx = (x >> s) & 1
                   ry = (y >> s) & 1
                   d  = ((3 * rx) XOR ry) << (2 * s)
                   h  = h | d
           
                   if ry == 0:
                       if rx == 1:
                           x = (~x) & mask
                           y = (~y) & mask
                       swap(x, y)
           
               return h
           

     * The resulting `h` is stored as the item’s Hilbert value (type `u32` with `bits = 16`).
  3. **Sort** : sort items by Hilbert value.


## Index Details¶
    
    
    message RTreeIndexDetails {
    
    }
    

## Storage Layout¶

The R-Tree index consists of two files:

  1. `page_data.lance` \- Stores all pages (leaf, branch) as repeated `(bbox, id)` tuples, written bottom-up (leaves first, then branch levels)
  2. `nulls.lance` \- Stores a serialized RowAddrTreeMap of rows with null


### Page File Schema¶

Column | Type | Nullable | Description  
---|---|---|---  
`bbox` | RectType | false | Type is Rect defined by [geoarrow-rs](<https://github.com/geoarrow/geoarrow-rs>) RectType; physical storage is Struct. Represents the node bounding box (leaf: item bbox; branch: child aggregation).  
`id` | UInt64 | false | Reuse the `id` column to store `rowid` in leaf pages and `pageid` in branch pages  
  
### Nulls File Schema¶

Column | Type | Nullable | Description  
---|---|---|---  
`nulls` | Binary | false | Serialized RowAddrTreeMap of rows with null/invalid geometry  
  
### Schema Metadata¶

The following optional keys can be used by implementations and are stored in the schema metadata:

Key | Type | Description  
---|---|---  
`page_size` | String | Page size per page  
`num_pages` | String | Total number of pages written  
`num_items` | String | Number of non-null leaf items in the index  
`bbox` | String | JSON-serialized global BoundingBox of the dataset  
  
### Query Traversal¶

This index serializes the multi-level hierarchical RTree structure into a single page file following the schema above. At lookup time, the reader computes each page offset using the algorithm below and reconstructs the hierarchy for traversal.

Offsets are derived from `num_items` and `page_size` of metadata as follows:

  * Leaf: `leaf_pages = ceil(num_items / page_size)`; leaf `i` has `page_offset = i * page_size`.
  * Branch: let `level_offset` be the starting offset for current level, which actually represents total items from all lower levels; let `prev_pages` be pages in the level below; `level_pages = ceil(prev_pages / page_size)`. For branch `j`, `page_offset = j * page_size + level_offset`.
  * Iterate levels until one page remains; the root is the last page and has `pageid = num_pages - 1`.
  * Page lengths: once all page offsets are collected, compute each `page_len` by the next offset difference; for the final page (root), `page_len = page_file_total_rows - page_offset` (where `page_file_total_rows` is total rows in `page_data.lance`).


Traversal starts from the root (`pageid = num_pages - 1`):

  * If `page_offset < num_items` (leaf), read items `[page_offset .. page_offset + page_len)` and emit candidate `rowid`s matching the query bbox.
  * Otherwise (branch), descend into children whose bounding boxes match the query bbox.
  * Continue until there are no more pages to visit; the union of emitted `rowid`s forms the candidate set for evaluation.


## Accelerated Queries¶

The R-Tree index accelerates the following query types by returning a candidate set of matching bounding boxes. Exact geometry verification must be performed by the execution engine.

Query Type | Description | Operation | Result Type  
---|---|---|---  
**Intersects** | `St_Intersects(col, geom)` | Prunes candidates by bbox intersection | AtMost  
**Contains** | `St_Contains(col, geom)` | Prunes candidates by bbox containment | AtMost  
**Within** | `St_Within(col, geom)` | Prunes candidates by bbox within relation | AtMost  
**Touches** | `St_Touches(col, geom)` | Prunes candidates by bbox touch relation | AtMost  
**Crosses** | `St_Crosses(col, geom)` | Prunes candidates by bbox crossing relation | AtMost  
**Overlaps** | `St_Overlaps(col, geom)` | Prunes candidates by bbox overlap relation | AtMost  
**Covers** | `St_Covers(col, geom)` | Prunes candidates by bbox cover relation | AtMost  
**CoveredBy** | `St_Coveredby(col, geom)` | Prunes candidates by bbox covered-by relation | AtMost  
**IsNull** | `col IS NULL` | Returns rows recorded in the nulls file | Exact  
  
Back to top



---

<!-- Source: https://lance.org/format/table/index/vector/ -->

# Vector Indices¶

Lance provides a powerful and extensible secondary index system for efficient vector similarity search. All vector indices are stored as regular Lance files, making them portable and easy to manage. It is designed for efficient similarity search across large-scale vector datasets.

## Concepts¶

Lance splits each vector index into 3 parts - clustering, sub-index and quantization.

### Clustering¶

Clustering divides all the vectors into different disjoint clusters (a.k.a. partitions). Lance currently supports using Inverted File (IVF) as the primary clustering mechanism. IVF partitions the vectors into clusters using the k-means clustering algorithm. Each cluster contains vectors that are similar to the cluster centroid. During search, only the most relevant clusters are examined, dramatically reducing search time. IVF can be combined with any sub-index type and quantization method.

### Sub-Index¶

The sub-index determines how vectors are organized for search. Lance currently supports:

  * **FLAT** : Exact search with no approximation - scans all vectors
  * **HNSW** : Hierarchical Navigable Small World graphs for fast approximate search


### Quantization¶

The quantization method determines how vectors are stored and compressed. Lance currently supports:

  * **Product Quantization (PQ)** : Compresses vectors by splitting them into smaller sub-vectors and quantizing each independently
  * **Scalar Quantization (SQ)** : Applies scalar quantization to each dimension of the vector independently
  * **RabitQ (RQ)** : Uses random rotation and binary quantization for extreme compression
  * **FLAT** : No quantization, keeps original vectors for exact search


### Common Combinations¶

When we refer to an index type, it is typically `{clustering}_{sub_index}_{quantization}`. If sub-index is just `FLAT`, we usually omit it and just refer to it by `{clustering}_{quantization}`. Here are the commonly used combinations:

Index Type | Name | Description  
---|---|---  
**IVF_PQ** | Inverted File with Product Quantization | Combines IVF clustering with PQ compression for efficient storage and search  
**IVF_HNSW_SQ** | Inverted File with HNSW and Scalar Quantization | Uses IVF for coarse clustering and HNSW for fine-grained search with scalar quantization  
**IVF_SQ** | Inverted File with Scalar Quantization | Combines IVF clustering with scalar quantization for balanced compression  
**IVF_RQ** | Inverted File with RabitQ | Combines IVF clustering with RabitQ for extreme compression using binary quantization  
**IVF_FLAT** | Inverted File without quantization | Uses IVF clustering with exact vector storage for precise search within clusters  
  
### Versioning¶

The Lance vector index format has gone through 3 versions so far. This document currently only records version 3 which is the latest version. The specific version of the vector index is recorded in the `index_version` field of the generic [index metadata](<../#loading-an-index>).

## Storage Layout (V3)¶

Each vector index is stored as 2 regular Lance files - index file and auxiliary file.

### Index File¶

The index structure file containing the search graph/structure with index-specific schema. It is stored as a Lance file with name `index.idx` within the index directory.

#### Arrow Schema¶

The index file stores the search structure with graph or flat organization. The Arrow schema of the Lance file varies depending on the sub-index type used.

Note

All partitions are stored in the same file, and partitions must be written in order.

##### FLAT¶

FLAT indices perform exact search with no approximation. This is essentially an empty file with a minimal schema:

Column | Type | Nullable | Description  
---|---|---|---  
`__flat_marker` | uint64 | false | Marker field for FLAT index (no actual data)  
  
##### HNSW¶

HNSW (Hierarchical Navigable Small World) indices provide fast approximate search through a multi-level graph structure. This stores the HNSW graph with the following schema:

Column | Type | Nullable | Description  
---|---|---|---  
`__vector_id` | uint64 | false | Vector identifier  
`__neighbors` | list | false | Neighbor node IDs  
`_distance` | list | false | Distances to neighbors  
  
Note

HNSW consists of multiple levels, and all levels must be written in order starting from level 0.

#### Arrow Schema Metadata¶

The index file contains metadata in its Arrow schema metadata to describe the index configuration and structure. Here are the metadata keys and their corresponding values:

##### "lance:index"¶

Contains basic index configuration information in JSON:

JSON Key | Type | Expected Values  
---|---|---  
`type` | String | Index type (e.g., "IVF_PQ", "IVF_RQ", "IVF_HNSW", "FLAT")  
`distance_type` | String | Distance metric (e.g., "l2", "cosine", "dot")  
  
##### "lance:ivf"¶

References the IVF metadata stored in the Lance file global buffer. This value records the global buffer index, currently this is always "1".

Note

Global buffer indices in Lance files are 1-based, so you need to subtract 1 when accessing them through code.

##### "lance:flat"¶

Contains partition-specific metadata for the `FLAT` sub-index structure. This is an empty string since FLAT indices don't require additional metadata at this moment.

##### "lance:hnsw"¶

Contains the HNSW-specific JSON metadata for each partition, including graph structure information:

JSON Key | Type | Expected Values  
---|---|---  
`entry_point` | u32 | Starting node for graph traversal  
`params` | Object | HNSW construction parameters (see below)  
`level_offsets` | Array | Offset for each level in the graph  
  
The `params` object contains the following HNSW construction parameters:

JSON Key | Type | Description | Default  
---|---|---|---  
`max_level` | u16 | Maximum level of the HNSW graph | 7  
`m` | usize | Number of connections to establish while inserting new element | 20  
`ef_construction` | usize | Size of the dynamic list for candidates | 150  
`prefetch_distance` | Option | Number of vectors ahead to prefetch while building | Some(2)  
  
#### Lance File Global Buffer¶

##### IVF Metadata¶

For efficiency, Lance serializes IVF metadata to protobuf format and stores it in the Lance file global buffer:
    
    
    message IVF {
      // Centroids of partitions. `dimension * num_partitions` of float32s.
      //
      // Deprecated, use centroids_tensor instead.
      repeated float centroids = 1;  // [deprecated = true];
    
      // File offset of each partition.
      repeated uint64 offsets = 2;
    
      // Number of records in the partition.
      repeated uint32 lengths = 3;
    
      // Tensor of centroids. `num_partitions * dimension` of float32s.
      Tensor centroids_tensor = 4;
    
      // KMeans loss.
      optional double loss = 5;
    
    }
    

### Auxiliary File¶

The auxiliary file is a vector storage for quantized vectors. It is stored as a Lance file named `auxiliary.idx` within the index directory.

#### Arrow Schema¶

Since the auxiliary file stores the actual (quantized) vectors, the Arrow schema of the Lance file varies depending on the quantization method used.

Note

All partitions are stored in the same file, and partitions must be written in order.

##### FLAT¶

No quantization applied - stores original vectors in their full precision:

Column | Type | Nullable | Description  
---|---|---|---  
`_rowid` | uint64 | false | Row identifier  
`flat` | list[dimension] | false | Original vector values (list_size = vector dimension)  
  
##### PQ¶

Compresses vectors using product quantization for significant memory savings:

Column | Type | Nullable | Description  
---|---|---|---  
`_rowid` | uint64 | false | Row identifier  
`__pq_code` | list[m] | false | PQ codes (list_size = number of subvectors)  
  
##### SQ¶

Compresses vectors using scalar quantization for moderate memory savings:

Column | Type | Nullable | Description  
---|---|---|---  
`_rowid` | uint64 | false | Row identifier  
`__sq_code` | list[dimension] | false | SQ codes (list_size = vector dimension)  
  
##### RQ¶

Compresses vectors using RabitQ with random rotation and binary quantization for extreme compression:

Column | Type | Nullable | Description  
---|---|---|---  
`_rowid` | uint64 | false | Row identifier  
`_rabit_codes` | list[dimension / 8] | false | Binary quantized codes (1 bit per dimension, packed into bytes)  
`__add_factors` | float32 | false | Additive correction factors for distance computation  
`__scale_factors` | float32 | false | Scale correction factors for distance computation  
  
#### Arrow Schema Metadata¶

The auxiliary file also contains metadata in its Arrow schema metadata for vector storage configuration. Here are the metadata keys and their corresponding values:

##### "distance_type"¶

The distance metric used to compute similarity between vectors (e.g., "l2", "cosine", "dot").

##### "lance:ivf"¶

Similar to the index file's "lance:ivf" but focused on vector storage layout. This doesn't contain the partitions' centroids. It's only used for tracking each partition's offset and length in the auxiliary file.

##### "lance:rabit"¶

Contains RabitQ-specific metadata in JSON format (only present for RQ quantization). This includes the rotation matrix position, number of bits, and packing information. See the RQ metadata specification in the "storage_metadata" section below.

##### "storage_metadata"¶

Contains quantizer-specific metadata as a list of JSON strings. Currently, the list always contains exactly 1 element with the quantizer metadata.

For **Product Quantization (PQ)** :

JSON Key | Type | Description  
---|---|---  
`codebook_position` | usize | Position of the codebook in the global buffer  
`nbits` | u32 | Number of bits per subvector code (e.g., 8 bits = 256 codewords)  
`num_sub_vectors` | usize | Number of subvectors (m)  
`dimension` | usize | Original vector dimension  
`transposed` | bool | Whether the codebook is stored in transposed layout  
  
For **Scalar Quantization (SQ)** :

JSON Key | Type | Description  
---|---|---  
`dim` | usize | Vector dimension  
`num_bits` | u16 | Number of bits for quantization  
`bounds` | Range | Min/max bounds for scalar quantization  
  
For **RabitQ (RQ)** :

JSON Key | Type | Description  
---|---|---  
`rotate_mat_position` | u32 | Position of the rotation matrix in the global buffer  
`num_bits` | u8 | Number of bits per dimension (currently always 1)  
`packed` | bool | Whether codes are packed for optimized computation  
  
#### Lance File Global Buffer¶

##### Quantization Codebook¶

For product quantization, the codebook is stored in `Tensor` format in the auxiliary file's global buffer for efficient access:
    
    
    message Tensor {
      enum DataType {
        BFLOAT16 = 0;
        FLOAT16 = 1;
        FLOAT32 = 2;
        FLOAT64 = 3;
        UINT8 = 4;
        UINT16 = 5;
        UINT32 = 6;
        UINT64 = 7;
      }
    
      DataType data_type = 1;
    
      // Data shape, [dim1, dim2, ...]
      repeated uint32 shape = 2;
    
      // Data buffer
      bytes data = 3;
    
    }
    

##### Rotation Matrix¶

For RabitQ, the rotation matrix is stored in `Tensor` format in the auxiliary file's global buffer. The rotation matrix is an orthogonal matrix used to rotate vectors before binary quantization:
    
    
    message Tensor {
      enum DataType {
        BFLOAT16 = 0;
        FLOAT16 = 1;
        FLOAT32 = 2;
        FLOAT64 = 3;
        UINT8 = 4;
        UINT16 = 5;
        UINT32 = 6;
        UINT64 = 7;
      }
    
      DataType data_type = 1;
    
      // Data shape, [dim1, dim2, ...]
      repeated uint32 shape = 2;
    
      // Data buffer
      bytes data = 3;
    
    }
    

The rotation matrix has shape `[code_dim, code_dim]` where `code_dim = dimension * num_bits`.

## Appendices¶

### Appendix 1: Example IVF_PQ Format¶

This example shows how an `IVF_PQ` index is physically laid out. Assume vectors have dimension 128, PQ uses 16 num_sub_vectors (m=16) with 8 num_bits per subvector, and distance type is "l2".

#### Index File¶

  * Arrow Schema Metadata:
  * `"lance:index"` → `{ "type": "IVF_PQ", "distance_type": "l2" }`
  * `"lance:ivf"` → "1" (references IVF metadata in the global buffer)
  * `"lance:flat"` → `["", "", ...]` (one empty string per partition; IVF_PQ uses a FLAT sub-index inside each partition)

  * Lance File Global buffer (Protobuf):

  * `Ivf` message containing:
    * `centroids_tensor`: shape `[num_partitions, 128]` (float32)
    * `offsets`: start offset (row) of each partition in `auxiliary.idx`
    * `lengths`: number of vectors in each partition
    * `loss`: k-means loss (optional)


#### Auxiliary File¶

  * Arrow Schema Metadata:
  * `"distance_type"` → `"l2"`
  * `"lance:ivf"` → tracks per-partition `offsets` and `lengths` (no centroids here)
  * `"storage_metadata"` → `[ "{"pq":{"num_sub_vectors":16,"nbits":8,"dimension":128,"transposed":true}}" ]`
  * Lance File Global buffer:
  * `Tensor` codebook with shape `[256, num_sub_vectors, dim/num_sub_vectors]` = `[256, 16, 8]` (float32)
  * Rows with Arrow schema:


    
    
    pa.schema([
        pa.field("_rowid", pa.uint64()),
        pa.field("__pq_code", pa.list(pa.uint8(), list_size=16)), # m subvector codes
    ])
    

### Appendix 2: Example IVF_RQ Format¶

This example shows how an `IVF_RQ` index is physically laid out. Assume vectors have dimension 128, RQ uses 1 bit per dimension (num_bits=1), and distance type is "l2".

#### Index File¶

  * Arrow Schema Metadata:
  * `"lance:index"` → `{ "type": "IVF_RQ", "distance_type": "l2" }`
  * `"lance:ivf"` → "1" (references IVF metadata in the global buffer)
  * `"lance:flat"` → `["", "", ...]` (one empty string per partition; IVF_RQ uses a FLAT sub-index inside each partition)

  * Lance File Global buffer (Protobuf):

  * `Ivf` message containing:
    * `centroids_tensor`: shape `[num_partitions, 128]` (float32)
    * `offsets`: start offset (row) of each partition in `auxiliary.idx`
    * `lengths`: number of vectors in each partition
    * `loss`: k-means loss (optional)


#### Auxiliary File¶

  * Arrow Schema Metadata:
  * `"distance_type"` → `"l2"`
  * `"lance:ivf"` → tracks per-partition `offsets` and `lengths` (no centroids here)
  * `"lance:rabit"` → `"{"rotate_mat_position":1,"num_bits":1,"packed":true}"`
  * Lance File Global buffer:
  * `Tensor` rotation matrix with shape `[code_dim, code_dim]` = `[128, 128]` (float32)
  * Rows with Arrow schema:


    
    
    pa.schema([
        pa.field("_rowid", pa.uint64()),
        pa.field("_rabit_codes", pa.list(pa.uint8(), list_size=16)), # dimension/8 = 128/8 = 16 bytes
        pa.field("__add_factors", pa.float32()),
        pa.field("__scale_factors", pa.float32()),
    ])
    

### Appendix 3: Accessing Index File with Python¶

The following example demonstrates how to read and parse different components in the Lance index files using Python:
    
    
    import pyarrow as pa
    import lance
    
    # Open the index file
    index_reader = lance.LanceFileReader.read_file("path/to/index.idx")
    
    # Access schema metadata
    schema_metadata = index_reader.metadata().schema.metadata
    
    # Get the IVF metadata reference from schema
    ivf_ref = schema_metadata.get(b"lance:ivf")  # Returns b"1" for global buffer index
    
    # Read the global buffer containing IVF metadata
    if ivf_ref:
        buffer_index = int(ivf_ref) - 1  # Global buffer indices are 1-based
        ivf_buffer = index_reader.global_buffer(buffer_index)
    
        # Parse the protobuf message (requires lance protobuf definitions)
        # ivf_metadata = parse_ivf_protobuf(ivf_buffer)
    
    # For auxiliary file with PQ codebook
    aux_reader = lance.LanceFileReader.read_file("path/to/auxiliary.idx")
    
    # Get storage metadata
    storage_metadata = aux_reader.metadata().schema.metadata.get(b"storage_metadata")
    if storage_metadata:
        import json
        pq_metadata = json.loads(storage_metadata.decode())[0]  # First element of the list
        pq_params = json.loads(pq_metadata)
    
        # Access the codebook from global buffer
        codebook_position = pq_params.get("codebook_position", 1)
        if codebook_position > 0:
            codebook_buffer = aux_reader.global_buffer(codebook_position - 1)
            # Parse the tensor protobuf
            # codebook_tensor = parse_tensor_protobuf(codebook_buffer)
    

Back to top



---

<!-- Source: https://lance.org/format/table/index/system/frag_reuse/ -->

# Fragment Reuse Index¶

The Fragment Reuse Index is an internal index used to optimize fragment operations during compaction and dataset updates.

When data modifications happen against a Lance table, it could trigger compaction and index optimization at the same time to improve data layout and index coverage. By default, compaction will remap all indices at the same time to prevent read regression. This means both compaction and index optimization could modify the same index and cause one process to fail. Typically, the compaction would fail because it has to modify all indices and takes longer, resulting in table layout degrading over time.

Fragment Reuse Index allows a compaction to defer the index remap process. Suppose a compaction removes fragments A and B and produces C. At query runtime, it reuses the old fragments A and B by updating the row addresses related to A and B in the index to the latest ones in C. Because indices are typically cached in memory after initial load, the in-memory index is up to date after the fragment reuse application process.

## Index Details¶
    
    
    message FragmentReuseIndexDetails {
    
      oneof content {
        // if < 200KB, store the content inline, otherwise store the InlineContent bytes in external file
        InlineContent inline = 1;
        ExternalFile external = 2;
      }
    
      message InlineContent {
        repeated Version versions = 1;
      }
    
      message FragmentDigest {
        uint64 id = 1;
    
        uint64 physical_rows = 2;
    
        uint64 num_deleted_rows = 3;
      }
    
      // A summarized version of the RewriteGroup information in a Rewrite transaction
      message Group {
        // A roaring treemap of the changed row addresses.
        // When combined with the old fragment IDs and new fragment IDs,
        // it can recover the full mapping of old row addresses to either new row addresses or deleted.
        // this mapping can then be used to remap indexes or satisfy index queries for the new unindexed fragments.
        bytes changed_row_addrs = 1;
    
        repeated FragmentDigest old_fragments = 2;
    
        repeated FragmentDigest new_fragments = 3;
      }
    
      message Version {
        // The dataset_version at the time the index adds this version entry
        uint64 dataset_version = 1;
    
        repeated Group groups = 3;
      }
    
    }
    

## Expected Use Pattern¶

Fragment Reuse Index should be created if the user defers index remap in compaction. The index accumulates a new **reuse version** every time a compaction is executed.

As long as all the scalar and vector indices are created after the specific reuse version, the indices are all caught up and the specific reuse version can be trimmed.

It is expected that the user schedules an additional process to trim the index periodically to keep the list of reuse versions in control.

Back to top



---

<!-- Source: https://lance.org/format/table/index/system/mem_wal/ -->

# MemWAL Index¶

The MemWAL Index is a system index that serves as the centralized structure for all MemWAL metadata. It stores configuration (region specs, indexes to maintain), merge progress, and region state snapshots.

A table has at most one MemWAL index.

For the complete specification, see:

  * [MemWAL Index Overview](<../../../mem_wal/#memwal-index>) \- Purpose and high-level description
  * [MemWAL Index Details](<../../../mem_wal/#memwal-index-details>) \- Storage format, schemas, and staleness handling
  * [MemWAL Implementation](<../../../mem_wal/#implementation-expectation>) \- Implementation details and expectations


Back to top



---

<!-- Source: https://lance.org/format/namespace/ -->

# Lance Namespace Spec¶

**Lance Namespace** is an open specification for describing access and operations against a collection of tables in a multimodal lakehouse. The spec provides a unified model for table-related objects, their relationships within a hierarchy, and the operations available on these objects, enabling integration with metadata services and compute engines alike.

## What the Lance Namespace Spec Contains¶

The Lance Namespace spec consists of four main parts:

  1. **[Client Spec](<client/>)** : A consistent abstraction that adapts to various catalog specs, allowing users to access and operate on a collection of tables in a multimodal lakehouse. This is the core reason why we call it "Namespace" rather than "Catalog" - namespace can mean catalog, schema, metastore, database, metalake, etc., and the spec provides a unified interface across all of them.

  2. **Native Catalog Specs** : Natively maintained catalog specs that are compliant with the Lance Namespace client spec:

     * **[Directory Namespace Spec](<dir/catalog-spec/>)** : A storage-only catalog spec that requires no external metadata service dependencies — tables are organized directly on storage (local filesystem, S3, GCS, Azure, etc.)
     * **[REST Namespace Spec](<rest/catalog-spec/>)** : A REST-based catalog spec ideal for data infrastructure teams that want to develop their own custom handling in their specific enterprise environments.
  3. **Implementation Specs** : Defines how a given catalog spec integrates with the client spec. It details how an object in a Lance Namespace maps to an object in the specific catalog spec, and how each operation in Lance Namespace is fulfilled by the catalog spec. The implementation specs for Directory and REST namespaces are part of the native Lance Namespace spec. Implementation specs for other catalog specs (e.g. Apache Polaris, Unity Catalog, Apache Hive Metastore, Apache Iceberg REST Catalog) are considered **integrations** \- anyone can provide additional implementation specs outside Lance Namespace, and they can be owned by external parties without needing to go through the Lance community voting process to be adopted.

  4. **[Partitioning Spec](<partitioning-spec/>)** : Defines a storage format for partitioned namespaces built on the Directory Namespace. It enables organizing data into physically separated units (partitions) that share a common schema, with support for partition evolution, pruning, and multi-partition transactions.


## How the Spec Translates to Code¶

For each programming language, a Lance Namespace Client SDK provides a unified interface that compute engines can integrate against. For example, the Java SDK `org.lance:lance-namespace-core` enables engines like Apache Spark, Apache Flink, Apache Kafka, Trino, Presto, etc. to build their Lance connectors.

Each catalog spec has corresponding implementations in supported languages that fulfill the client SDK interface/trait. In this example, the Java implementations for Directory and REST namespaces are in `org.lance:lance-core`, while integrations are provided by dedicated libraries like `org.lance:lance-namespace-polaris` and `org.lance:lance-namespace-unity`.

All implementations must follow their respective implementation specs as the source of truth. This separation of spec and implementation enables both developers to have consistent implementations across different language SDKs, as well as AI code agents to easily generate high-quality, language-specific implementations.

Back to top



---

<!-- Source: https://lance.org/format/namespace/client/object-relationship/ -->

# Objects & Relationships¶

This page describes the objects in a namespace and their relationships with each other.

## Namespace Definition¶

A namespace is a centralized repository for discovering, organizing, and managing tables. It can not only contain a collection of tables, but also a collection of namespaces recursively. It is designed to encapsulates concepts including namespace, metastore, database, schema, etc. that frequently appear in other similar data systems to allow easy integration with any system of any type of object hierarchy.

Here is an example layout of a namespace:

## Parent & Child¶

We use the term **parent** and **child** to describe relationship between 2 objects. If namespace A directly contains B, then A is the parent namespace of B, i.e. B is a child of A. For examples:

  * Namespace `ns1` contains a **child namespace** `ns4`. i.e. `ns1` is the **parent namespace** of `ns4`.
  * Namespace `ns2` contains a **child table** `t2`, i.e. `t2` belongs to **parent namespace** `ns2`.


## Root Namespace¶

A root namespace is a namespace that has no parent. The root namespace is assumed to always exist and is ready to be connected to by a tool to explore objects in the namespace. The lifecycle management (e.g. creation, deletion) of the root namespace is out of scope of this specification.

## Object Name¶

The **name** of an object is a string that uniquely identifies the object within the parent namespace it belongs to. The name of any object must be unique among all other objects that share the same parent namespace. For examples:

  * `cat2`, `cat3` and `cat4` are all unique names under the root namespace
  * `t3` and `t4` are both unique names under `cat4`


## Object Identifier¶

The **identifier** of an object uniquely identifies the object within the root namespace it belongs to. The identifier of any object must be unique among all other objects that share the same root namespace.

Based on the uniqueness property of an object name within its parent namespace, an object identifier is the list of object names starting from (not including) the root namespace to (including) the object itself. This is also called an **list style identifier**. For examples:

  * the list style identifier of `cat5` is `[cat2, cat5]`
  * the list style identifier of `t1` is `[cat2, cat5, t1]`


The dollar (`$`) symbol is used as the default delimiter to join all the names to form an **string style identifier** , but other symbols could also be used if the dollar sign is used in the object name. For examples:

  * the string style identifier of `cat5` is `cat2$cat5`
  * the string style identifier of `t1` is `cat2$cat5$t1`
  * the string style identifier of `t3` is `cat4#t3` when using delimiter `#`


## Name and Identifier for Root Namespace¶

The root namespace itself has no name or identifier. When represented in code, its name and string style identifier is represented by an empty or null string, and its list style identifier is represented by an empty or null list.

The actual name and identifier of the root namespace is typically assigned by users through some configuration when used in a tool. For example, a root namespace can be called `cat1` in Ray, but called `cat2` in Apache Spark, and they are both configured to connect to the same root namespace.

## Object Level¶

The root namespace is always at level 0. This means if an object has list style identifier with list size `N`, the object is at the `N`th level in the entire namespace hierarchy, and its corresponding object identifier has `N` levels. For examples, a namespace `[ns1, ns2]` is at level 2, and its identifier `ns1$ns2` has 2 levels. A table `[catalog1, database2, table3]` is at level 3, and its identifier `catalog1$database2$table3` has 3 levels.

### Leveled Namespace¶

If every table in the root namespace are at the same level `N`, the namespace is called **leveled** , and we say this namespace is a `N`-level namespace. For example, a [directory namespace](<../../dir/catalog-spec/>) is a 1-level namespace, and a Hive 2.x namespace is a 2-level namespace.

Back to top



---

<!-- Source: https://lance.org/format/namespace/client/operations/ -->

# Namespace Operations¶

The Lance Namespace Specification defines a list of operations that can be performed against any Lance namespace.

## OpenAPI Standardization¶

The spec uses [OpenAPI](<https://www.openapis.org/>) to define the request and response models for each operation. This standardization allows clients in any language to generate a client library from the [OpenAPI specification](<https://editor-next.swagger.io/?url=https://raw.githubusercontent.com/lance-format/lance-namespace/refs/heads/main/docs/src/rest.yaml>) and use it to invoke operations with the corresponding request model, receiving responses in the expected response model.

The actual execution of an operation can be:

  * **Client-side** : The operation is executed entirely within the client (e.g., directory namespace)
  * **Server-side** : The operation is sent to a remote server for execution (e.g., REST namespace)
  * **Hybrid** : A combination of both, depending on the integrated catalog spec and service


This flexibility allows the same client interface to work across different namespace implementations while maintaining consistent request/response contracts.

## Duality with REST Namespace Spec¶

The request and response models defined here are designed to work seamlessly with the [REST Namespace](<../../rest/catalog-spec/>) spec. The REST namespace uses these same schemas directly as HTTP request and response bodies, minimizing data conversion between client and server.

This duality explains why certain fields like `id` are marked as optional in the request models:

  * **In REST Namespace Spec** : The object identifier is already present in the REST route path (e.g., `/v1/table/{id}/describe`), so the `id` field in the request body is optional and can be omitted to avoid redundancy.
  * **In Client-Side Access Spec** : When invoking operations directly through a client library (e.g., for directory namespace), the `id` field **must be specified** in the request since there is no REST route to carry this information.


When both the route path and request body contain the `id`, the REST server must validate that they match and return a 400 Bad Request error if they differ. See [REST Routes](<../../rest/catalog-spec/#rest-routes>) for more details.

## Operation List¶

Operation ID | Current Version | Namespace | Table | Index | Metadata | Data | Transaction  
---|---|---|---|---|---|---|---  
CreateNamespace | 1 | ✓ |  |  | ✓ |  |   
ListNamespaces | 1 | ✓ |  |  | ✓ |  |   
DescribeNamespace | 1 | ✓ |  |  | ✓ |  |   
DropNamespace | 1 | ✓ |  |  | ✓ |  |   
NamespaceExists | 1 | ✓ |  |  | ✓ |  |   
ListTables | 1 | ✓ | ✓ |  | ✓ |  |   
ListAllTables | 1 |  | ✓ |  | ✓ |  |   
RegisterTable | 1 |  | ✓ |  | ✓ |  |   
DescribeTable | 1 |  | ✓ |  | ✓ |  |   
TableExists | 1 |  | ✓ |  | ✓ |  |   
DropTable | 1 |  | ✓ |  | ✓ |  |   
DeregisterTable | 1 |  | ✓ |  | ✓ |  |   
InsertIntoTable | 1 |  | ✓ |  |  | ✓ |   
MergeInsertIntoTable | 1 |  | ✓ |  |  | ✓ |   
UpdateTable | 1 |  | ✓ |  |  | ✓ |   
DeleteFromTable | 1 |  | ✓ |  |  | ✓ |   
QueryTable | 1 |  | ✓ |  |  | ✓ |   
CountTableRows | 1 |  | ✓ |  |  | ✓ |   
CreateTable | 1 |  | ✓ |  |  | ✓ |   
DeclareTable | 1 |  | ✓ |  | ✓ |  |   
CreateEmptyTable | 1 (deprecated) |  | ✓ |  | ✓ |  |   
CreateTableIndex | 1 |  | ✓ | ✓ | ✓ |  |   
CreateTableScalarIndex | 1 |  | ✓ | ✓ | ✓ |  |   
ListTableIndices | 1 |  | ✓ | ✓ | ✓ |  |   
DescribeTableIndexStats | 1 |  | ✓ | ✓ | ✓ |  |   
RestoreTable | 1 |  | ✓ |  | ✓ |  |   
RenameTable | 1 |  | ✓ |  | ✓ |  |   
ListTableVersions | 1 |  | ✓ |  | ✓ |  |   
CreateTableVersion | 1 |  | ✓ |  | ✓ |  |   
BatchCreateTableVersions | 1 |  | ✓ |  | ✓ |  |   
DescribeTableVersion | 1 |  | ✓ |  | ✓ |  |   
BatchDeleteTableVersions | 1 |  | ✓ |  | ✓ |  |   
ExplainTableQueryPlan | 1 |  | ✓ |  |  | ✓ |   
AnalyzeTableQueryPlan | 1 |  | ✓ |  |  | ✓ |   
AlterTableAddColumns | 1 |  | ✓ |  |  | ✓ |   
AlterTableAlterColumns | 1 |  | ✓ |  | ✓ |  |   
AlterTableDropColumns | 1 |  | ✓ |  | ✓ |  |   
UpdateTableSchemaMetadata | 1 |  | ✓ |  | ✓ |  |   
GetTableStats | 1 |  | ✓ |  | ✓ |  |   
ListTableTags | 1 |  | ✓ |  | ✓ |  |   
GetTableTagVersion | 1 |  | ✓ |  | ✓ |  |   
CreateTableTag | 1 |  | ✓ |  | ✓ |  |   
DeleteTableTag | 1 |  | ✓ |  | ✓ |  |   
UpdateTableTag | 1 |  | ✓ |  | ✓ |  |   
DropTableIndex | 1 |  | ✓ | ✓ | ✓ |  |   
DescribeTransaction | 1 |  |  |  | ✓ |  | ✓  
AlterTransaction | 1 |  |  |  | ✓ |  | ✓  
  
## Recommended Basic Operations¶

To have a functional basic namespace implementation, the following metadata operations are recommended as a minimum:

**Namespace Metadata Operations:**

  * CreateNamespace - Create a new namespace
  * ListNamespaces - List available namespaces
  * DescribeNamespace - Get namespace details
  * DropNamespace - Remove a namespace


**Table Metadata Operations:**

  * DeclareTable - Declare a table as exist
  * ListTables - List tables in a namespace
  * DescribeTable - Get table details
  * DeregisterTable - Unregister a table while preserving its data


These operations provide the foundational metadata management capabilities needed for namespace and table administration without requiring data or index operation support. With the namespace able to provide basic information about the table, the Lance SDK can be used to fulfill the other operations.

### Restrictions for Basic Operations¶

The following restrictions apply to the recommended basic operations to minimize implementation complexity:

**DropNamespace:** Only the `Restrict` behavior mode is required. This means the namespace must be empty (no tables or child namespaces) before it can be dropped. The `Cascade` behavior mode, which recursively drops all contents, is not required for basic implementations.

**DescribeTable:** Only `load_detailed_metadata=false` (the default) is required. This means the implementation only needs to return the table `location` without opening the dataset. Returning detailed metadata such as `version`, `schema`, and `stats` (which require opening the dataset) is not required for basic implementations.

### Why Not `CreateTable` and `DropTable`?¶

`CreateTable` and `DropTable` are common in most catalog systems, but are intentionally excluded from the recommended basic operations because they involve data operations that present challenges for catalog implementations:

  * **Data Operation Complexity:** Both `CreateTable` and `DropTable` are data operations rather than pure metadata operations. They can be long-running, especially when dealing with large datasets or remote storage systems. This makes them difficult to implement reliably in catalog systems designed for fast metadata lookups.

  * **Atomicity Guarantees:** Data operations require careful handling of atomicity. A failed `CreateTable` or `DropTable` operation can leave the system in an inconsistent state with partially created or deleted data files. Catalog implementations would need to implement complex cleanup and recovery mechanisms.

  * **CreateTable Challenges:** `CreateTable` is particularly difficult for catalogs to fully implement because features like CREATE TABLE AS SELECT (CTAS) require either complicated staging mechanisms or multi-statement transaction support.


While some catalog systems can handle these complex workflows, doing so typically requires deep, dedicated integration. Lance Namespace aims to enable as many catalogs as possible to adopt Lance format. By focusing on `DeclareTable` and `DeregisterTable` instead of `CreateTable` and `DropTable`, namespace implementations only need to handle metadata operations that are simple, fast and atomic across all catalog solutions. `CreateTable` and `DropTable` can then be fulfilled by combining these metadata operations with the Lance SDK.

## Operation Versioning¶

When a backwards incompatible change is introduced, a new operation version needs to be created, with a naming convention of `<OperationId>V<version>`, for example `ListNamespacesV2`, `DescribeTableV3`, etc.

## Request and Response Models¶

Each operation has a corresponding request and response model defined in the [Models](<models/>) section. The naming convention is `<OperationId>Request` and `<OperationId>Response`.

For example:

  * `CreateNamespaceRequest` / `CreateNamespaceResponse`
  * `ListTablesRequest` / `ListTablesResponse`
  * `DescribeTableRequest` / `DescribeTableResponse`


## Error Handling¶

All operations use a standardized error model with numeric error codes. Each operation documents the specific errors it may return. See [Error Handling](<errors/>) for the complete list of error codes and per-operation error documentation.

Back to top



---

<!-- Source: https://lance.org/format/namespace/dir/catalog-spec/ -->

# Lance Directory Namespace Catalog Spec¶

**Lance directory namespace** is a catalog that stores tables in a directory structure on any local or remote storage system. It has gone through 2 major spec versions so far:

  * **V1 (Directory Listing)** : A lightweight, simple 1-level namespace that discovers tables by scanning the directory.
  * **V2 (Manifest)** : A more advanced implementation backed by a manifest table (a Lance table) that supports nested namespaces and better performance at scale.


## V1: Directory Listing¶

V1 is a simple 1-level namespace where each table corresponds to a subdirectory with the format `<table_name>.lance`. This mode is ideal for getting started quickly with Lance tables.

### Directory Layout¶

A directory namespace maps to a directory on storage, called the **namespace directory**. A Lance table corresponds to a subdirectory in the namespace directory that has the format `<table_name>.lance`, called a **table directory**.

Consider the following example namespace directory layout:
    
    
    .
    └── /my/dir1/
        ├── table1.lance/
        │   ├── data/
        │   │   ├── 0aa36d91-8293-406b-958c-faf9e7547938.lance
        │   │   └── ed7af55d-b064-4442-bcb5-47b524e98d0e.lance
        │   ├── _versions/
        │   │   └── 9223372036854775707.manifest
        │   └── _indices/
        │       └── 85814508-ed9a-41f2-b939-2050bb7a0ed5-fts/
        │           └── index.idx
        ├── table2.lance/
        ├── table3.lance/
        │   └── .lance-deregistered      # Marker: table3 is deregistered
        └── table4.lance/
            └── .lance-reserved          # Marker: table4 is reserved but not created
    

This describes a Lance directory namespace with the namespace directory at `/my/dir1/`. It contains active tables `table1` and `table2` at table directories `/my/dir1/table1.lance` and `/my/dir1/table2.lance`. Table `table3` exists on storage but is deregistered (excluded from table listings). Table `table4` is reserved but not yet created with data.

### Table Existence¶

In V1, a table exists in a Lance directory namespace if a table directory of the specific name exists and the table is not marked as deregistered. In object store terms, this means the prefix `<table_name>.lance/` has at least one file in it and the file `<table_name>.lance/.lance-deregistered` does not exist.

### Marker Files¶

V1 uses marker files within table directories to track table state:

Marker File | Purpose  
---|---  
`.lance-reserved` | Indicates a table name/location is reserved but not yet created  
`.lance-deregistered` | Indicates a table has been deregistered but data is preserved  
  
When a table is deregistered via the `DeregisterTable` operation, the `.lance-deregistered` marker file is created inside the table directory. This causes the table to be excluded from `ListTables` results and to return "not found" for `DescribeTable` and `TableExists` operations, while preserving the table data for potential re-registration.

## V2: Manifest¶

V2 uses a special `__manifest` table (a Lance table) stored in the namespace directory to track all tables and namespaces. This provides several advantages over V1:

  * **Nested namespaces** : Support for hierarchical namespace organization
  * **Better performance** : Table discovery queries the manifest table instead of scanning the directory and leverages Lance's random access capability.
  * **Metadata support** : All operations can be supported, e.g. namespaces can have associated properties/metadata, tables can be renamed.
  * **Optimized directory path** : Hash-based directory naming prevents conflicts and maximizes throughput in object storage.


### Directory Layout¶
    
    
    .
    └── /my/dir1/
        ├── __manifest/                    # The manifest table
        │   ├── data/
        │   │   └── ...
        │   └── _versions/
        │       └── ...
        ├── table1.lance/                  # Root namespace table (compatibility mode)
        │   └── ...
        ├── a1b2c3d4_table2/               # Root namespace table (V2)
        │   └── ...
        └── e5f6g7h8_ns1$table3/           # Table in child namespace
            └── ...
    

### Manifest Table Schema¶

The `__manifest` table has the following schema:

Column | Type | Description  
---|---|---  
`object_id` | String | Unique identifier for the object. For root-level objects, this is the name. For nested objects, this is the namespace path joined by `$` delimiter (e.g., `ns1$ns2$table_name`)  
`object_type` | String | Either `"namespace"` or `"table"`  
`location` | String (nullable) | Relative path to the table directory within the root (only for tables)  
`metadata` | String (nullable) | JSON-encoded metadata/properties (only for namespaces)  
`base_objects` | List (nullable) | Reserved for future use (e.g., view dependencies)  
  
**Primary Key** : The `object_id` column is the [unenforced primary key](<https://lance.org/format/table/#unenforced-primary-key>) for the manifest table. Implementation of this spec must always enforce the primary key uniqueness using features like Lance merge insert with primary key deduplication.

**Schema Extensibility** : The `__manifest` table schema may include additional columns beyond those listed above. Extensions like [partitioned namespaces](<../../partitioning-spec/>) add columns for efficient filtering. Implementations should preserve unrecognized columns during updates.

### Root Namespace Properties¶

In V2, the root namespace is implicit and does not have a row in the `__manifest` table. Instead, root namespace properties are stored in the `__manifest` Lance table's metadata map. Properties are stored as key-value pairs where the key is the property name and the value is a UTF-8 encoded byte array.

For example, a partitioned namespace stores its `partition_spec_v1`, `partition_spec_v2`, and `schema` properties in the `__manifest` table's metadata.

### Manifest Table Indexes¶

The following indexes are created on the manifest table for query performance:

  * BTREE index on `object_id` for fast lookups
  * Bitmap index on `object_type` for efficient type filtering
  * LabelList index on `base_objects` for view dependency queries


### Manifest Table Commits¶

When adding a new entry in the manifest table, it must atomically check if the table already exists such entry, as well as if any concurrent operation writes the same entry, and fail the operation accordingly if such conflict exists.

### Manifest Table Directory¶

In V2, table data is stored in directories with hash-based names in the format `<hash>_<object_id>`. For example, a table `my_table` in namespace `ns1` would be stored in a directory like `a1b2c3d4_ns1$my_table`.

The hash prefix serves two purposes:

  1. **Object store throughput** : Many object stores (e.g., S3) partition data by key prefix. Random hash prefixes distribute tables across partitions for better parallelism.
  2. **Conflict prevention** : High entropy prevents issues when a table is created, deleted, and recreated with the same name in quick succession.


The `object_id` suffix ensures uniqueness and aids debugging.

In compatibility mode, root namespace tables use `<table_name>.lance` naming to remain compatible with V1.

### Table Version Management¶

V2 optionally supports managed table versioning, where table versions are tracked in the `__manifest` table instead of relying on Lance's native version management. When enabled, the directory namespace acts as an [external manifest store](<https://lance.org/format/table/transaction/#external-manifest-store>). This feature must be enabled for the entire namespace.

#### Enabling Table Version Management¶

To enable table version management, store `table_version_management=true` in the `__manifest` Lance table's metadata map. Once enabled, all table version operations must use the namespace APIs (`CreateTableVersion`, `BatchCreateTableVersions`, `DescribeTableVersion`, `ListTableVersions`, `BatchDeleteTableVersions`) instead of the default single-table storage-only version management.

#### Table Version Object ID¶

Table versions are stored in the `__manifest` table with `object_id` in the format `<table_id>$<version>`. For example:

  * Table `users` version 1: `object_id = "users$1"`
  * Table `analytics$events` (in namespace `analytics`) version 5: `object_id = "analytics$events$5"`


The `object_type` for table version entries is `"table_version"`.

#### Table Version Metadata Schema¶

The `metadata` column for table version entries contains a JSON object with the following schema:

Field | Type | Required | Description  
---|---|---|---  
`manifest_path` | string | Yes | Path to the manifest file for this version  
`manifest_size` | integer | No | Size of the manifest file in bytes  
`e_tag` | string | No | ETag for the manifest file, useful for S3 and similar object stores  
`metadata` | object (string -> string) | No | Optional key-value pairs of version metadata  
`naming_scheme` | string | No | The [naming scheme](<https://lance.org/format/table/transaction/#manifest-naming-schemes>) used for manifest files  
  
Example metadata JSON:
    
    
    {
        "manifest_path": "_versions/9223372036854775806.manifest",
        "manifest_size": 4096,
        "e_tag": "abc123",
        "metadata": {"author": "user1", "description": "Initial schema"},
        "naming_scheme": "V2"
    }
    

## Compatibility Mode¶

By default, the directory namespace operates in compatibility mode, supporting both V1 and V2 tables simultaneously. This allows gradual migration from V1 to V2 without disrupting existing workflows.

In compatibility mode:

  1. When checking if a table exists in the root namespace, the implementation first checks the manifest table, then falls back to checking if a `<table_name>.lance` directory exists.
  2. When listing tables in the root namespace, results from both the manifest table and directory listing are merged, with manifest entries taking precedence when duplicates exist.
  3. When creating tables in the root namespace, the table is registered in the manifest and uses the V1 `<table_name>.lance` naming convention for backward compatibility.
  4. If a table in the root namespace is renamed, it transitions to the V2 hash-based path naming.
  5. For operations in child namespaces, only V2 behavior is used since V1 does not support nested namespaces.


### Migration from V1 to V2¶

To fully migrate from V1 to V2, add all existing V1 table directory paths to the manifest table. Once all tables are registered in the manifest, compatibility mode can be disabled to use only V2 behavior.

Back to top



---

<!-- Source: https://lance.org/format/namespace/dir/impl-spec/ -->

# Lance Directory Namespace Implementation Spec¶

This document describes how the Lance Directory Namespace implements the Lance Namespace client spec.

## Background¶

The Lance Directory Namespace is a catalog that stores tables in a directory structure on any local or remote storage system. For details on the catalog design including V1 (directory listing), V2 (manifest), and compatibility mode, see the [Directory Namespace Catalog Spec](<../catalog-spec/>).

## Namespace Implementation Configuration Properties¶

The Lance directory namespace implementation accepts the following configuration properties:

The **root** property is required and specifies the root directory of the namespace where tables are stored. This can be a local path like `/my/dir` or a cloud storage URI like `s3://bucket/prefix`.

The **manifest_enabled** property controls whether the manifest table is used for tracking tables and namespaces (V2). Defaults to `true`.

The **dir_listing_enabled** property controls whether directory scanning is used for table discovery (V1). Defaults to `true`.

By default, both properties are enabled, which means the implementation operates in [Compatibility Mode](<../catalog-spec/#compatibility-mode>).

Properties with the **storage.** prefix are passed directly to the underlying Lance ObjectStore after removing the prefix. For example, `storage.region` becomes `region` when passed to the storage layer.

## Object Mapping¶

### Namespace¶

The **root namespace** is the root directory specified by the `root` configuration property. This is the base path where all tables are stored.

A **child namespace** is a logical container tracked in the manifest table. Child namespaces are only supported in V2; V1 treats the root directory as a flat namespace containing only tables. Child namespaces do not correspond to physical subdirectories.

The **namespace identifier** is a list of strings representing the namespace path. For example, a namespace `["prod", "analytics"]` is serialized to `prod$analytics` when stored in the manifest table's `object_id` column.

**Namespace properties** are stored as JSON in the `metadata` column of the manifest table. This is only available in V2.

### Table¶

A **table** is a subdirectory containing Lance table data. The directory must contain valid Lance format files including the `_versions/` directory with version manifests.

The **table identifier** is a list of strings representing the namespace path followed by the table name. For example, a table `["prod", "analytics", "users"]` represents a table named `users` in namespace `["prod", "analytics"]`. This is serialized to `prod$analytics$users` when stored in the manifest table's `object_id` column.

The **table location** depends on the mode and namespace level:

  * In V1 (root namespace only), tables are stored as `<table_name>.lance` directories
  * In V2 with `dir_listing_enabled=true` and an empty namespace (root level), tables use the `<table_name>.lance` naming convention for backward compatibility
  * In V2 for child namespaces, or when `dir_listing_enabled=false`, tables are stored as `<hash>_<object_id>` directories where hash provides entropy for object store throughput


**Table properties** are stored in Lance table metadata and can be accessed via the Lance SDK.

## Lance Table Identification¶

In a Directory Namespace, a Lance table is identified differently depending on the mode:

In **V1** , a Lance table is any directory with the `.lance` suffix (e.g., `users.lance/`). The directory must contain valid Lance table data to be usable. Only single-level table identifiers (e.g., `["users"]`) are supported in this mode.

In **V2** , a Lance table is identified by a row in the manifest table with `object_type="table"`. The row's `location` field points to the Lance table directory. Multi-level table identifiers (e.g., `["prod", "analytics", "users"]`) are supported.

A valid Lance table directory must be non-empty.

## Basic Operations¶

### CreateNamespace¶

This operation is only supported in V2. V1 does not support explicit namespace creation since it uses a flat directory structure.

The implementation creates a new namespace using a merge-insert operation on the manifest table:

  1. Validate the parent namespace exists (if not creating at root level)
  2. Merge-insert a new row into the manifest table with:
     * `object_id` set to the namespace identifier (e.g., `prod$analytics`)
     * `object_type` set to `"namespace"`
     * `metadata` containing the namespace properties as JSON
     * `created_at` set to the current timestamp


Primary-key deduplication on `object_id` ensures no duplicate rows are inserted. If a namespace with the same identifier already exists, the operation fails.

**Error Handling:**

If a namespace with the same identifier already exists, return error code `2` (NamespaceAlreadyExists).

If the parent namespace does not exist (for nested namespaces), return error code `1` (NamespaceNotFound).

If the identifier format is invalid, return error code `13` (InvalidInput).

### ListNamespaces¶

This operation lists child namespaces within a parent namespace.

In **V1** , this operation returns an empty list since namespaces are not supported.

In **V2** , the implementation queries the manifest table:

  1. Query for rows where `object_type = "namespace"`
  2. Filter to rows where `object_id` starts with the parent namespace prefix
  3. Further filter to rows where `object_id` has exactly one more level than the parent
  4. Return the list of namespace names (the last component of each identifier)


**Error Handling:**

If the parent namespace does not exist (V2 only), return error code `1` (NamespaceNotFound).

### DescribeNamespace¶

This operation is only supported in V2 and returns namespace metadata.

The implementation:

  1. Query the manifest table for the row with the matching `object_id`
  2. Parse the `metadata` column as JSON
  3. Return the namespace name and properties


**Error Handling:**

If the namespace does not exist, return error code `1` (NamespaceNotFound).

### DropNamespace¶

This operation is only supported in V2 and removes a namespace.

The implementation:

  1. Check that the namespace exists in the manifest table
  2. Query for any child namespaces or tables with identifiers starting with this namespace's prefix
  3. If any children exist, the operation fails
  4. Delete the namespace row from the manifest table using the `object_id` primary key


**Error Handling:**

If the namespace does not exist, return error code `1` (NamespaceNotFound).

If the namespace contains tables or child namespaces, return error code `3` (NamespaceNotEmpty).

### DeclareTable¶

This operation declares a new Lance table, reserving the table name and location without creating actual data files.

The implementation:

  1. Validate the parent namespace exists (in V2)
  2. Determine the table location:
     * In V1: `<root>/<table_name>.lance`
     * In V2 with `dir_listing_enabled=true` at root level: `<root>/<table_name>.lance`
     * In V2 for child namespaces or with `dir_listing_enabled=false`: `<root>/<hash>_<object_id>/`
  3. Create a `.lance-reserved` file at the location to mark the table's existence
  4. In V2, merge-insert a row into the manifest table with:
     * `object_id` set to the table identifier
     * `object_type` set to `"table"`
     * `location` set to the table directory path


Primary-key deduplication on `object_id` ensures no duplicate rows are inserted. If a table with the same identifier already exists, the operation fails.

**Error Handling:**

If the parent namespace does not exist, return error code `1` (NamespaceNotFound).

If a table with the same identifier already exists, return error code `5` (TableAlreadyExists).

If there is a concurrent creation attempt, return error code `14` (ConcurrentModification).

### ListTables¶

This operation lists tables within a namespace.

In **V1** :

  1. List all entries in the root directory
  2. Filter to directories matching the `*.lance` pattern
  3. Return the table names (directory names without the `.lance` suffix)


In **V2** :

  1. Query the manifest table for rows where `object_type = "table"`
  2. Filter to rows where `object_id` starts with the namespace prefix
  3. Further filter to rows where `object_id` has exactly one more level than the namespace
  4. Return the list of table names


When **both V1 and V2 are enabled** (the default [Compatibility Mode](<../catalog-spec/#compatibility-mode>)), the implementation performs both queries and merges results, with manifest entries taking precedence when duplicates exist.

**Error Handling:**

If the namespace does not exist (V2 only), return error code `1` (NamespaceNotFound).

### DescribeTable¶

This operation returns table metadata including schema, version, and properties.

The implementation:

  1. Locate the table:
     * In V1, check for the `<table_name>.lance` directory
     * In V2, query the manifest table for the table location
     * When both V1 and V2 are enabled (the default [Compatibility Mode](<../catalog-spec/#compatibility-mode>)), first check the manifest table, then fall back to checking the `.lance` directory
  2. Open the Lance table using the Lance SDK
  3. Read the table metadata and return:
     * `name`: The table name
     * `schema`: The Arrow schema of the table
     * `version`: The current version number
     * `location`: The table directory path


**Error Handling:**

If the parent namespace does not exist, return error code `1` (NamespaceNotFound).

If the table does not exist, return error code `4` (TableNotFound).

If a specific version is requested and does not exist, return error code `11` (TableVersionNotFound).

### DeregisterTable¶

This operation deregisters a table from the namespace while preserving its data on storage. The table files remain at their storage location and can be re-registered later using RegisterTable.

In **V1** :

  1. Locate the table by checking for the `<table_name>.lance` directory
  2. Verify the table exists and is not already deregistered
  3. Create a `.lance-deregistered` marker file inside the table directory
  4. Return the table location for reference


The marker file approach ensures that: \- Table data remains intact at its original location \- The table is excluded from `ListTables` results \- The table returns `TableNotFound` for `DescribeTable` and `TableExists` operations \- The table can be re-registered by removing the marker file and calling `RegisterTable` \- `DropTable` still works on deregistered tables (removes both data and marker file)

In **V2** :

  1. Locate the table by querying the manifest table for the table location
  2. Remove the table row from the manifest table using the `object_id` primary key
  3. Keep the table files at the storage location
  4. Return the table location and properties for reference


When **both V1 and V2 are enabled** (the default [Compatibility Mode](<../catalog-spec/#compatibility-mode>)), first check the manifest table, then fall back to checking the `.lance` directory. If found in manifest, follow V2 behavior; otherwise follow V1 behavior.

**Error Handling:**

If the parent namespace does not exist, return error code `1` (NamespaceNotFound).

If the table does not exist or is already deregistered, return error code `4` (TableNotFound).

## Additional Operations¶

### DropTable¶

This operation removes a table and its data.

In **V1** :

  1. Locate the table by checking for the `<table_name>.lance` directory
  2. Delete the table directory and all its contents from storage
  3. If deletion fails midway (directory is still non-empty), the drop has failed and should be retried


In **V2** :

  1. Locate the table by querying the manifest table for the table location
  2. Remove the table row from the manifest table using the `object_id` primary key
  3. Delete the table directory and all its contents from storage (failure here does not affect the success of the drop since the table is no longer reachable)


When **both V1 and V2 are enabled** (the default [Compatibility Mode](<../catalog-spec/#compatibility-mode>)), first check the manifest table, then fall back to checking the `.lance` directory. If found in manifest, follow V2 behavior; otherwise follow V1 behavior.

**Error Handling:**

If the parent namespace does not exist, return error code `1` (NamespaceNotFound).

If the table does not exist, return error code `4` (TableNotFound).

If there is a file system permission error, return error code `15` (PermissionDenied).

If there is an unexpected I/O error, return error code `18` (Internal).

### CreateTableVersion¶

This operation creates a new version entry for a table. It supports `put_if_not_exists` semantics.

When **table version management is not enabled** :

  1. Resolve the table location
  2. Parse the staging manifest path from the request
  3. Determine the final manifest path based on the naming scheme (V1 or V2)
  4. Copy the staging manifest to the final path in the `_versions/` directory using `put_if_not_exists` semantics
  5. Delete the staging manifest file
  6. Return the created version info including the final manifest path


When **table version management is enabled** (V2 with `table_version_management=true` in `__manifest` metadata), the directory namespace acts as an external manifest store. The commit process follows these steps:

  1. **Stage manifest in object storage** : The caller writes the new manifest to a staging path (e.g., `{table_location}/_versions/{version}.manifest-{uuid}`). This staged manifest is not yet visible to readers.
  2. **Atomically commit to manifest table** : Merge-insert a new row into the `__manifest` table with:
     * `object_id` set to `<table_id>$<version>` (e.g., `users$1` or `ns1$users$1`)
     * `object_type` set to `"table_version"`
     * `metadata` containing the JSON-encoded version metadata including the staging manifest path


Primary-key deduplication on `object_id` ensures no duplicate rows are inserted. The commit is effectively complete after this step. If this fails, another writer has already committed that version. 3\. **Finalize in object storage** : Copy the staged manifest to the standard location (`{table_location}/_versions/{version}.manifest`). This makes it discoverable by readers that do not use the manifest table. 4\. **Update manifest table pointer** : Update the `metadata` in the manifest table row to point to the finalized manifest path, synchronizing both systems.

**Error Handling:**

If the table does not exist, return error code `4` (TableNotFound).

If the version already exists, return error code `12` (TableVersionAlreadyExists).

If there is a concurrent creation attempt, return error code `14` (ConcurrentModification).

### BatchCreateTableVersions¶

This operation atomically creates version entries for multiple tables.

When **table version management is not enabled** , this operation iterates through each entry and calls `CreateTableVersion` for each one. Atomicity is not guaranteed.

When **table version management is enabled** , the batch commit process follows these steps:

  1. **Stage manifests in object storage** : For each entry, the caller writes the new manifest to a staging path (e.g., `{table_location}/_versions/{version}.manifest-{uuid}`).
  2. **Atomically commit to manifest table** : Merge-insert all version rows into the `__manifest` table in a single atomic commit, each with:
     * `object_id` set to `<table_id>$<version>`
     * `object_type` set to `"table_version"`
     * `metadata` containing the JSON-encoded version metadata including the staging manifest path


Primary-key deduplication on `object_id` ensures no duplicate rows are inserted. The commit is effectively complete after this step. If any version already exists, the entire batch fails. 3\. **Finalize in object storage** : For each entry, copy the staged manifest to the standard location. 4\. **Update manifest table pointers** : Update the `metadata` in each manifest table row to point to the finalized manifest paths.

**Error Handling:**

If any table does not exist, return error code `4` (TableNotFound).

If any version already exists, return error code `12` (TableVersionAlreadyExists).

If there is a concurrent modification, return error code `14` (ConcurrentModification).

### ListTableVersions¶

This operation lists version entries for a table.

When **table version management is not enabled** :

  1. Resolve the table location
  2. List all files in the `_versions/` directory
  3. Parse version numbers from manifest filenames (handling both V1 and V2 naming schemes)
  4. Extract metadata from file attributes (size, e_tag, last_modified timestamp)
  5. Sort results by version number (descending if `descending=true`)
  6. Apply pagination using `page_token` and `limit`


When **table version management is enabled** :

  1. Query the manifest table for rows where:
     * `object_type = "table_version"`
     * `object_id` starts with `<table_id>$`
  2. Parse the version number from each `object_id`
  3. Parse the `metadata` column as JSON to extract version details
  4. Sort results by version number (descending if `descending=true`)
  5. Apply pagination using `page_token` and `limit`


**Error Handling:**

If the table does not exist, return error code `4` (TableNotFound).

### DescribeTableVersion¶

This operation retrieves details for a specific table version.

When **table version management is not enabled** :

  1. Resolve the table location
  2. Open the Lance dataset at the specified version
  3. Read the manifest file to extract version metadata
  4. Return the version information including manifest_path, manifest_size, e_tag, timestamp_millis, and metadata


When **table version management is enabled** , the read process validates and synchronizes the manifest:

  1. **Query manifest table** : Retrieve the manifest path for the requested version from the row with `object_id = <table_id>$<version>`. If the path matches the expected path based on the naming scheme, synchronization is complete.
  2. **Synchronize to object storage** : If the manifest path does not match the expected path based on the naming scheme (i.e., it is a staging path), copy the staged manifest to its final location (`{table_location}/_versions/{version}.manifest`). This is an idempotent operation.
  3. **Update manifest table** : Update the `metadata` in the manifest table row to reflect the finalized path for future readers.
  4. **Return version information** : Return the version information with the finalized manifest path, or error if synchronization fails.


**Error Handling:**

If the table does not exist, return error code `4` (TableNotFound).

If the version does not exist, return error code `11` (TableVersionNotFound).

### BatchDeleteTableVersions¶

This operation deletes multiple version entries for a table.

When **table version management is not enabled** :

  1. Resolve the table location
  2. Delete the manifest files in the `_versions/` directory for each specified version
  3. Return the count of deleted versions


When **table version management is enabled** :

  1. Delete the manifest files in the `_versions/` directory for each specified version
  2. Delete rows from the manifest table using the `object_id` primary key for each specified version
  3. Return the count of deleted versions


**Error Handling:**

If the table does not exist, return error code `4` (TableNotFound).

If any specified version does not exist, the operation may either skip it silently or return error code `11` (TableVersionNotFound), depending on the `ignore_missing` parameter.

Back to top



---

<!-- Source: https://lance.org/format/namespace/rest/catalog-spec/ -->

# Lance REST Namespace Catalog Spec¶

In an enterprise environment, typically there is a requirement to store tables in a metadata service for more advanced governance features around access control, auditing, lineage tracking, etc. **Lance REST Namespace** is an OpenAPI catalog protocol that enables reading, writing and managing Lance tables by connecting those metadata services or building a custom metadata server in a standardized way. The REST server definition can be found in the [OpenAPI specification](<https://editor-next.swagger.io/?url=https://raw.githubusercontent.com/lance-format/lance-namespace/refs/heads/main/docs/src/rest.yaml>).

## Duality with Client-Side Access Spec¶

The Lance Namespace client-side access spec defines request and response models using OpenAPI. The REST namespace spec leverages this fact — the REST API is largely identical to the client-side access spec, with the request and response schemas directly used as HTTP request and response bodies.

This duality minimizes data conversion between client and server: a client can serialize its request model directly to JSON for the HTTP body, and deserialize the HTTP response body directly into the response model.

There are a few exceptions where the REST spec diverges from the client-side access spec. For example, for some operations like `InsertIntoTable`, `CreateTable`, `MergeInsertIntoTable`, the HTTP request body is used for transmitting Arrow IPC binary data, and the operation request fields are transmitted through query parameters instead. For some list operations like `ListNamespaces` and `ListTables`, pagination tokens and limits may be passed as query parameters for easier URL construction and caching.

These non-standard operations are documented in the Non-Standard Operations section below.

## REST Routes¶

The REST route for an operation typically follows the pattern of `POST /<version>/<object>/{id}/<action>`, for example `POST /v1/namespace/{id}/list` for `ListNamespace`. The request and response schemas are used as the actual request and response of the route.

The key design principle of the REST route is that all the necessary information for a reverse proxy (e.g. load balancing, authN, authZ) should be available for access without the need to deserialize request body. For example, the route for `CreateTable` is `POST /v1/table/{id}/create` instead of `POST /v1/table` so that the table identifier is visible to the reverse proxy without parsing the request body.

## Standard Operations¶

Standard operations should take the same request and return the same response as any other implementation.

The information in the route could also present in the request body. When the information in the route and request body both present but do not match, the server must throw a 400 Bad Request error. When the information in the request body is missing, the server must use the information in the route instead.

## Identity Header Mapping¶

All request schemas include an optional `identity` field for authentication. For REST Namespace, the identity fields are mapped to HTTP headers:

Identity Field | REST Form | Location  
---|---|---  
`api_key` | `x-api-key` | Header  
`auth_token` | `Authorization` | Header  
  
The `auth_token` is sent using the Bearer scheme (e.g., `Authorization: Bearer <token>`).

When identity information is provided in both the request body and headers, the header values take precedence.

## Context Header Mapping¶

All request schemas include an optional `context` field for passing arbitrary key-value pairs. This allows clients to send implementation-specific context that can be used by the server or forwarded to downstream services.

For REST Namespace, context entries are mapped to HTTP headers using the naming convention:

Context Entry | REST Form | Location  
---|---|---  
`{"<key>": "<value>"}` | `x-lance-ctx-<key>` | Header  
  
For example, a context entry `{"trace_id": "abc123", "user_region": "us-west"}` would be sent as:
    
    
    x-lance-ctx-trace_id: abc123
    x-lance-ctx-user_region: us-west
    

How to use the context is custom to the specific implementation. Common use cases include:

  * Passing trace IDs for distributed tracing
  * Forwarding user context to downstream services
  * Providing hints to the implementation for optimization


When context is provided in both the request body and headers, the header values take precedence.

## Non-Standard Operations¶

For request and response that cannot be simply described as a JSON object the REST server needs to perform special handling to describe equivalent information through path parameters, query parameters and headers.

### ListNamespaces¶

**Route:** `GET /v1/namespace/{id}/list`

Uses GET without a request body. Pagination parameters are passed as query parameters.

Request Field | REST Form | Location  
---|---|---  
`id` | `{id}` | Path parameter  
`page_token` | `page_token` | Query parameter  
`limit` | `limit` | Query parameter  
  
### ListTables¶

**Route:** `GET /v1/namespace/{id}/table/list`

Uses GET without a request body. Pagination parameters are passed as query parameters.

Request Field | REST Form | Location  
---|---|---  
`id` | `{id}` | Path parameter  
`page_token` | `page_token` | Query parameter  
`limit` | `limit` | Query parameter  
  
### ListAllTables¶

**Route:** `GET /v1/table/`

Uses GET without a request body. Pagination parameters are passed as query parameters.

Request Field | REST Form | Location  
---|---|---  
`page_token` | `page_token` | Query parameter  
`limit` | `limit` | Query parameter  
`delimiter` | `delimiter` | Query parameter  
  
### DescribeTable¶

**Route:** `POST /v1/table/{id}/describe`

The `with_table_uri` field is passed as a query parameter instead of in the request body.

Request Field | REST Form | Location  
---|---|---  
`id` | `{id}` | Path parameter  
`with_table_uri` | `with_table_uri` | Query parameter  
  
### CreateTable¶

**Route:** `POST /v1/table/{id}/create`

**Content-Type:** `application/vnd.apache.arrow.stream`

The request body contains Arrow IPC stream data. The table schema is derived from the Arrow stream schema. If the stream is empty, an empty table is created.

Request Field | REST Form | Location  
---|---|---  
`id` | `{id}` | Path parameter  
`mode` | `mode` | Query parameter  
`location` | `x-lance-table-location` | Header  
`properties` | `x-lance-table-properties` | Header (JSON-encoded string map)  
`data` | Request body | Body (Arrow IPC stream)  
  
### InsertIntoTable¶

**Route:** `POST /v1/table/{id}/insert`

**Content-Type:** `application/vnd.apache.arrow.stream`

The request body contains Arrow IPC stream data with records to insert.

Request Field | REST Form | Location  
---|---|---  
`id` | `{id}` | Path parameter  
`mode` | `mode` | Query parameter (`append` or `overwrite`, default: `append`)  
`data` | Request body | Body (Arrow IPC stream)  
  
### MergeInsertIntoTable¶

**Route:** `POST /v1/table/{id}/merge_insert`

**Content-Type:** `application/vnd.apache.arrow.stream`

The request body contains Arrow IPC stream data. Performs a merge insert (upsert) operation that updates existing rows based on a matching column and inserts new rows that don't match.

Request Field | REST Form | Location  
---|---|---  
`id` | `{id}` | Path parameter  
`on` | `on` | Query parameter (required)  
`when_matched_update_all` | `when_matched_update_all` | Query parameter (boolean)  
`when_matched_update_all_filt` | `when_matched_update_all_filt` | Query parameter (SQL expression)  
`when_not_matched_insert_all` | `when_not_matched_insert_all` | Query parameter (boolean)  
`when_not_matched_by_source_delete` | `when_not_matched_by_source_delete` | Query parameter (boolean)  
`when_not_matched_by_source_delete_filt` | `when_not_matched_by_source_delete_filt` | Query parameter (SQL expression)  
`timeout` | `timeout` | Query parameter (duration string, e.g., "30s", "5m")  
`use_index` | `use_index` | Query parameter (boolean)  
`data` | Request body | Body (Arrow IPC stream)  
  
### QueryTable¶

**Route:** `POST /v1/table/{id}/query`

**Response Content-Type:** `application/vnd.apache.arrow.file`

The response body contains Arrow IPC file data instead of JSON.

Response Field | REST Form | Notes  
---|---|---  
(results) | Response body | Arrow IPC file (binary, not JSON)  
  
### CountTableRows¶

**Route:** `POST /v1/table/{id}/count_rows`

The response is returned as a plain integer instead of a JSON object.

Response Field | REST Form | Notes  
---|---|---  
(count) | Response body | Plain integer (not JSON wrapped)  
  
### DropTable¶

**Route:** `POST /v1/table/{id}/drop`

No request body. All parameters are in the path.

### DropTableIndex¶

**Route:** `POST /v1/table/{id}/index/{index_name}/drop`

No request body. All parameters are in the path.

### ListTableVersions¶

**Route:** `POST /v1/table/{id}/version/list`

No request body. Pagination parameters are passed as query parameters.

Request Field | REST Form | Location  
---|---|---  
`id` | `{id}` | Path parameter  
`page_token` | `page_token` | Query parameter  
`limit` | `limit` | Query parameter  
  
### ListTableTags¶

**Route:** `POST /v1/table/{id}/tags/list`

No request body. Pagination parameters are passed as query parameters.

Request Field | REST Form | Location  
---|---|---  
`id` | `{id}` | Path parameter  
`page_token` | `page_token` | Query parameter  
`limit` | `limit` | Query parameter  
  
### ExplainTableQueryPlan¶

**Route:** `POST /v1/table/{id}/explain_plan`

The response is returned as a plain string instead of a JSON object.

Request Field | REST Form | Location  
---|---|---  
`id` | `{id}` | Path parameter  
`query` | `query` | Request body field  
`verbose` | `verbose` | Request body field  
Response Field | REST Form | Notes  
---|---|---  
`plan` | Response body | Plain string (not JSON wrapped)  
  
### AnalyzeTableQueryPlan¶

**Route:** `POST /v1/table/{id}/analyze_plan`

The response is returned as a plain string instead of a JSON object.

Request Field | REST Form | Location  
---|---|---  
`id` | `{id}` | Path parameter  
`query` | `query` | Request body field  
Response Field | REST Form | Notes  
---|---|---  
`analysis` | Response body | Plain string (not JSON wrapped)  
  
### UpdateTableSchemaMetadata¶

**Route:** `POST /v1/table/{id}/schema_metadata/update`

Both request and response bodies are direct objects (map of string to string) instead of being wrapped in a `metadata` field.

Request Field | REST Form | Location  
---|---|---  
`id` | `{id}` | Path parameter  
`metadata` | Request body | Direct object `{"key": "value", ...}` (not `{"metadata": {...}}`)  
Response Field | REST Form | Notes  
---|---|---  
`metadata` | Response body | Direct object `{"key": "value", ...}` (not `{"metadata": {...}}`)  
  
## Namespace Server and Adapter¶

Any REST HTTP server that implements this OpenAPI protocol is called a **Lance Namespace server**. If you are a metadata service provider that is building a custom implementation of Lance namespace, building a REST server gives you standardized integration to Lance without the need to worry about tool support and continuously distribute newer library versions compared to using an implementation.

If the main purpose of this server is to be a proxy on top of an existing metadata service, converting back and forth between Lance REST API models and native API models of the metadata service, then this Lance namespace server is called a **Lance Namespace adapter**.

## Choosing between an Adapter vs an Implementation¶

Any adapter can always be directly a Lance namespace implementation bypassing the REST server, and vise versa. In fact, an implementation is basically the backend of an adapter. For example, we natively support a Lance HMS Namespace implementation, as well as a Lance namespace adapter for HMS by using the HMS Namespace implementation to fulfill requests in the Lance REST server.

If you are considering between a Lance namespace adapter vs implementation to build or use in your environment, here are some criteria to consider:

  1. **Multi-Language Feasibility & Maintenance Cost**: If you want a single strategy that works across all Lance language bindings, an adapter is preferred. Sometimes it is not even possible for an integration to go with the implementation approach since it cannot support all the languages. Sometimes an integration is popular or important enough that it is viable to build an implementation and maintain one library per language.
  2. **Tooling Support** : each tool needs to declare the Lance namespace implementations it supports. That means there will be a preference for tools to always support a REST namespace, but it might not always support a specific implementation. This favors the adapter approach.
  3. **Security** : if you have security concerns about the adapter being a man-in-the-middle, you should choose an implementation
  4. **Performance** : after all, adapter adds one layer of indirection and is thus not the most performant solution. If you are performance sensitive, you should choose an implementation


Back to top



---

<!-- Source: https://lance.org/format/namespace/rest/impl-spec/ -->

# Lance REST Namespace Implementation Spec¶

This document describes how the Lance REST Namespace implements the Lance Namespace client spec.

## Background¶

The Lance REST Namespace is a catalog that provides access to Lance tables via a REST API. For details on the API design, endpoints, and data models, see the [REST Namespace Catalog Spec](<../catalog-spec/>).

## Namespace Implementation Configuration Properties¶

The Lance REST namespace implementation accepts the following configuration properties:

The **uri** property is required and specifies the URI endpoint for the REST API, for example `https://api.example.com/lance`.

The **delimiter** property specifies the delimiter used to parse object string identifiers in REST routes. Defaults to `$`. Other examples include `::` or `__delim__`.

Properties with the **headers.** prefix are passed as HTTP headers with every request to the REST server after removing the prefix. For example, `headers.Authorization` becomes the `Authorization` header. Common configurations include `headers.Authorization` for authentication tokens, `headers.X-API-Key` for API key authentication, and `headers.X-Request-ID` for request tracking.

## Object Mapping¶

### Namespace¶

The **root namespace** is represented by the delimiter character itself in REST routes (e.g., `$`). All REST API calls are made relative to the base URI.

A **child namespace** is managed by the REST server and accessed via namespace routes. The server is responsible for storing and organizing namespace metadata.

The **namespace identifier** is a list of strings representing the namespace path. For example, a namespace `["prod", "analytics"]` is serialized to `prod$analytics` in the REST route path using the configured delimiter (default `$`).

**Namespace properties** are managed by the REST server and accessed via the DescribeNamespace operation.

### Table¶

A **table** is managed by the REST server. The server handles table storage, versioning, and metadata management.

The **table identifier** is a list of strings representing the namespace path followed by the table name. For example, a table `["prod", "analytics", "users"]` represents a table named `users` in namespace `["prod", "analytics"]`. This is serialized to `prod$analytics$users` in the REST route path using the configured delimiter.

The **table location** is managed by the REST server and returned in the DescribeTable response. This location points to where the Lance table data is stored (e.g., an S3 path).

**Table properties** are managed by the REST server and accessed via table operations.

## Lance Table Identification¶

In a REST Namespace, the server is responsible for managing Lance tables. The client identifies tables by their string identifier and delegates all table operations to the server.

The server implementation must ensure that:

  * Tables are stored as valid Lance table directories on the underlying storage
  * The `location` field in DescribeTable response points to the Lance table root directory
  * Table properties include any Lance-specific metadata required by the Lance SDK


## Basic Operations¶

### CreateNamespace¶

Creates a new namespace.

**HTTP Request:**
    
    
    POST /v1/namespace/{id}/create
    Content-Type: application/json
    

The request body contains optional namespace properties:
    
    
    {
      "properties": {
        "description": "Production analytics namespace"
      }
    }
    

The implementation:

  1. Parse the namespace identifier from the route path `{id}`
  2. Validate the request body format
  3. Check if the parent namespace exists (for nested namespaces)
  4. Check if a namespace with this identifier already exists
  5. Create the namespace in the server's storage
  6. Return the created namespace details


**Response:**
    
    
    {
      "name": "analytics",
      "properties": {
        "description": "Production analytics namespace"
      }
    }
    

**Error Handling:**

If the request body is malformed, return HTTP `400 Bad Request` with error code `13` (InvalidInput).

If a namespace with the same identifier already exists, return HTTP `409 Conflict` with error code `2` (NamespaceAlreadyExists).

If the parent namespace does not exist, return HTTP `404 Not Found` with error code `1` (NamespaceNotFound).

### ListNamespaces¶

Lists child namespaces within a parent namespace.

**HTTP Request:**
    
    
    GET /v1/namespace/{id}/list?page_token=xxx&limit=100
    

The `page_token` and `limit` query parameters support pagination.

The implementation:

  1. Parse the parent namespace identifier from the route path `{id}`
  2. Validate the parent namespace exists
  3. Query the server's storage for child namespaces
  4. Apply pagination using `page_token` and `limit`
  5. Return the list of namespace names


**Response:**
    
    
    {
      "namespaces": ["analytics", "ml", "reporting"],
      "next_page_token": "abc123"
    }
    

The `next_page_token` field is only present if there are more results.

**Error Handling:**

If the parent namespace does not exist, return HTTP `404 Not Found` with error code `1` (NamespaceNotFound).

### DescribeNamespace¶

Returns namespace metadata.

**HTTP Request:**
    
    
    POST /v1/namespace/{id}/describe
    Content-Type: application/json
    

The request body is empty:
    
    
    {}
    

The implementation:

  1. Parse the namespace identifier from the route path `{id}`
  2. Look up the namespace in the server's storage
  3. Return the namespace name and properties


**Response:**
    
    
    {
      "name": "analytics",
      "properties": {
        "description": "Production analytics namespace",
        "created_at": "2024-01-15T10:30:00Z"
      }
    }
    

**Error Handling:**

If the namespace does not exist, return HTTP `404 Not Found` with error code `1` (NamespaceNotFound).

### DropNamespace¶

Removes a namespace.

**HTTP Request:**
    
    
    POST /v1/namespace/{id}/drop
    Content-Type: application/json
    

The request body is empty:
    
    
    {}
    

The implementation:

  1. Parse the namespace identifier from the route path `{id}`
  2. Check that the namespace exists
  3. Check that the namespace is empty (no child namespaces or tables)
  4. Delete the namespace from the server's storage


**Response:**
    
    
    {}
    

**Error Handling:**

If the namespace does not exist, return HTTP `404 Not Found` with error code `1` (NamespaceNotFound).

If the namespace contains tables or child namespaces, return HTTP `409 Conflict` with error code `3` (NamespaceNotEmpty).

### DeclareTable¶

Declares a new Lance table, reserving the table name and location without creating actual data files.

**HTTP Request:**
    
    
    POST /v1/table/{id}/declare
    Content-Type: application/json
    

The request body contains an optional location:
    
    
    {
      "location": "s3://bucket/data/users.lance"
    }
    

The implementation:

  1. Parse the table identifier from the route path `{id}`
  2. Extract the parent namespace from the identifier
  3. Validate the parent namespace exists
  4. Check if a table with this identifier already exists
  5. Determine the table location (use provided location or generate one)
  6. Reserve the table in the server's storage
  7. Register the table in the namespace


**Response:**
    
    
    {
      "location": "s3://bucket/data/users.lance",
      "storage_options": {
        "aws_access_key_id": "...",
        "aws_secret_access_key": "..."
      }
    }
    

**Error Handling:**

If the parent namespace does not exist, return HTTP `404 Not Found` with error code `1` (NamespaceNotFound).

If a table with the same identifier already exists, return HTTP `409 Conflict` with error code `5` (TableAlreadyExists).

If there is a concurrent creation attempt, return HTTP `409 Conflict` with error code `14` (ConcurrentModification).

### ListTables¶

Lists tables within a namespace.

**HTTP Request:**
    
    
    GET /v1/namespace/{id}/table/list?page_token=xxx&limit=100
    

The `page_token` and `limit` query parameters support pagination.

The implementation:

  1. Parse the namespace identifier from the route path `{id}`
  2. Validate the namespace exists
  3. Query the server's storage for tables in the namespace
  4. Apply pagination using `page_token` and `limit`
  5. Return the list of table names


**Response:**
    
    
    {
      "tables": ["users", "orders", "products"],
      "next_page_token": "def456"
    }
    

The `next_page_token` field is only present if there are more results.

**Error Handling:**

If the namespace does not exist, return HTTP `404 Not Found` with error code `1` (NamespaceNotFound).

### DescribeTable¶

Returns table metadata including schema and version.

**HTTP Request:**
    
    
    POST /v1/table/{id}/describe
    Content-Type: application/json
    

The request body can optionally specify a version:
    
    
    {
      "version": 5
    }
    

The implementation:

  1. Parse the table identifier from the route path `{id}`
  2. Extract the parent namespace from the identifier
  3. Validate the parent namespace exists
  4. Look up the table in the server's storage
  5. If `version` is specified, retrieve that specific version's metadata
  6. Return the table metadata


**Response:**
    
    
    {
      "name": "users",
      "location": "s3://bucket/data/users.lance",
      "schema": {
        "fields": [
          {"name": "id", "type": {"name": "int64"}, "nullable": false},
          {"name": "name", "type": {"name": "utf8"}, "nullable": true}
        ]
      },
      "version": 5
    }
    

**Error Handling:**

If the parent namespace does not exist, return HTTP `404 Not Found` with error code `1` (NamespaceNotFound).

If the table does not exist, return HTTP `404 Not Found` with error code `4` (TableNotFound).

If the specified version does not exist, return HTTP `404 Not Found` with error code `11` (TableVersionNotFound).

### DeregisterTable¶

Deregisters a table from the namespace while preserving its data on storage. The table metadata is removed from the namespace catalog but the table files remain at their storage location.

**HTTP Request:**
    
    
    POST /v1/table/{id}/deregister
    Content-Type: application/json
    

The request body is empty:
    
    
    {}
    

The implementation:

  1. Parse the table identifier from the route path `{id}`
  2. Extract the parent namespace from the identifier
  3. Validate the parent namespace exists
  4. Look up the table in the server's storage
  5. Remove the table registration from the namespace catalog
  6. Return the table location and properties for reference


**Response:**
    
    
    {
      "location": "s3://bucket/data/users.lance",
      "properties": {
        "created_at": "2024-01-15T10:30:00Z"
      }
    }
    

**Error Handling:**

If the parent namespace does not exist, return HTTP `404 Not Found` with error code `1` (NamespaceNotFound).

If the table does not exist, return HTTP `404 Not Found` with error code `4` (TableNotFound).

## Additional Operations¶

The REST namespace supports all operations defined in the [Lance Namespace client spec](<../../client/operations/>). Each operation follows the same HTTP request/response pattern as the basic operations above.

### DropTable¶

Removes a table and its data.

**HTTP Request:**
    
    
    POST /v1/table/{id}/drop
    Content-Type: application/json
    

The request body is empty:
    
    
    {}
    

The implementation:

  1. Parse the table identifier from the route path `{id}`
  2. Extract the parent namespace from the identifier
  3. Validate the parent namespace exists
  4. Look up the table in the server's storage
  5. Delete the table data from storage
  6. Remove the table registration from the namespace


**Response:**
    
    
    {}
    

**Error Handling:**

If the parent namespace does not exist, return HTTP `404 Not Found` with error code `1` (NamespaceNotFound).

If the table does not exist, return HTTP `404 Not Found` with error code `4` (TableNotFound).

If there is a storage permission error, return HTTP `403 Forbidden` with error code `15` (PermissionDenied).

If there is an unexpected server error, return HTTP `500 Internal Server Error` with error code `18` (Internal).

### RegisterTable¶

Registers an existing Lance table at a given location.

**HTTP Request:**
    
    
    POST /v1/table/{id}/register
    Content-Type: application/json
    
    
    
    {
      "location": "s3://bucket/data/users.lance"
    }
    

**Error Handling:**

If the parent namespace does not exist, return HTTP `404 Not Found` with error code `1` (NamespaceNotFound).

If a table with the same identifier already exists, return HTTP `409 Conflict` with error code `5` (TableAlreadyExists).

If the location does not contain a valid Lance table, return HTTP `400 Bad Request` with error code `13` (InvalidInput).

### RenameTable¶

Renames a table, optionally moving it to a different namespace.

**HTTP Request:**
    
    
    POST /v1/table/{id}/rename
    Content-Type: application/json
    
    
    
    {
      "new_id": ["new_namespace", "new_table_name"]
    }
    

**Error Handling:**

If the source table does not exist, return HTTP `404 Not Found` with error code `4` (TableNotFound).

If a table with the new identifier already exists, return HTTP `409 Conflict` with error code `5` (TableAlreadyExists).

If the target namespace does not exist, return HTTP `404 Not Found` with error code `1` (NamespaceNotFound).

### CreateTableVersion¶

Creates a new version entry for a table.

**HTTP Request:**
    
    
    POST /v1/table/{id}/version/create
    Content-Type: application/json
    
    
    
    {
      "version": 2,
      "manifest_path": "s3://bucket/data/users.lance/_versions/staging-uuid.manifest",
      "naming_scheme": "V2"
    }
    

**Error Handling:**

If the table does not exist, return HTTP `404 Not Found` with error code `4` (TableNotFound).

If the version already exists, return HTTP `409 Conflict` with error code `12` (TableVersionAlreadyExists).

### ListTableVersions¶

Lists version entries for a table.

**HTTP Request:**
    
    
    GET /v1/table/{id}/version/list?descending=true&limit=100
    

**Error Handling:**

If the table does not exist, return HTTP `404 Not Found` with error code `4` (TableNotFound).

### DescribeTableVersion¶

Retrieves details for a specific table version.

**HTTP Request:**
    
    
    POST /v1/table/{id}/version/describe
    Content-Type: application/json
    
    
    
    {
      "version": 2
    }
    

**Error Handling:**

If the table does not exist, return HTTP `404 Not Found` with error code `4` (TableNotFound).

If the version does not exist, return HTTP `404 Not Found` with error code `11` (TableVersionNotFound).

### BatchCreateTableVersions¶

Atomically creates version entries for multiple tables.

**HTTP Request:**
    
    
    POST /v1/table/version/batch-create
    Content-Type: application/json
    
    
    
    {
      "entries": [
        {
          "id": ["namespace", "table1"],
          "version": 2,
          "manifest_path": "s3://bucket/data/table1.lance/_versions/staging-uuid.manifest"
        },
        {
          "id": ["namespace", "table2"],
          "version": 3,
          "manifest_path": "s3://bucket/data/table2.lance/_versions/staging-uuid.manifest"
        }
      ]
    }
    

**Error Handling:**

If any table does not exist, return HTTP `404 Not Found` with error code `4` (TableNotFound).

If any version already exists, return HTTP `409 Conflict` with error code `12` (TableVersionAlreadyExists).

### BatchDeleteTableVersions¶

Deletes multiple version entries for a table.

**HTTP Request:**
    
    
    POST /v1/table/{id}/version/batch-delete
    Content-Type: application/json
    
    
    
    {
      "versions": [1, 2, 3]
    }
    

**Error Handling:**

If the table does not exist, return HTTP `404 Not Found` with error code `4` (TableNotFound).

If any specified version does not exist and `ignore_missing` is false, return HTTP `404 Not Found` with error code `11` (TableVersionNotFound).

### NamespaceExists¶

Checks if a namespace exists.

**HTTP Request:**
    
    
    POST /v1/namespace/{id}/exists
    

### TableExists¶

Checks if a table exists.

**HTTP Request:**
    
    
    POST /v1/table/{id}/exists
    

### ListAllTables¶

Lists all tables across all namespaces.

**HTTP Request:**
    
    
    GET /v1/table/list?page_token=xxx&limit=100
    

### RestoreTable¶

Restores a table to a previous version.

**HTTP Request:**
    
    
    POST /v1/table/{id}/restore
    Content-Type: application/json
    
    
    
    {
      "version": 5
    }
    

### CreateTable¶

Creates a new table with initial data.

**HTTP Request:**
    
    
    POST /v1/table/{id}/create
    Content-Type: application/json
    

### CreateEmptyTable¶

Creates an empty table with a specified schema.

**HTTP Request:**
    
    
    POST /v1/table/{id}/create-empty
    Content-Type: application/json
    

### GetTableStats¶

Returns statistics for a table.

**HTTP Request:**
    
    
    POST /v1/table/{id}/stats
    

### UpdateTableSchemaMetadata¶

Updates schema-level metadata for a table.

**HTTP Request:**
    
    
    POST /v1/table/{id}/schema/metadata
    Content-Type: application/json
    

### AlterTableAddColumns¶

Adds new columns to a table.

**HTTP Request:**
    
    
    POST /v1/table/{id}/alter/add-columns
    Content-Type: application/json
    

### AlterTableAlterColumns¶

Modifies existing columns in a table.

**HTTP Request:**
    
    
    POST /v1/table/{id}/alter/alter-columns
    Content-Type: application/json
    

### AlterTableDropColumns¶

Removes columns from a table.

**HTTP Request:**
    
    
    POST /v1/table/{id}/alter/drop-columns
    Content-Type: application/json
    

### InsertIntoTable¶

Inserts data into a table.

**HTTP Request:**
    
    
    POST /v1/table/{id}/insert
    Content-Type: application/json
    

### MergeInsertIntoTable¶

Performs a merge insert (upsert) operation.

**HTTP Request:**
    
    
    POST /v1/table/{id}/merge-insert
    Content-Type: application/json
    

### UpdateTable¶

Updates rows in a table.

**HTTP Request:**
    
    
    POST /v1/table/{id}/update
    Content-Type: application/json
    

### DeleteFromTable¶

Deletes rows from a table.

**HTTP Request:**
    
    
    POST /v1/table/{id}/delete
    Content-Type: application/json
    

### QueryTable¶

Queries data from a table.

**HTTP Request:**
    
    
    POST /v1/table/{id}/query
    Content-Type: application/json
    

### CountTableRows¶

Counts rows in a table.

**HTTP Request:**
    
    
    POST /v1/table/{id}/count
    Content-Type: application/json
    

### ExplainTableQueryPlan¶

Returns the query execution plan.

**HTTP Request:**
    
    
    POST /v1/table/{id}/query/explain
    Content-Type: application/json
    

### AnalyzeTableQueryPlan¶

Analyzes the query execution plan with statistics.

**HTTP Request:**
    
    
    POST /v1/table/{id}/query/analyze
    Content-Type: application/json
    

### CreateTableIndex¶

Creates a vector index on a table.

**HTTP Request:**
    
    
    POST /v1/table/{id}/index/create
    Content-Type: application/json
    

### CreateTableScalarIndex¶

Creates a scalar index on a table.

**HTTP Request:**
    
    
    POST /v1/table/{id}/index/create-scalar
    Content-Type: application/json
    

### ListTableIndices¶

Lists all indices on a table.

**HTTP Request:**
    
    
    GET /v1/table/{id}/index/list
    

### DescribeTableIndexStats¶

Returns statistics for a table index.

**HTTP Request:**
    
    
    POST /v1/table/{id}/index/{index_name}/stats
    

### DropTableIndex¶

Removes an index from a table.

**HTTP Request:**
    
    
    POST /v1/table/{id}/index/{index_name}/drop
    

### ListTableTags¶

Lists all tags for a table.

**HTTP Request:**
    
    
    GET /v1/table/{id}/tag/list
    

### GetTableTagVersion¶

Gets the version associated with a tag.

**HTTP Request:**
    
    
    POST /v1/table/{id}/tag/{tag_name}/describe
    

### CreateTableTag¶

Creates a new tag for a table version.

**HTTP Request:**
    
    
    POST /v1/table/{id}/tag/create
    Content-Type: application/json
    

### DeleteTableTag¶

Deletes a tag from a table.

**HTTP Request:**
    
    
    POST /v1/table/{id}/tag/{tag_name}/delete
    

### UpdateTableTag¶

Updates a tag to point to a different version.

**HTTP Request:**
    
    
    POST /v1/table/{id}/tag/{tag_name}/update
    Content-Type: application/json
    

### DescribeTransaction¶

Returns details about a transaction.

**HTTP Request:**
    
    
    POST /v1/transaction/{id}/describe
    

### AlterTransaction¶

Modifies a transaction's state.

**HTTP Request:**
    
    
    POST /v1/transaction/{id}/alter
    Content-Type: application/json
    

## Error Response Format¶

All error responses follow the JSON error response model based on [RFC-7807](<https://datatracker.ietf.org/doc/html/rfc7807>).

The response body contains an [ErrorResponse](<../../client/operations/models/ErrorResponse/>) with a `code` field containing the Lance Namespace error code. See [Error Handling](<../../client/operations/errors/>) for the complete list of error codes.

**Example error response:**
    
    
    {
      "error": "Table 'users' not found in namespace 'production'",
      "code": 4,
      "detail": "java.lang.RuntimeException: Table not found\n\tat com.example.TableService.describe(TableService.java:42)\n\tat ...",
      "instance": "/v1/table/production$users/describe"
    }
    

The `detail` field contains detailed error information such as stack traces for debugging purposes.

## Error Code to HTTP Status Mapping¶

REST namespace implementations must map Lance error codes to HTTP status codes as follows:

  * Error code `0` (Unsupported) maps to HTTP `406 Not Acceptable`
  * Error codes `1`, `4`, `6`, `8`, `10`, `11`, `12` (not found errors) map to HTTP `404 Not Found`
  * Error codes `2`, `3`, `5`, `7`, `9`, `14`, `19` (conflict errors) map to HTTP `409 Conflict`
  * Error codes `13`, `20` (input validation errors) map to HTTP `400 Bad Request`
  * Error code `15` (PermissionDenied) maps to HTTP `403 Forbidden`
  * Error code `16` (Unauthenticated) maps to HTTP `401 Unauthorized`
  * Error code `17` (ServiceUnavailable) maps to HTTP `503 Service Unavailable`
  * Error code `18` (Internal) maps to HTTP `500 Internal Server Error`
  * Error code `21` (Throttling) maps to HTTP `429 Too Many Requests`


Back to top

