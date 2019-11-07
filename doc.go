/*
Package sntable contains a custom SSTable implementation which,
instead of arbitrary bytes strings, assumes 8-byte numeric keys.

Data Structure Documentation

Store

A store contains a series of data blocks followed by an index and
a store footer.

    Store layout:
    +---------+---------+---------+-------------+--------------+
    | block 1 |   ...   | block n | block index | store footer |
    +---------+---------+---------+-------------+--------------+

    Block index:
    +----------------------------+--------------------+----------------------------------+--------------------------+--------+
    | last cell block 1 (varint) |  offset 2 (varint) | last cell block 2 (varint,delta) |  offset 2 (varint,delta) |   ...  |
    +----------------------------+--------------------+----------------------------------+--------------------------+--------+

    Store footer:
    +------------------------+------------------+
    | index offset (8 bytes) |  magic (8 bytes) |
    +------------------------+------------------+

Block

A block comprises of a series of sections, followed by a section
index and a single-byte compression type indicator.

    Block layout:
    +-----------+---------+-----------+---------------+---------------------------+
    | section 1 |   ...   | section n | section index | compression type (1-byte) |
    +-----------+---------+-----------+---------------+---------------------------+

    Section index:
    +----------------------------+-------+----------------------------+-------------------------------+
    | section offset 2 (4 bytes) |  ...  | section offset n (4 bytes) |  number of sections (4 bytes) |
    +----------------------------+-------+----------------------------+-------------------------------+

Section

A section is a series of key/value pairs where the first key is stored as a full uint64 while subsequent keys
are delta encoded.

    +----------------+----------------------+------------------+----------------------+----------------------+------------------+-------+
    | key 1 (varint) | value len 1 (varint) | value 1 (varlen) | key 2 (varint,delta) | value len 2 (varint) | value 2 (varlen) |  ...  |
    +----------------+----------------------+------------------+----------------------+----------------------+------------------+-------+
*/
package sntable
