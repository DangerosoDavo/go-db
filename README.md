# go-db
golang package to have easy database creation and usage.

Currently untested, but should work.  Use it to store structs and relationships between structs in a mysql database.  Has Search and Upsert which queries a database for the given records / related records.

Use the tags `GoDBKey:"primary|secondary"` to mark specific fields on any given struct to determine the keys/relationships.

This package was made with some assumptions in mind.

Database support:
Mysql

Usage:

Use
GoDB.AddConnections to add a connection pool.

Connection pools can be read, write or read/write to support master/slave databases.  Just be sure to only add one type of connection pool at once.  Requests are blocking but handled as fast as possible and can be used/accessed safely from multiple goroutines.  If more queries are requested at the same time than there are active connections then it will wait for a connection to be avaliable and execute the instant one becomes free.

Table names are the pluralized lower case name of the struct if its a basic record upsert, for relationships the table names are the lower case struct names joined with _.

GoDB.Upsert is used to insert or update the given records in the database.
GoDB.Search Allows you to search for instances of the given struct the conditions for the search are defined by a map of columnNames to Where clauses.
GoDB.SearchJunctionTable Allows you to search a junction table from the two given structs with a slice of additional fields, the conditions for the search are defined by a map of columnNames to Where clauses.