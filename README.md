This is a project to demonstrate effective use of JDBC with Greenplum Database to load (insert/update/delete) data into a Greenplum Database.

Single or traditional batch inserts are not effective (very slow) in Greenplum MPP environment. To get around this problem, Greenplum has provided
users with parallel file load using gpfdist and external tables. For smaller load volumes (less than a million records and less than 100MB of data approximately),
PostgreSQLs copy (integrated as CopyManager in the jdbc driver works well too).

One of the problems of incorporating either of the methods is that both read data from a delimited flat file. This calls for a couple pf things:
1. Staging of data into delimited files before load.
2. Handling the delimiters well so presence of the delimiter as well as newlines in the input data does not disrupt the load.

The project here shows a way to achieve fast insert/update/delete using both of these methods.
