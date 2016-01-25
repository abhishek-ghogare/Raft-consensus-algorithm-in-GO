
Used RWMutex for mutual exclusion. Write lock used for delete, cas and write commands.
Read lock used for read command.

Assuming one command per connection at server side to avoid complexity as errors from 
first command may affect next commands on same connection.


To enable server-side logging, uncomment "log" package & replace all "// log." with "log." in server.go


