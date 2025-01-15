-- Average number of web events of a session from a user on Tech Creator
SELECT AVG(event_count) FROM event_sessions WHERE host LIKE '%techcreator%'; 

-- Average number of web events of a session from a user on Tech Creator per sub domain
SELECT host, AVG(event_count) FROM event_sessions WHERE host LIKE '%techcreator.io' GROUP BY host;

-- Average number of of web events of a session from a user for all recorded hosts
SELECT host, AVG(event_count) FROM event_sessions GROUP BY host;