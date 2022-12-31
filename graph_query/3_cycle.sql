select count(*) from edge e1, edge e2, edge e3 where e1.tid=e2.sid and e2.tid=e3.tid and e3.sid = e1.sid
and e1.sid < e1.tid and e2.sid < e2.tid and e3.sid < e3.tid;