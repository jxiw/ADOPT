SELECT count(*) FROM edge e1, edge e2, edge e3, edge e4, edge e5, edge e6, edge e7, edge e8, edge e9, edge e10
WHERE e1.tid = e2.sid AND e2.tid = e3.sid AND e3.tid = e4.sid AND e4.tid = e5.tid AND e1.sid = e5.sid
AND e6.sid = e1.sid and e6.tid = e2.tid
and e7.sid = e1.sid and e7.tid = e3.tid
and e8.sid = e2.sid and e8.tid = e3.tid
and e9.sid = e2.sid and e9.tid = e4.tid
and e10.sid = e3.sid and e10.tid = e4.tid
AND e1.sid < e1.tid AND e2.sid < e2.tid AND e3.sid < e3.tid AND e4.sid < e4.tid;