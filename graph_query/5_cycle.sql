SELECT count(*) FROM edge e1, edge e2, edge e3, edge e4, edge e5
WHERE e1.tid = e2.sid AND e2.tid = e3.sid AND e3.tid = e4.sid AND e4.tid = e5.tid AND e1.sid = e5.sid
AND e1.sid < e1.tid AND e2.sid < e2.tid AND e3.sid < e3.tid AND e4.sid < e4.tid;