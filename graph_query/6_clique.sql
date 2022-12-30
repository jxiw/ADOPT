SELECT count(*) FROM edge e1, edge e2, edge e3, edge e4, edge e5, edge e6, edge e7, edge e8, edge e9,
edge e10, edge e11, edge e12, edge e13, edge e14, edge e15
where e1.tid = e2.sid AND e2.tid = e3.sid AND e3.tid = e4.sid AND e4.tid = e5.sid AND e1.sid = e6.sid AND e5.tid = e6.tid
AND e1.sid = e7.sid and e7.tid = e2.tid AND e1.sid = e8.sid and e8.tid = e3.tid and e1.sid = e9.sid and e9.tid = e4.tid
AND e2.sid = e10.sid and e10.tid = e3.tid AND e2.sid = e11.sid and e11.tid = e4.tid AND e2.sid = e12.sid and e12.tid = e5.tid
AND e3.sid = e13.sid and e13.tid = e4.tid AND e3.sid = e14.sid AND e14.tid = e5.tid
AND e4.sid = e15.sid and e15.tid = e5.tid AND e1.sid < e1.tid AND e2.sid < e2.tid AND e3.sid < e3.tid
AND e4.sid < e4.tid AND e5.sid < e5.tid AND e6.sid < e6.tid AND e7.sid < e7.tid AND e8.sid < e8.tid
AND e9.sid < e9.tid   AND e10.sid < e10.tid AND e11.sid < e11.tid AND e12.sid < e12.tid
AND e13.sid < e13.tid AND e14.sid < e14.tid AND e15.sid < e15.tid;