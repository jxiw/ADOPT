SELECT count(*) FROM edge e1, edge e2, edge e3, edge e4, edge e5, edge e6, edge e7, edge e8, edge e9,
edge e10, edge e11, edge e12, edge e13, edge e14, edge e15, edge e16, edge e17, edge e18, edge e19, edge e20, edge e21
where e1.sid = e7.sid AND e1.sid = e8.sid AND e1.sid = e9.sid AND e1.sid = e10.sid AND e1.sid = e11.sid
AND e1.tid = e2.sid AND e1.tid = e12.sid AND e1.tid = e13.sid AND e1.tid = e14.sid AND e1.tid = e15.sid
AND e2.tid = e3.sid AND e2.tid = e7.tid AND e2.tid = e16.sid AND e2.tid = e17.sid AND e2.tid = e18.sid
AND e3.tid = e4.sid AND e3.tid = e8.tid AND e3.tid = e12.tid AND e3.tid = e19.sid AND e3.tid = e20.sid
AND e4.tid = e5.sid AND e4.tid = e9.tid AND e4.tid = e13.tid AND e4.tid = e16.tid AND e4.tid = e21.sid
AND e5.tid = e6.sid AND e5.tid = e10.tid AND e5.tid = e14.tid AND e5.tid = e17.tid AND e5.tid = e19.tid
AND e6.tid = e11.tid AND e6.tid = e15.tid AND e6.tid = e18.tid AND e6.tid = e20.tid AND e6.tid = e21.tid
AND e1.sid < e1.tid AND e2.sid < e2.tid AND e3.sid < e3.tid
AND e4.sid < e4.tid AND e5.sid < e5.tid AND e6.sid < e6.tid
AND e7.sid < e7.tid AND e8.sid < e8.tid AND e9.sid < e9.tid
AND e10.sid < e10.tid AND e11.sid < e11.tid AND e12.sid < e12.tid
AND e13.sid < e13.tid AND e14.sid < e14.tid AND e15.sid < e15.tid
AND e16.sid < e16.tid AND e17.sid < e17.tid AND e18.sid < e18.tid
AND e19.sid < e19.tid AND e20.sid < e20.tid AND e21.sid < e21.tid;