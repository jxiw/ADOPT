select count(*) from edge e1,edge e2,edge e3,edge e4,edge e5,edge e6,edge e7,edge e8,edge e9,edge e10,edge e11,edge e12,edge e13,edge e14,edge e15 where e1.sid = e6.sid AND e1.sid = e7.sid AND e1.sid = e8.sid AND e1.sid = e9.sid AND e1.tid = e2.sid AND e1.tid = e10.sid AND e1.tid = e11.sid AND e1.tid = e12.sid AND e2.tid = e3.sid AND e2.tid = e6.tid AND e2.tid = e13.sid AND e2.tid = e14.sid AND e3.tid = e4.sid AND e3.tid = e7.tid AND e3.tid = e10.tid AND e3.tid = e15.sid AND e4.tid = e5.sid AND e4.tid = e8.tid AND e4.tid = e11.tid AND e4.tid = e13.tid AND e5.tid = e9.tid AND e5.tid = e12.tid AND e5.tid = e14.tid AND e5.tid = e15.tid AND e1.sid < 37000000 AND e6.sid < 37000000 AND e7.sid < 37000000 AND e8.sid < 37000000 AND e9.sid < 37000000 AND e1.tid < 37000000 AND e2.sid < 37000000 AND e10.sid < 37000000 AND e11.sid < 37000000 AND e12.sid < 37000000 AND e3.tid < 17000000 AND e4.sid < 17000000 AND e7.tid < 17000000 AND e10.tid < 17000000 AND e15.sid < 17000000 ;
