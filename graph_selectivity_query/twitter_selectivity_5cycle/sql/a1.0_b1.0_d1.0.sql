select count(*) from edge e1,edge e2,edge e3,edge e4,edge e5 where e1.sid = e5.sid AND e1.tid = e2.sid AND e2.tid = e3.sid AND e3.tid = e4.sid AND e4.tid = e5.tid AND e1.sid < 570000000 AND e5.sid < 570000000 AND e1.tid < 570000000 AND e2.sid < 570000000 AND e3.tid < 570000000 AND e4.sid < 570000000 ;
