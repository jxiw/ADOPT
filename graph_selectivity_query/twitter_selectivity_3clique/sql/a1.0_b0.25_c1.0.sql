select count(*) from edge e1,edge e2,edge e3 where e1.sid = e3.sid AND e1.tid = e2.sid AND e2.tid = e3.tid AND e1.sid < 570000000 AND e3.sid < 570000000 AND e1.tid < 17000000 AND e2.sid < 17000000 AND e2.tid < 570000000 AND e3.tid < 570000000 ;
