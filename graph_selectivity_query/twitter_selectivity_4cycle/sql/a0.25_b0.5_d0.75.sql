select count(*) from edge e1,edge e2,edge e3,edge e4 where e1.sid = e4.sid AND e1.tid = e2.sid AND e2.tid = e3.sid AND e3.tid = e4.tid AND e1.sid < 17000000 AND e4.sid < 17000000 AND e1.tid < 37000000 AND e2.sid < 37000000 AND e3.tid < 145000000 AND e4.tid < 145000000 ;
