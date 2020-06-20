BEGIN {FS=":| +|=" ; print "#time name RemainLen QJState delay1 delay2 delay3"} { 
  if ($8 != "[ScheduleNext]") { next }
  if ($10 == "Blocking") { block=1; next } # account for 1 sec HOL Blocking time
  if ($9 == "activeQ.Pop") { # job pop first, may not be the one poped after PriorityUpdate 
    name=""; h0=$2; m0=$3; s0=$4; q0=$15; state0=$42; block=0;
    printf("=== %s %s RemainLen=%d QJState=%s\n", $4, $10, q0, state0); next }
  if ($9 == "activeQ.Pop_afterPriorityUpdate") { # job pop after PriorityUpdate
    h[$10]=h0; m[$10]=m0; s[$10]=s0; # record time of first pop for HOL
    name=$10; q[$10]=$15; state[$10]=$42; delay1[$10]=($2-h0)*3600+($3-m0)*60+$4-s0-block; 
    printf("=== %s %s RemainLen=%d QJState=%s   delay1=%.6f\n", $4, $10, q[$10], state[$10], delay1[$10]); 
    h0=$2; m0=$3; s0=$4; q0=$15; if (block==1) {block=0}; next }
  if ($9 == "after" && $10 == "Pop") { # after HOL updateEtcd
    if (name=="") { name=$14 }
    if (name != $14) { printf("=== error: name not match previous: name=%s $0=%s\n", name, $0); next }
    q[$14]=$12; state[$14]=$43; delay2[$14]=($2-h0)*3600+($3-m0)*60+$4-s0-block; 
    printf("=== %s %s RemainLen=%d QJState=%s delay2=%.6f\n", $4, $14, q[$14], state[$14], delay2[$14]); 
    h0=$2; m0=$3; s0=$4; q0=$15; if (block==1) {block=0}; next }
  if ($9 == "XQJ") { # after count resource 
    if (name=="") { name=$10; state[$10]="HeadOfLine"; q[$10]=q0; h[$10]=h0; m[$10]=m0; s[$10]=s0 }
    if (name != $10 ) { # v4 only has XQJ entry
      printf("=== error: name not match previous: name=%s $0=%s\n", name, $0); 
      state[$10]="HeadOfLine"; q[$10]=q0; h[$10]=h0; m[$10]=m0; s[$10]=s0 
    }
    delay3[$10]=($2-h0)*3600+($3-m0)*60+$4-s0-block; 
    printf("=== %s %s RemainLen=%d QJState=%s delay3=%.6f\n", $4, $10, q[$10], state[$10], delay3[$10]); 
    h0=$2; m0=$3; s0=$4; q0=$15; if (block==1) {block=0}; next }
  if ($10 == "afterCanRunUpdate") { # after CanRunUpdate 
    if (name != $9 ) { # Shouldn't happen
      printf("=== error: name not match previous: name=%s $0=%s\n", name, $0); 
      state[$9]="HeadOfLine"; q[$9]=q0; h[$9]=h0; m[$9]=m0; s[$9]=s0 
    }
    delay4[$9]=($2-h0)*3600+($3-m0)*60+$4-s0-block; 
    printf("=== %s %s RemainLen=%d QJState=%s delay4=%.6f\n", $4, $9, q[$9], state[$9], delay4[$9]); 
    h0=$2; m0=$3; s0=$4; q0=$15; if (block==1) {block=0}; next }
  if ($10 == "2Delay") { # job added to eventQueue
    if (name != $9 ) { # v4 only has 2Delay entry
      printf("=== error: name not match previous: name=%s $0=%s\n", name, $0); 
      q[$9]=q0; h[$9]=h0; m[$9]=m0; s[$9]=s0 
    }
    state[$9]=$44; delay5[$9]=($2-h0)*3600+($3-m0)*60+$4-s0-block; 
    printf("=== %s %s RemainLen=%d QJState=%s delay5=%.6f\n", $4, $9, q[$9], state[$9], delay5[$9]); 
    h0=$2; m0=$3; s0=$4; q0=$15; if (block==1) {block=0}; next }
  if ($13 == "after_qjqueue.Delete") { # job deleted from qjqueue 
    if (name != $9 ) { # Shouldn't happen
      printf("=== error: name not match previous: name=%s $0=%s\n", name, $0); 
      q[$9]=q0; h[$9]=h0; m[$9]=m0; s[$9]=s0 
    }
    state[$9]=$44; delay6[$9]=($2-h0)*3600+($3-m0)*60+$4-s0-block; 
    printf("=== %s %s RemainLen=%d QJState=%s delay6=%.6f\n", $4, $9, q[$9], state[$9], delay6[$9]); 
    h0=$2; m0=$3; s0=$4; q0=$15; if (block==1) {block=0}; next }
} 
END { 
  for (na in h) {
    printf("%s:%s:%s %s %d %s %.6f %.6f %.6f %.6f %.6f %.6f\n", h[na], m[na], s[na], na, q[na], state[na], delay1[na], delay2[na], delay3[na], delay4[na], delay5[na], delay6[na]) 
    sum1=sum1+delay1[na]; sum2=sum2+delay2[na]; sum3=sum3+delay3[na]; sum4=sum4+delay4[na]; sum5=sum5+delay5[na]; sum6=sum6+delay6[na]; 
    n1=n1+1; n2=n2+1; n3=n3+1; n4=n4+1; n5=n5+1; n6=n6+1; 
  }
  printf("# mean(delay1)= %.6f mean(delay2)= %.6f mean(delay3)= %.6f mean(delay4)= %.6f mean(delay5)= %.6f mean(delay6)= %.6f mean(all)=%.6f\n", sum1/n1, sum2/n2, sum3/n3, sum4/n4, sum5/n5, sum6/n6,  sum1/n1+sum2/n2+sum3/n3+sum4/n4+sum5/n5+sum6/n6) ;
}
#print "mean(delay1)= " sum1/n1 " mean(delay2)= " sum2/n2 " mean(delay3)= " sum3/n3 ;
# printf ( "delay1_sum_n_mean= %.6f %d %.6f delay2_sum_n_mean= %.6f %d %.6f delay3_sum_n_mean= %.6f %d %.6f\n",  sum1, n1, sum1/n1, sum2, n2, sum2/n2, sum3, n3, sum3/n3) ;
