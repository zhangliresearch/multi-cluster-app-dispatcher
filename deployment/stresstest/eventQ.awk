BEGIN {FS=":| +" ; print "#name Tetcd Tctrl 0Delay 1Delay 2Delay 3Delay 4Delay"} { 
  if ($12 ~ /eventQueue.Add_byEnqueue/) {
    if ( $9 == backup ) { backup=""; printf ("--- %s caught up %s: %s\n", $9, $12, $0); next }
    switch ( $36 ) {
    case "Init": # first entry
      if ( ct1[$9] <= 0 ) { q1=q1+1; rank11[$9]=q1; rank12[$9]=q2; rank13[$9]=q3; 
        ct1[$9]=ct1[$9]+1; h1[$9]=$2; m1[$9]=$3; s1[$9]=$4 } 
      printf ("%s %s %s %s q1,q2,q3= %d %d %d entryRank1,2,3= %d %d %d\n", $4, $9, $12, $36, q1, q2, q3, rank11[$9], rank12[$9], rank13[$9] )
      break
    case "Queueing":    # second entry.  happens occasionally when UpdateQueueJobs() has out-of-date info
      if ( ct2[$9] <= 0 ) { q2=q2+1; rank21[$9]=q1; rank22[$9]=q2; rank23[$9]=q3; 
        ct2[$9]=ct2[$9]+1; h2[$9]=$2; m2[$9]=$3; s2[$9]=$4 } 
      printf ("%s %s %s %s q1,q2,q3= %d %d %d entryRank1,2,3= %d %d %d\n", $4, $9, $12, $36, q1, q2, q3, rank21[$9], rank22[$9], rank23[$9] )
      break
    case "HeadOfLine":  # third entry.  happens occasionally when UpdateQueueJobs() has out-of-date info
    case "Dispatched":  # third entry
    case "Running":     # third entry
      if ( ct3[$9] <= 0 ) { q3=q3+1; rank31[$9]=q1; rank32[$9]=q2; rank33[$9]=q3; 
        ct3[$9]=ct3[$9]+1; h3[$9]=$2; m3[$9]=$3; s3[$9]=$4 } 
      printf ("%s %s %s %s q1,q2,q3= %d %d %d entryRank1,2,3= %d %d %d\n", $4, $9, $12, $36, q1, q2, q3, rank31[$9], rank32[$9], rank33[$9] )
      break
    default:
      print "eventQueue.Add_byEnqueue unknown QueueJobStatus " $36 " : " $0
    }
  }
  if ($12 ~ /eventQueue.Add_afterHeadOfLine/) {
    if ( $9 == backup ) { backup=""; printf ("--- %s caught up %s: %s\n", $9, $12, $0); next }
    switch ( $38 ) {
    case "HeadOfLine": # second entry
      if ( ct2[$9] <= 0 ) { q2=q2+1; rank21[$9]=q1; rank22[$9]=q2; rank23[$9]=q3; 
        ct2[$9]=ct2[$9]+1; h2[$9]=$2; m2[$9]=$3; s2[$9]=$4 } 
      printf ("%s %s %s %s q1,q2,q3= %d %d %d entryRank1,2,3= %d %d %d\n", $4, $9, $12, $38, q1, q2, q3, rank21[$9], rank22[$9], rank23[$9] )
      break
    default:
      print "eventQueue.Add_afterHeadOfLine unknown QueueJobStatus " $38 " : " $0
    }
  }

  if ($12 ~ /eventQueue.Pop/) {
    # print "eventQueue.Pop " $4, $9; 
    switch ( $36 ) {
    case "Init": # first entry
      if ( ct1[$9] <= 0 ) { 
        printf ("--- %s not in eventQueue for Pop: %s\n", $9, $0 )
        backup=$9 ;
        printf ("%s %s %s %s q1,q2,q3= %d %d %d entryRank1,2,3= %d %d %d delay1= %.6f\n", $4, $9, $12, $36, q1, q2, q3, rank11[$9], rank12[$9], rank13[$9], 0 )
        sum1=sum1+0; n1=n1+1
      }
      else { q1=q1-1 ; ct1[$9]=ct1[$9]-1 ; delay=($2-h1[$9])*3600 + ($3-m1[$9])*60 + $4-s1[$9] ;
        printf ("%s %s %s %s q1,q2,q3= %d %d %d entryRank1,2,3= %d %d %d delay1= %.6f\n", $4, $9, $12, $36, q1, q2, q3, rank11[$9], rank12[$9], rank13[$9], delay )
        sum1=sum1+delay/(rank11[$9]+rank12[$9]+rank13[$9]); n1=n1+1
      }  
      break
    case "Queueing":    # second entry.  happens occasionally when UpdateQueueJobs() has out-of-date info
    case "HeadOfLine":  # second entry
      if ( ct2[$9] <= 0 ) { 
        printf ("--- %s not in eventQueue for Pop: %s\n", $9, $0 )
        backup=$9 ;
        printf ("%s %s %s %s q1,q2,q3= %d %d %d entryRank1,2,3= %d %d %d delay2= %.6f\n", $4, $9, $12, $36, q1, q2, q3, rank21[$9], rank22[$9], rank23[$9], 0 )
        sum2=sum2+0; n2=n2+1
      }
      else { q2=q2-1 ; ct2[$9]=ct2[$9]-1 ; delay=($2-h2[$9])*3600 + ($3-m2[$9])*60 + $4-s2[$9] ;
        printf ("%s %s %s %s q1,q2,q3= %d %d %d entryRank1,2,3= %d %d %d delay2= %.6f\n", $4, $9, $12, $36, q1, q2, q3, rank21[$9], rank22[$9], rank23[$9], delay )
        sum2=sum2+delay/(rank21[$9]+rank22[$9]+rank23[$9]); n2=n2+1
      }
      break
    case "Dispatched":  # third entry
    case "Running":     # third entry
      if ( ct3[$9] <= 0 ) { 
        printf ("--- %s not in eventQueue for Pop: %s\n", $9, $0 )
        backup=$9 ;
        printf ("%s %s %s %s q1,q2,q3= %d %d %d entryRank1,2,3= %d %d %d delay3= %.6f\n", $4, $9, $12, $36, q1, q2, q3, rank31[$9], rank32[$9], rank33[$9], 0 )
        sum3=sum3+0; n3=n3+1
      }
      else { q3=q3-1 ; ct3[$9]=ct3[$9]-1 ; delay=($2-h3[$9])*3600 + ($3-m3[$9])*60 + $4-s3[$9] ;
        printf ("%s %s %s %s q1,q2,q3= %d %d %d entryRank1,2,3= %d %d %d delay3= %.6f\n", $4, $9, $12, $36, q1, q2, q3, rank31[$9], rank32[$9], rank33[$9], delay )
        sum3=sum3+delay/(rank31[$9]+rank32[$9]+rank33[$9]); n3=n3+1
      }
      break
    default:
      print "eventQueue.Pop unknown QueueJobStatus " $36 " : " $0
    }
  }
} 
END { print "mean(delay1)= " sum1/n1 " mean(delay2)= " sum2/n2 " mean(delay3)= " sum3/n3 ;
 printf ( "delay1_sum_n_mean= %.6f %d %.6f delay2_sum_n_mean= %.6f %d %.6f delay3_sum_n_mean= %.6f %d %.6f\n",  sum1, n1, sum1/n1, sum2, n2, sum2/n2, sum3, n3, sum3/n3) ;
  #for (name in h) print name, h[name], m[name], s[name] 
}
