BEGIN {FS=":| +" ; print "#name Tetcd Tctrl 0Delay 1Delay 2Delay 3Delay 4Delay"} { 
  if ($12 ~ /eventQueue.Add_afterHeadOfLine/) {
    print "eventQueue.Add_afterHeadOfLine " $4, $9, $37, $38, $39; 
  }
} 
END { for (name in h) print name, h[name], m[name], s[name] }
