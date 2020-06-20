BEGIN {FS="=| +" ; print "#name Tetcd Tctrl 0Delay 1Delay 2Delay 3Delay 4Delay"} 
  { if ($7 ~ /0Delay/) { d0[$6]=$8 ; c0[$6]=$12 ; c1[$6]=$17 }  
    if ($7 ~ /1Delay/) { d1[$6]=$8 }  
    if ($7 ~ /2Delay/) { d2[$6]=$8 }  
    if ($7 ~ /3Delay/) { d3[$6]=$8 }  
    if ($7 ~ /4Delay/) { d4[$6]=$8 } } 
END { for (name in d0) print name, c0[name], c1[name], d0[name], d1[name], d2[name], d3[name], d4[name] }
